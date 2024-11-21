import asyncio
from datetime import datetime
import signal
import time
import ujson
import websockets
import os

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
import backoff

from pathlib import Path

from loguru import logger

CURSOR_FILENAME = Path(__file__).parent.parent / "cursor.txt"
MAX_RETRIES = 3


class JetstreamHoover:
    def __init__(
        self,
        url: str = "wss://jetstream2.us-west.bsky.network/subscribe",
        batch_size: int = 8192,
        timeout: float = 5.0,
    ):
        self.url = url
        self.post_queue = asyncio.Queue()
        self.batch_size = batch_size
        self.timeout = timeout
        self.cursor = self.get_checkpoint()
        self.websocket_task = None
        self.running = True

        signal.signal(
            signal.SIGINT, lambda _, __: asyncio.create_task(self.signal_handler())
        )

    async def signal_handler(self) -> None:
        logger.warning("Keyboard interrupt received. Stopping...")
        self.running = False

        tasks_to_cancel = []

        # Cancel websocket task
        if self.websocket_task and not self.websocket_task.done():
            tasks_to_cancel.append(self.websocket_task)

        # Wait for queue to process remaining items
        if not self.post_queue.empty():
            logger.info("Waiting for remaining posts to be processed (5s timeout)...")
            try:
                await asyncio.wait_for(self.post_queue.join(), timeout=5.0)
                logger.success("Successfully processed remaining posts")
            except asyncio.TimeoutError:
                logger.warning("Timeout reached while processing remaining posts")
                if self.process_task and not self.process_task.done():
                    tasks_to_cancel.append(self.process_task)

        # Wait for all tasks to be cancelled
        if tasks_to_cancel:
            logger.info("Waiting for tasks to cancel...")
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        logger.info("Shutdown complete")

    def get_checkpoint(self) -> int | None:
        try:
            with open(CURSOR_FILENAME, "r") as f:
                return int(f.read())
        except FileNotFoundError:
            return None

    async def start(self):
        try:
            self.websocket_task = asyncio.create_task(self.slurp_websocket())
            self.process_task = asyncio.create_task(self.process_queue())

            await asyncio.gather(
                self.websocket_task, self.process_task, return_exceptions=True
            )
        finally:
            if self.websocket_task and not self.websocket_task.done():
                self.websocket_task.cancel()
                try:
                    await self.websocket_task
                except asyncio.CancelledError:
                    pass
            if self.process_task and not self.process_task.done():
                self.process_task.cancel()
                try:
                    await self.process_task
                except asyncio.CancelledError:
                    pass

    async def process_queue(self):
        clickhouse = ClickHouseManager()
        batch = []
        last_insert = time.monotonic()

        while self.running:
            try:
                record = await asyncio.wait_for(
                    self.post_queue.get(), timeout=self.timeout
                )
                batch.append(record)

                if len(batch) >= self.batch_size or (
                    time.monotonic() - last_insert > self.timeout
                ):
                    logger.info(
                        f"Triggering insert of {len(batch)} records, last insert {time.monotonic() - last_insert:.2f}s ago"
                    )

                    client = await clickhouse.get_client()
                    await self.insert_batch(client, batch)
                    self.post_queue.task_done()

                    last_insert = time.monotonic()
                    batch = []
            except asyncio.TimeoutError:
                if batch and self.running:
                    logger.info(
                        f"Queue timeout reached - writing batch of {len(batch)} records on time grounds"
                    )

                    client = await clickhouse.get_client()
                    await self.insert_batch(client, batch)
                    self.post_queue.task_done()

                    last_insert = time.monotonic()
                    batch = []
            except asyncio.CancelledError:
                logger.info("Process queue cancelled")
                if batch:
                    try:
                        client = await clickhouse.get_client()
                        await self.insert_batch(client, batch)
                        self.post_queue.task_done()
                    except Exception as e:
                        logger.error(f"Error processing final batch: {e}")
                raise

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, TimeoutError),
        max_tries=MAX_RETRIES,
        max_time=30,
    )
    async def slurp_websocket(self):
        url = self.url + "?wantedCollections=app.bsky.feed.post&compression=True"

        if self.cursor:
            url += f"&cursor={self.cursor}"

        try:
            async for ws in websockets.connect(
                self.url + "?wantedCollections=app.bsky.feed.post&compression=True"
            ):
                logger.info(f"Connected to {self.url}")
                try:
                    async for data in ws:
                        if not self.running:
                            logger.info("Stopping websocket connection...")
                            await ws.close()
                            break
                        self.handle_message(data)
                except websockets.exceptions.ConnectionClosed:
                    if self.running:
                        logger.warning("Websocket connection closed, reconnecting...")
                    continue

                if not self.running:
                    break

                # Yield to allow other tasks to run
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            logger.info("Websocket task cancelled")
            raise

    def handle_message(self, data: str):
        """Handle incoming messages from the websocket.

        Depending on the parameters passed to the websocket, we could end up with different types of messages - for now
        we'll just handle post creation.
        """

        message = ujson.loads(data)

        match message.get("commit", {}).get("operation"):
            case "create":
                self.handle_create(message)
            case _:
                pass

    def handle_create(self, message: dict):
        """Handle post creation - extract the data we want and put it in the queue for insertion

        Example payload:
        ```
        {
            "did": "did:plc:t7prddsjja2omguswnc6z7sn",
            "time_us": 1732226394053386,
            "kind": "commit",
            "commit": {
                "rev": "3lbihksrxoi23",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3lbihksecec2h",
                "record": {
                "$type": "app.bsky.feed.post",
                "createdAt": "2024-11-21T21:59:31.102Z",
                "langs": [
                    "en"
                ],
                "text": "This is a test message!"
                },
                "cid": "bafyreiasskmx56dhpv4i47z35zfliev2pkjv3ic5s6kvgih5ngn75k6s3u"
            }
        }
        ```

        Example reply payload:
        ```
        {
            "did": "did:plc:t7prddsjja2omguswnc6z7sn",
            "time_us": 1732226643666081,
            "kind": "commit",
            "commit": {
                "rev": "3lbihrzttft2d",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3lbihry4yik2h",
                "record": {
                "$type": "app.bsky.feed.post",
                "createdAt": "2024-11-21T22:03:32.035Z",
                "langs": [
                    "en"
                ],
                "reply": {
                    "parent": {
                    "cid": "bafyreiasskmx56dhpv4i47z35zfliev2pkjv3ic5s6kvgih5ngn75k6s3u",
                    "uri": "at://did:plc:t7prddsjja2omguswnc6z7sn/app.bsky.feed.post/3lbihksecec2h"
                    },
                    "root": {
                    "cid": "bafyreiasskmx56dhpv4i47z35zfliev2pkjv3ic5s6kvgih5ngn75k6s3u",
                    "uri": "at://did:plc:t7prddsjja2omguswnc6z7sn/app.bsky.feed.post/3lbihksecec2h"
                    }
                },
                "text": "This is a test message 2"
                },
                "cid": "bafyreihag3g5zihxxnq5hnhnlgjz5yqux6sq3ntem4vwvj6wdkkwwekq7a"
            }
        }
        ```

        """

        # Extract out the data we wish to store - it's a bit gross using a tuple but clickhouse
        # prefers this (and doesn't support dicts?)
        record = (
            message.get("did", ""),
            message.get("commit", {}).get("cid", ""),
            datetime.fromisoformat(
                message.get("commit", {})
                .get("record", {})
                .get("createdAt", "")
                .replace("Z", "+00:00")
            ),
            message.get("did", ""),
            message.get("commit", {}).get("record", {}).get("text", ""),
            message.get("commit", {})
            .get("record", {})
            .get("reply", {})
            .get("parent", {})
            .get("uri", ""),
        )

        self.post_queue.put_nowait(record)

        time_us = message.get("time_us", 0)

        if self.cursor is None or time_us > self.cursor + 5_000_000:
            logger.info(f"Writing checkpoint at {time_us}")
            with open(CURSOR_FILENAME, "w") as f:
                f.write(str(time_us))
            self.cursor = time_us

        # logger.info(f"Enqueued post: {record[1]}")

    async def insert_batch(self, client: Client, batch: list[tuple]) -> None:
        """
        Take a batch of posts and insert them into ClickHouse
        """

        logger.info(f"Inserting batch of {len(batch)} records")
        client.insert(
            "posts",
            batch,
            column_names=[
                "cid",
                "uri",
                "created_at",
                "author",
                "text",
                "reply_parent_uri",
            ],
        )


class ClickHouseManager:
    def __init__(self):
        self.client: Client | None = None
        self.reconnect_lock = asyncio.Lock()

    async def get_client(self) -> Client:
        if self.client is None:
            async with self.reconnect_lock:
                if self.client is None:  # Double-check pattern
                    await self.connect()
        return self.client

    @backoff.on_exception(
        backoff.expo,
        (ClickHouseError, ConnectionError),
        max_tries=MAX_RETRIES,
        max_time=30,
    )
    async def connect(self) -> None:
        try:
            self.client = clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                database=os.getenv("CLICKHOUSE_DATABASE", "default"),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            )

            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS posts (
                cid String,
                uri String,
                created_at DateTime64(3),
                author String,
                text String,
                reply_parent_uri String,
                insertion_time DateTime64(3) DEFAULT now64()
            ) ENGINE = MergeTree()
            ORDER BY (created_at, cid)
            """
            self.client.command(create_table_query)
            logger.info("Connected to ClickHouse and verified table exists")
        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {str(e)}")
            self.client = None
            raise


if __name__ == "__main__":
    logger.info("Starting JetstreamHoover...")

    # Create event loop explicitly
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    hoover = JetstreamHoover()

    try:
        loop.run_until_complete(hoover.start())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received in main...")
        # Run the signal handler in the event loop
        loop.run_until_complete(hoover.signal_handler())
    finally:
        loop.close()
