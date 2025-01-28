import asyncio
from datetime import datetime
import signal
import time
import ujson
import websockets
from websockets.asyncio.client import connect
import os

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
import backoff

from pathlib import Path

from loguru import logger

CURSOR_FILENAME = Path(__file__).parent.parent / "cursor.txt"
MAX_RETRIES = 10
CHECKPOINT_METHOD = os.getenv("CHECKPOINT_METHOD", "Filesystem")
CURSOR_REFRESH_MS = 5_000_000


class JetstreamHoover:
    def __init__(
        self,
        url: str = "wss://jetstream2.us-east.bsky.network/subscribe",
        batch_size: int = 8192 * 2,
        timeout: float = 5,
    ):
        self.url = url
        self.post_queue = asyncio.Queue()
        self.batch_size = batch_size
        self.timeout = timeout
        self.cursor = None
        self.websocket_task = None
        self.running = True
        self.clickhouse = ClickHouseManager()
        if "KeepermapUpdating" == CHECKPOINT_METHOD:
            self.checkpoint = KeepermapUpdatingCheckpoint()
        elif "KeepermapDeleting" == CHECKPOINT_METHOD:
            self.checkpoint = KeepermapDeletingCheckpoint()
        else:
            self.checkpoint = FilesystemCheckpoint()

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

    async def start(self):
        try:
            self.websocket_task = asyncio.create_task(self.slurp_websocket())
            self.process_task = asyncio.create_task(self.process_queue())
            await asyncio.gather(
                self.websocket_task, self.process_task, return_exceptions=True
            )
        except Exception as e:
            logger.error("Something went wrong", exc_info=True)
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
        batch = []
        last_insert = time.monotonic()

        while self.running:
            try:
                record = await asyncio.wait_for(
                    self.post_queue.get(), timeout=self.timeout
                )
                batch.append(record)
                self.post_queue.task_done()

                if len(batch) >= self.batch_size or (
                    time.monotonic() - last_insert > self.timeout
                ):
                    logger.info(
                        f"Triggering insert of {len(batch)} records, last insert {time.monotonic() - last_insert:.2f}s ago"
                    )

                    client = await self.clickhouse.get_client()
                    await self.insert_batch(client, batch)

                    last_insert = time.monotonic()
                    batch = []
            except TimeoutError:
                if batch and self.running:
                    logger.info(
                        f"Queue timeout reached - writing batch of {len(batch)} records on time grounds"
                    )

                    client = await self.clickhouse.get_client()
                    await self.insert_batch(client, batch)

                    last_insert = time.monotonic()
                    batch = []
            except asyncio.CancelledError:
                logger.info("Process queue cancelled")
                if batch:
                    try:
                        client = await self.clickhouse.get_client()
                        await self.insert_batch(client, batch)
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
        if cursor := await self.checkpoint.get_checkpoint():
            url += f"&cursor={cursor}"

        logger.info(f"Connecting to {url}")

        try:
            async for ws in connect(url):
                logger.info(f"Connected to {url}")
                try:
                    while self.running:
                        data = await asyncio.wait_for(ws.recv(), 1)
                        await self.handle_message(data)
                        # Yield to allow other tasks to run
                        await asyncio.sleep(0)
                except websockets.exceptions.ConnectionClosed:
                    continue

                if self.running:
                    logger.info("Websocket disconnected, reconnecting...")
                    continue
                else:
                    logger.info("Websocket connection closed")
                    break
        except websockets.exceptions.ConnectionClosed:
            logger.info("Websocket connection closed, reconnecting...")
        except asyncio.CancelledError:
            logger.info("Websocket task cancelled")
            raise

    async def handle_message(self, data: str):
        """Handle incoming messages from the websocket.

        Depending on the parameters passed to the websocket, we could end up with different types of messages - for now
        we'll just handle post creation.
        """

        message = ujson.loads(data)

        match message.get("commit", {}).get("operation"):
            case "create":
                await self.handle_create(message)
            case _:
                pass

    async def handle_create(self, message: dict) -> None:
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

        await self.post_queue.put(record)
        time_us = message.get("time_us", 0)
        if not self.cursor or (time_us > self.cursor + CURSOR_REFRESH_MS):
            logger.info(f"Writing checkpoint at {time_us}")
            await self.checkpoint.save_checkpoint(time_us)
            self.cursor = time_us

    async def insert_batch(self, client: Client, batch: list[tuple]) -> None:
        """
        Take a batch of posts and insert them into ClickHouse
        """

        logger.info(f"Inserting batch of {len(batch)} records")
        await client.insert(
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
        logger.info(f"Inserted batch of {len(batch)} records")


class ClickHouseManager:
    def __init__(self):
        self.client = None
        self.reconnect_lock = asyncio.Lock()

    async def get_client(self):
        if self.client is None:
            async with self.reconnect_lock:
                if self.client is None:
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
            self.client = await clickhouse_connect.get_async_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                database=os.getenv("CLICKHOUSE_DATABASE", "default"),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            )

            # Create table if it doesn't exist
            create_posts_table_query = """
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

            create_tokens_by_interval_table_query = """
            CREATE TABLE IF NOT EXISTS tokens_by_interval
                (
                    `period` DateTime64(3),
                    `token` String,
                    `count` UInt64
                )
                ENGINE = SummingMergeTree
                ORDER BY (period, token)
            """

            create_tokenizer_query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS tokenizer TO tokens_by_interval
            (
                `period` DateTime,
                `token` String,
                `count` UInt8
            )
            AS SELECT
                toStartOfInterval(created_at, toIntervalMinute(10)) AS period,
                arrayJoin(splitByNonAlpha(lower(text))) AS token,
                1 AS count
            FROM posts
            """

            create_trends_1hr_table_query = """
            CREATE TABLE IF NOT EXISTS trends_1hr
            (
                `refresh_time` DateTime64(3) DEFAULT now(),
                `timestamp` DateTime64(3),
                `token` String,
                `1hr_count` UInt64,
                `24hr_avg` Float32
            )
            ENGINE = MergeTree
            ORDER BY (timestamp, token)
            """

            create_trends_24hr_table_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS trends_1hr_mv
REFRESH EVERY 1 HOUR APPEND TO trends_1hr
(
    `timestamp` DateTime,
    `token` String,
    `1hr_count` UInt64,
    `24hr_avg` Float64
)
AS WITH
    1 AS hours_in_period,
    24 AS hours_in_full_period
SELECT
    toStartOfInterval(now() - toIntervalHour(hours_in_period), toIntervalHour(hours_in_period)) AS timestamp,
    token AS token,
    this_period.period_count AS `1hr_count`,
    full_period.avg_count AS `24hr_avg`
FROM
(
    SELECT
        toStartOfInterval(period, toIntervalHour(hours_in_period)) AS agg_period,
        token,
        sum(count) AS period_count
    FROM tokens_by_interval
    FINAL
    WHERE agg_period = toStartOfInterval(now() - toIntervalHour(hours_in_period), toIntervalHour(hours_in_period))
    GROUP BY
        agg_period,
        token
    HAVING period_count >= 250
) AS this_period,
(
    SELECT
        token,
        sum(count) / hours_in_full_period AS avg_count
    FROM tokens_by_interval
    FINAL
    WHERE period >= (now() - toIntervalHour(hours_in_full_period))
    GROUP BY token
) AS full_period
WHERE (this_period.token = full_period.token) AND (length(token) > 4) AND (full_period.avg_count > 10)
ORDER BY `1hr_count` / `24hr_avg` DESC
LIMIT 100
"""

            create_cursor_table_query = """
                CREATE TABLE IF NOT EXISTS bluehoover_parameters(`key` String, `uint64_value` UInt64 ) ENGINE = KeeperMap('/bluehoover_parameters') PRIMARY KEY key
            """

            await self.client.command(create_posts_table_query)
            await self.client.command(create_tokens_by_interval_table_query)
            await self.client.command(create_tokenizer_query)
            await self.client.command(create_trends_1hr_table_query)
            await self.client.command(create_trends_24hr_table_query)
            await self.client.command(create_cursor_table_query)

            logger.info("Connected to ClickHouse and verified tables/MVs exists")
        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {str(e)}")
            self.client = None
            raise

    async def insert_batch(self, batch: list[tuple]) -> None:
        """
        Take a batch of posts and insert them into ClickHouse
        """
        if not batch:
            return

        client = await self.get_client()
        logger.info(f"Inserting batch of {len(batch)} records")

        try:
            await client.insert(
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
            logger.info(f"Successfully inserted batch of {len(batch)} records")
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.client = None  # Force reconnect on next attempt
            raise


class FilesystemCheckpoint:
    async def get_checkpoint(self) -> int | None:
        try:
            with open(CURSOR_FILENAME, "r") as f:
                return int(f.read())
        except (FileNotFoundError, ValueError, TypeError, OSError):
            return None

    async def save_checkpoint(self, cursor: int) -> None:
        with open(CURSOR_FILENAME, "w") as f:
            f.write(str(cursor))


class KeepermapUpdatingCheckpoint:
    def __init__(self):
        self.clickhouse = ClickHouseManager()

    async def get_checkpoint(self) -> int | None:
        try:
            client = await self.clickhouse.get_client()
            result = await client.query("SELECT uint64_value FROM bluehoover_parameters WHERE key = 'cursor'")
            return int(result.first_row[0])
        except (ClickHouseError, ValueError, TypeError) as e:
            logger.error(f"Error reading checkpoint: {e}", exc_info=True)
            return None
    """
    This save_checkpoint will not work if the cursor already exist in the table
    """
    async def save_checkpoint(self, cursor: int) -> None:
        try:
            client = await self.clickhouse.get_client()
            insert_query = f"INSERT INTO TABLE bluehoover_parameters VALUES ('cursor', {cursor})"
            # update_query = f"ALTER TABLE bluehoover_parameters UPDATE uint64_value = {cursor} WHERE key = 'cursor'"
            await client.command(insert_query)
            # await client.command(update_query)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("Starting JetstreamHoover...")

    # Create event loop explicitly
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    hoover = JetstreamHoover()
    logger.info("Hoovering!")

    try:
        loop.run_until_complete(hoover.start())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received in main...")
        # Run the signal handler in the event loop
        loop.run_until_complete(hoover.signal_handler())
    finally:
        loop.close()
