import asyncio
from datetime import datetime
import signal
from collections import defaultdict
from types import FrameType
from typing import List, Optional
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
import backoff
import os
from loguru import logger

from atproto import (
    CAR,
    AsyncFirehoseSubscribeReposClient,
    AtUri,
    firehose_models,
    models,
    parse_subscribe_repos_message,
)

_INTERESTED_RECORDS = {
    models.ids.AppBskyFeedLike: models.AppBskyFeedLike,
    models.ids.AppBskyFeedPost: models.AppBskyFeedPost,
    models.ids.AppBskyGraphFollow: models.AppBskyGraphFollow,
}

BATCH_SIZE = 1024
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_TABLE = "posts"

post_queue: asyncio.Queue = asyncio.Queue()

# Add retry configuration
MAX_RETRIES = 3
INITIAL_WAIT_SECONDS = 1
MAX_WAIT_SECONDS = 10


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> defaultdict:
    operation_by_type = defaultdict(lambda: {"created": [], "deleted": []})

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f"at://{commit.repo}/{op.path}")

        if op.action == "create":
            if not op.cid:
                continue

            create_info = {
                "uri": str(uri),
                "cid": str(op.cid),
                "author": commit.repo,
                "path": op.path,
            }

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)
            record_type = _INTERESTED_RECORDS.get(uri.collection)
            if record_type and models.is_record_type(record, record_type):
                operation_by_type[uri.collection]["created"].append(
                    {"record": record, **create_info}
                )

        if op.action == "delete":
            operation_by_type[uri.collection]["deleted"].append({"uri": str(uri)})

    return operation_by_type


@backoff.on_exception(
    backoff.expo, (ClickHouseError, ConnectionError), max_tries=MAX_RETRIES, max_time=30
)
async def process_batch(client: Client, batch: List[dict]) -> None:
    if not batch:
        return

    try:
        # Prepare data for insertion
        data = []
        for post in batch:
            try:
                created_at = datetime.fromisoformat(
                    post["created_at"].replace("Z", "+00:00")
                )
                data.append(
                    [
                        post["cid"],
                        post["uri"],
                        created_at,
                        post["author"],
                        post.get("text") or "",
                        post.get("reply_parent_uri") or "",
                    ]
                )
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping malformed post: {str(e)}, {post}")
                continue

        if not data:
            return

        # Insert data into ClickHouse
        client.insert(
            CLICKHOUSE_TABLE,
            data,
            column_names=[
                "cid",
                "uri",
                "created_at",
                "author",
                "text",
                "reply_parent_uri",
            ],
        )
        logger.info(f"Inserted batch of {len(data)} posts")
    except Exception as e:
        logger.error(f"Error inserting batch: {str(e)}")
        raise


class ClickHouseManager:
    def __init__(self):
        self.client: Optional[Client] = None
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
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DATABASE,
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


async def batch_processor() -> None:
    clickhouse = ClickHouseManager()
    batch = []

    while True:
        try:
            # Get post from queue with timeout
            try:
                post = await asyncio.wait_for(post_queue.get(), timeout=5.0)
                batch.append(post)
                post_queue.task_done()
            except asyncio.TimeoutError:
                if batch:  # Process partial batch on timeout
                    client = await clickhouse.get_client()
                    await process_batch(client, batch)
                    batch = []
                continue

            # Process batch if it reaches BATCH_SIZE
            if len(batch) >= BATCH_SIZE:
                client = await clickhouse.get_client()
                await process_batch(client, batch)
                batch = []

        except Exception as e:
            logger.error(f"Error in batch processor: {e}")
            # Don't clear the batch - it will retry on next iteration


async def queue_for_insertion(post: dict) -> None:
    await post_queue.put(post)


async def signal_handler(_: int, __: FrameType) -> None:
    logger.warning("Keyboard interrupt received. Stopping...")

    # Wait for queue to be empty with a 10-second timeout
    if not post_queue.empty():
        logger.info("Waiting for remaining posts to be processed (5s timeout)...")
        try:
            await asyncio.wait_for(post_queue.join(), timeout=5.0)
            logger.success("Successfully processed remaining posts")
        except asyncio.TimeoutError:
            logger.warning("Timeout reached while processing remaining posts")

    # Stop receiving new messages
    await client.stop()


async def main(firehose_client: AsyncFirehoseSubscribeReposClient) -> None:
    # Start the batch processor
    processor_task = asyncio.create_task(batch_processor())

    async def on_message_handler(message: firehose_models.MessageFrame) -> None:
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        if commit.seq % 20 == 0:
            if commit.seq % 1000 == 0:
                logger.info(f"Cursor is at {commit.seq}")
                with open("seq.txt", "w") as seq_f:
                    seq_f.write(f"{commit.seq}")
            asyncio.sleep(0)
            firehose_client.update_params(
                models.ComAtprotoSyncSubscribeRepos.Params(cursor=commit.seq)
            )

        if not commit.blocks:
            return

        ops = _get_ops_by_type(commit)
        for created_post in ops[models.ids.AppBskyFeedPost]["created"]:
            post = {
                "cid": created_post["cid"],
                "uri": created_post["uri"],
                "created_at": created_post["record"].created_at,
                "author": created_post["author"],
                "text": created_post["record"].text,
                "reply_parent_uri": created_post["record"].reply.parent.uri
                if created_post["record"].reply
                else None,
            }

            await queue_for_insertion(post)

    try:
        await client.start(on_message_handler)
    finally:
        # Wait for processor task to complete
        if not post_queue.empty():
            await post_queue.join()
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    signal.signal(
        signal.SIGINT, lambda _, __: asyncio.create_task(signal_handler(_, __))
    )

    start_cursor = None
    try:
        with open("seq.txt", "r") as seq:
            start_cursor = int(seq.read().strip())
    except Exception:
        pass

    params = None
    if start_cursor is not None:
        params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=start_cursor)

    client = AsyncFirehoseSubscribeReposClient(params)

    # use run() for a higher Python version
    asyncio.get_event_loop().run_until_complete(main(client))
