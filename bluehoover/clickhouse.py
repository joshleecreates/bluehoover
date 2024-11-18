import asyncio
from datetime import datetime
import signal
from collections import defaultdict
from types import FrameType
from typing import List
import clickhouse_connect
from clickhouse_connect.driver.client import Client

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

BATCH_SIZE = 256
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = 'default'
CLICKHOUSE_TABLE = 'posts'

post_queue: asyncio.Queue = asyncio.Queue()


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> defaultdict:
    operation_by_type = defaultdict(lambda: {'created': [], 'deleted': []})

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo, 'path': op.path}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)
            record_type = _INTERESTED_RECORDS.get(uri.collection)
            if record_type and models.is_record_type(record, record_type):
                operation_by_type[uri.collection]['created'].append({'record': record, **create_info})

        if op.action == 'delete':
            operation_by_type[uri.collection]['deleted'].append({'uri': str(uri)})

    return operation_by_type


async def process_batch(client: Client, batch: List[dict]) -> None:
    try:
        # Prepare data for insertion
        cids = [post['cid'] for post in batch]
        uris = [post['uri'] for post in batch]
        created_ats = [datetime.fromisoformat(post['created_at'].replace('Z', '+00:00')) for post in batch]
        authors = [post['author'] for post in batch]
        texts = [post['text'] for post in batch]
        reply_parent_uris = [post['reply_parent_uri'] or '' for post in batch]

        data = [
            cids,
            uris,
            created_ats,
            authors,
            texts,
            reply_parent_uris,
        ]

        # Insert data into ClickHouse
        client.insert(CLICKHOUSE_TABLE, data, column_names=['cid', 'uri', 'created_at', 'author', 'text', 'reply_parent_uri'], column_oriented=True)
        print(f"Inserted batch of {len(batch)} posts")
    except Exception as e:
        print(f"Error inserting batch: {str(e)}")

async def batch_processor() -> None:
    # Create ClickHouse client
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE
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
    client.command(create_table_query)

    print("Batch processor started")
    
    batch = []
    while True:
        try:
            # Get post from queue
            post = await post_queue.get()
            batch.append(post)
            
            # Process batch if it reaches BATCH_SIZE
            if len(batch) >= BATCH_SIZE:
                await process_batch(client, batch)
                batch = []
                
        except Exception as e:
            print(f"Error in batch processor: {e}")
        finally:
            post_queue.task_done()

async def queue_for_insertion(post: dict) -> None:
    await post_queue.put(post)

async def signal_handler(_: int, __: FrameType) -> None:
    print('Keyboard interrupt received. Stopping...')
    
    # Wait for queue to be empty with a 10-second timeout
    if not post_queue.empty():
        print('Waiting for remaining posts to be processed (5s timeout)...')
        try:
            await asyncio.wait_for(post_queue.join(), timeout=5.0)
            print('Successfully processed remaining posts')
        except asyncio.TimeoutError:
            print('Timeout reached while processing remaining posts')
    
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
            firehose_client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=commit.seq))

        if not commit.blocks:
            return

        ops = _get_ops_by_type(commit)
        for created_post in ops[models.ids.AppBskyFeedPost]['created']:
            post = {
                'cid': created_post['cid'],
                'uri': created_post['uri'],
                'created_at': created_post['record'].created_at,
                'author': created_post['author'],
                'text': created_post['record'].text,
                'reply_parent_uri': created_post['record'].reply.parent.uri if created_post['record'].reply else None,
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

if __name__ == '__main__':
    signal.signal(signal.SIGINT, lambda _, __: asyncio.create_task(signal_handler(_, __)))

    start_cursor = None

    params = None
    if start_cursor is not None:
        params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=start_cursor)

    client = AsyncFirehoseSubscribeReposClient(params)

    # use run() for a higher Python version
    asyncio.get_event_loop().run_until_complete(main(client))