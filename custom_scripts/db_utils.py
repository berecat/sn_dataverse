import sqlite3
import contextlib
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
    HuggingFaceMetadata,
)
import datetime as dt
from common import constants, utils
from collections import defaultdict

database = "/home/nabeix3/dev/data-universe/SqliteMinerStorage.sqlite"

# conn = sqlite3.connect("/home/nabeix3/dev/data-universe/SqliteMinerStorage.sqlite")
# cursor = conn.cursor()

# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# tables = cursor.fetchall()

# for table in tables:
#     print(table[0])

# conn.close()

def list_tables(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        print(table[0])
        
def show_table(conn, table):
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({table});")
    columns = cursor.fetchall()

    for col in columns:
        col_name = col[1]
        col_type = col[2]
        is_primary = col[5]  # 1 if primary key

        if is_primary:
            print(f"{col_name} ({col_type}) [PRIMARY KEY]")
        else:
            print(f"{col_name} ({col_type})")
        
def get_compressed_index():
    def _create_connection():
        # Create the database if it doesn't exist, defaulting to the local directory.
        # Use PARSE_DECLTYPES to convert accessed values into the appropriate type.
        connection = sqlite3.connect(
            database, detect_types=sqlite3.PARSE_DECLTYPES, timeout=60.0
        )
        # Allow this connection to parse results from returned rows by column name.
        connection.row_factory = sqlite3.Row

        return connection
    
    connection = _create_connection()
    
    cursor = connection.cursor()

    oldest_time_bucket_id = TimeBucket.from_datetime(
        dt.datetime.now()
        - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
    ).id

    # Get sum of content_size_bytes for all rows grouped by DataEntityBucket.
    cursor.execute(
        """SELECT SUM(contentSizeBytes) AS bucketSize, timeBucketId, source, label FROM DataEntity
                WHERE timeBucketId >= ?
                GROUP BY timeBucketId, source, label
                ORDER BY bucketSize DESC
                LIMIT ?
                """,
        [
            oldest_time_bucket_id,
            constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4,
        ],  # Always get the max for caching and truncate to each necessary size.
    )

    buckets_by_source_by_label = defaultdict(dict)

    for row in cursor:
        # Ensure the miner does not attempt to report more than the max DataEntityBucket size.
        size = (
            constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
            if row["bucketSize"]
            >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
            else row["bucketSize"]
        )

        label = row["label"] if row["label"] != "NULL" else None

        bucket = buckets_by_source_by_label[DataSource(row["source"])].get(
            label, CompressedEntityBucket(label=label)
        )
        bucket.sizes_bytes.append(size)
        bucket.time_bucket_ids.append(row["timeBucketId"])
        buckets_by_source_by_label[DataSource(row["source"])][
            label
        ] = bucket

    # Convert the buckets_by_source_by_label into a list of lists of CompressedEntityBucket and return
    cached_index_4 = CompressedMinerIndex(
        sources={
            source: list(labels_to_buckets.values())
            for source, labels_to_buckets in buckets_by_source_by_label.items()
        }
    )
    
    print(cached_index_4)


def merge_tables(source_db, dest_db, table_name):
    # Connect to destination database
    dest_conn = sqlite3.connect(dest_db)
    dest_cursor = dest_conn.cursor()

    # Attach the source database
    dest_cursor.execute(f"ATTACH DATABASE '{source_db}' AS src;")

    # Insert all rows (ignore conflicts on primary key)
    dest_cursor.execute(f"""
        INSERT OR IGNORE INTO {table_name}
        SELECT * FROM src.{table_name};
    """)

    # Commit and clean up
    dest_conn.commit()
    dest_cursor.execute("DETACH DATABASE src;")
    dest_conn.close()

    print(f"Data copied from {source_db} to {dest_db} successfully!")
    
if __name__ == "__main__":
    # merge_tables(
    #     source_db="db2.sqlite",
    #     dest_db="SqliteMinerStorage.sqlite",
    #     table_name="DataEntity"
    # )
    dest_db = "SqliteMinerStorage.sqlite"
    for i in range(0, 20):
        source_db = f"db/SqliteMinerStorage_{i}.sqlite"
        # source_db = f"SqliteMinerStorage.sqlite"
        
        merge_tables(
            source_db,
            dest_db,
            table_name="DataEntity"
        )
    
    # conn = sqlite3.connect("/home/nabeix3/dev/data-universe/SqliteMinerStorage.sqlite")
    # show_table(conn, "DataEntity")
    
    # list_tables(conn)
    
    
    # get_compressed_index()
    
    # conn.close()