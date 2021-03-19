import boto3
from moto import mock_dynamodb2, mock_s3

from datawarehouse import implementations


REGION = "us-east-1"
SOURCE_BUCKET = "source-bucket"
PARSED_BUCKET = "parsed-bucket"
REGISTRY_TABLE = "registry-table"
SOURCE_TABLE = "source-table"


s3 = mock_s3()
ddb = mock_dynamodb2()


def mock_start():
    s3.start()
    ddb.start()


def mock_stop():
    s3.stop()
    ddb.stop()


def setup_resources():
    create_bucket(SOURCE_BUCKET)
    create_bucket(PARSED_BUCKET)
    create_registry_table(REGISTRY_TABLE, REGION)
    create_source_table(SOURCE_TABLE, REGION)


# helper method to get a fresh warehouse instance.
def get_warehouse_sesh(db=None, coll=None, ttl=None, prefix=None):
    return implementations.DynamoWarehouse(
        region_name=REGION,
        registry_table_name=REGISTRY_TABLE,
        source_table_name=SOURCE_TABLE,
        source_bucket_name=SOURCE_BUCKET,
        parsed_bucket_name=PARSED_BUCKET,
        bucket_prefix=prefix,
        cache_ttl=ttl,
        load_defaults=False,
        database=db,
        collection=coll,
    )


def create_bucket(bucket_name):
    boto3.client("s3").create_bucket(Bucket=bucket_name)


def create_registry_table(table_name, region_name):
    boto3.client("dynamodb", region_name=region_name).create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "feed_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "feed_id", "KeyType": "HASH"}],
    )


def create_source_table(table_name, region_name):
    boto3.client("dynamodb", region_name=region_name).create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "file_key", "KeyType": "HASH"},
            {"AttributeName": "source_version", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "ContentStartIndex",
                "KeySchema": [
                    {"AttributeName": "feed_id", "KeyType": "HASH"},
                    {"AttributeName": "content_start", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "ReleaseDateIndex",
                "KeySchema": [
                    {"AttributeName": "feed_id", "KeyType": "HASH"},
                    {"AttributeName": "release_date", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        AttributeDefinitions=[
            {"AttributeName": "file_key", "AttributeType": "S"},
            {"AttributeName": "source_version", "AttributeType": "S"},
            {"AttributeName": "content_start", "AttributeType": "N"},
            {"AttributeName": "release_date", "AttributeType": "N"},
            {"AttributeName": "feed_id", "AttributeType": "S"},
        ],
    )
