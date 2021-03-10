import boto3


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
            {"AttributeName": "request_key", "KeyType": "HASH"},
            {"AttributeName": "retrieved_date", "KeyType": "RANGE"},
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
            {"AttributeName": "request_key", "AttributeType": "S"},
            {"AttributeName": "retrieved_date", "AttributeType": "N"},
            {"AttributeName": "content_start", "AttributeType": "N"},
            {"AttributeName": "release_date", "AttributeType": "N"},
            {"AttributeName": "feed_id", "AttributeType": "S"},
        ],
    )
