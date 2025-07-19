import aioboto3


async def upload_to_minio(
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        data: str,
        key: str,
):
    session = aioboto3.Session()
    async with session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
    ) as s3:
        await s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data.encode("utf-8"),
            ContentType="application/json"
        )
