import boto3
import gzip
import tempfile
import os

from dotenv import load_dotenv

load_dotenv()


def get_first_uri(s3_client: boto3.client, bucket_name: str, file_key: str):
    """
    Retrieves the first URI from a gzip-compressed file in an S3 bucket.

    Args:
        s3_client (boto3.client): The S3 client.
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key of the file in the bucket.

    Returns:
        str: The first URI in the file.

    Raises:
        Exception: If there is an error retrieving the file or decompressing it.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        gzipped_data = response["Body"].read()
        decompressed_data = gzip.decompress(gzipped_data).decode("utf-8")
        first_uri = decompressed_data.split("\n", 1)[0].strip()
        return first_uri
    except Exception as e:
        raise e


def download_and_process_file_from_uri(s3_client, bucket_name, file_key):
    """
    Downloads a file from an S3 bucket, extracts it from a gzip archive,
    and prints each line of the extracted file to stdout.
    """
    temp_dir = tempfile.TemporaryDirectory()

    try:
        local_file_path = os.path.join(temp_dir.name, os.path.basename(file_key))
        s3_client.download_file(bucket_name, file_key, local_file_path)
        with gzip.open(local_file_path, "rt", encoding="utf-8") as gz_file:
            for line in gz_file:
                print(line.strip())

    finally:
        temp_dir.cleanup()


def main():
    """
    Downloads a file from an S3 bucket, extracts it from a gzip archive,
    and prints each line of the extracted file to stdout.
    """
    # Set up the S3 client
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3 = boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )

    # Set up the bucket and file details
    bucket_name = "commoncrawl"
    file_key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    # Get the first URI from the file and download and process it
    uri = get_first_uri(s3, bucket_name, file_key)
    download_and_process_file_from_uri(s3, bucket_name, uri)


if __name__ == "__main__":
    main()
