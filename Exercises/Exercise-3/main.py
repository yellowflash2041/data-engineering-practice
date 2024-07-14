import boto3
import gzip
import tempfile
import os

from dotenv import load_dotenv

load_dotenv()


def get_first_uri(s3, bucket_name, file_key):

    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        gzipped_content = response['Body'].read()
        new_key = gzip.decompress(gzipped_content).decode('utf-8').split('\n', 1)[0].strip()
        return new_key

    except Exception as e:
        raise e


def get_file_from_uri(s3, bucket_name, file_key):

    temp_folder = tempfile.TemporaryDirectory()

    try:
        local_file_path = temp_folder.name + '/' + file_key.split('/')[-1]
        s3.download_file(bucket_name, file_key, local_file_path)
        with gzip.open(local_file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                print(line.strip())

    except Exception as e:
        raise e


def main():
    # your code here
    bucket_name = 'commoncrawl'
    file_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    s3 = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

    get_file_from_uri(s3, bucket_name, get_first_uri(s3, bucket_name, file_key))
    pass


if __name__ == "__main__":
    main()
