import requests
import zipfile
import io

from pathlib import Path
from requests.exceptions import HTTPError

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

download_directory = Path(__file__).parent / "downloads"


def download_and_unzip(uri: str, download_directory: Path):
    with requests.Session() as s, s.get(uri, stream=True, timeout=60) as response:
        try:
            response.raise_for_status()
            zipfile.ZipFile(io.BytesIO(response.content)).extractall(download_directory)
        except HTTPError as h:
            print(f"Invalid request for {uri}")
            print(h)


def main():
    # your code here
    for uri in download_uris:
        download_and_unzip(uri, download_directory)
    pass


if __name__ == "__main__":
    main()
