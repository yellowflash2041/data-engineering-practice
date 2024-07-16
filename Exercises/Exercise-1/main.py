import requests
import zipfile
import io

from pathlib import Path

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


def download_and_extract(url: str, directory: Path):
    """
    Downloads a file from the given URL and extracts its contents to the specified directory.

    Args:
        url (str): The URL of the file to download.
        directory (Path): The directory where the extracted files will be saved.
    """
    with requests.Session() as session:
        with session.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            zip_content = io.BytesIO(response.content)
            with zipfile.ZipFile(zip_content) as zip_file:
                zip_file.extractall(directory)


def main():
    """
    Downloads and extracts files from the given URLs to the specified directory.
    """
    for url in download_uris:
        download_and_extract(url, download_directory)


if __name__ == "__main__":
    main()
