import requests
import pandas
import io

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def find_filenames_regex(base_url: str) -> list:
    """
    Find filenames in base_url that match the pattern '[A-z0-9]{11}.csv'
    and were modified on '2024-01-19 10:45'.
    """
    import re

    response = requests.get(base_url, timeout=16)
    text = response.text

    pattern = re.compile(r"([A-z0-9]{11}\.csv)")
    filenames = pattern.findall(text)

    modified_lines = [
        line for line in text.splitlines() if re.search(r"2024-01-19 10:45\s{2}", line)
    ]
    urls = [
        f"{base_url}{filename}"
        for filename in filenames
        if any(filename in line for line in modified_lines)
    ]

    return urls


def download_and_print_max_temperature(file_urls: list) -> None:
    """
    Download the CSV files from the provided URLs, extract the maximum hourly dry bulb temperature,
    and print the corresponding row from each file.

    Args:
        file_urls (list): List of URLs pointing to CSV files

    Returns:
        None
    """
    for file_url in file_urls:
        # Download the file from the URL
        response = requests.get(file_url, timeout=60)

        # Read the CSV data into a DataFrame
        csv_data = io.BytesIO(response.content)
        data_frame = pandas.read_csv(csv_data)

        # Extract the hourly dry bulb temperature data
        temperature_data = pandas.to_numeric(
            data_frame["HourlyDryBulbTemperature"], errors="coerce"
        )

        # Find the row with the maximum temperature
        max_temperature_row = data_frame.loc[temperature_data.idxmax()]

        # Print the row with the maximum temperature
        print(max_temperature_row)


def main():
    """
    Main function to execute the program.
    """
    file_urls = find_filenames_regex(url)
    download_and_print_max_temperature(file_urls)


if __name__ == "__main__":
    main()
