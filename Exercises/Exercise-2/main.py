import requests
import pandas
import io

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def find_filenames_regex(url: str) -> list:
    import re

    with requests.get(url, timeout=16) as response:
        pattern = re.compile(r"([A-z0-9]{11}\.csv)")

        s = [
            pattern.search(i)
            for i in response.text.splitlines()
            if re.search(r"2022-02-07 14:03\s{2}", i)
        ]

        return [f"{url}{i.group(0)}" for i in s if i]


def download_and_print(url_list: list) -> None:
    for e in url_list:
        res = requests.get(e, timeout=60)

        df = pandas.read_csv(io.BytesIO(res.content)).assign(
            HourlyDryBulbTemperature=lambda x: pandas.to_numeric(
                x.HourlyDryBulbTemperature, errors="coerce"
            )
        )
        print(df.loc[df.HourlyDryBulbTemperature.idxmax()])


def main():
    # your code here
    url_list = find_filenames_regex(url)

    download_and_print(url_list)
    pass


if __name__ == "__main__":
    main()
