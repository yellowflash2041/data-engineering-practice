import json
import csv

from pathlib import Path


def flatten_json(path_to_json: str):
    output_dict = {}

    def _flatten(k, v):
        if isinstance(v, dict):
            for i, j in v.items():
                _flatten(i, j)
            return
        if isinstance(v, list):
            output_dict.update({f"{k}_{num}": el for num, el in enumerate(v)})
            return
        output_dict.update({k: v})

    with open(path_to_json, "r") as f:
        for k, v in json.load(f).items():
            _flatten(k, v)
    return output_dict


def write_csv_file(dict_list: list, path: str):
    with open(path, "w") as f:
        writer = csv.DictWriter(f, dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)


def main():
    # your code here
    file_list = list(Path("data").rglob("*.json"))
    dict_list = [flatten_json(e) for e in file_list]
    if all(e.keys() == dict_list[0].keys() for e in dict_list):
        write_csv_file(dict_list, Path(__file__).parent / "output.csv")
        return
    raise Exception("JSON files have different keys")
    # pass


if __name__ == "__main__":
    main()
