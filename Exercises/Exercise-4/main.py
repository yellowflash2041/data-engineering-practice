import json
import csv

from pathlib import Path


def flatten_json(file_path: str) -> dict:
    """Flatten a JSON file into a dictionary."""
    flat_dict = {}

    def flatten(key, value):
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                flatten(f"{key}_{nested_key}", nested_value)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                flatten(f"{key}_{index}", item)
        else:
            flat_dict[key] = value

    with open(file_path, "r") as json_file:
        data = json.load(json_file)
        for key, value in data.items():
            flatten(key, value)

    return flat_dict


def write_flattened_data_to_csv(
    flattened_data_list: list, output_file_path: str
) -> None:
    """
    Writes flattened data to a CSV file.

    Args:
        flattened_data_list (list): List of dictionaries containing flattened data.
        output_file_path (str): Path to the output CSV file.

    Returns:
        None
    """
    with open(output_file_path, "w", newline="") as csv_file:
        fieldnames = flattened_data_list[0].keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened_data_list)


def main():
    """
    Main function to flatten JSON files and write them to a CSV file.
    """
    # Get a list of all JSON files in the 'data' directory
    json_files = list(Path("data").rglob("*.json"))

    # Flatten each JSON file
    flattened_data = [flatten_json(file) for file in json_files]

    # Check if all flattened data have the same keys
    if len(set(frozenset(data.keys()) for data in flattened_data)) != 1:
        raise Exception("JSON files have different keys")

    # Write the flattened data to a CSV file
    output_file_path = Path(__file__).parent / "output.csv"
    write_flattened_data_to_csv(flattened_data, output_file_path)


if __name__ == "__main__":
    main()
