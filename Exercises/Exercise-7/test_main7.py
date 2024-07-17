import os
import shutil

from pyspark.sql import SparkSession
from main import create_spark


def test_create_spark():
    """Tests the create_spark function."""
    temp_reports_dir = "./temp_test_reports"

    try:
        spark = (
            SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
        )
        create_spark(spark, "./data", temp_reports_dir, "exercise7.csv")

        report_path = os.path.join(temp_reports_dir, "exercise7.csv")
        report_df = spark.read.csv(report_path, header=True, inferSchema=True)
        report_dict = report_df.rdd.map(lambda row: row.asDict()).collect()

        expected_report_dict = [
            {
                "date": "2022-01-01",
                "serial_number": "ZA1FLE1P",
                "model": "ST8000NM0055",
                "capacity_bytes": 8001563222016,
                "failure": 0,
                "source_file": "hard-drive-2022-01-01-failures.csv.zip",
                "file_date": "2022-03-13",
                "brand": "unknown",
                "rank": 2,
                "primary_key": "e0060b696e2b946c548009a4e600fdfb8816baf4",
            },
            {
                "date": "2022-01-01",
                "serial_number": "1050A084F97G",
                "model": "TOSHIBA MG07ACA14TA",
                "capacity_bytes": 14000519643136,
                "failure": 0,
                "source_file": "hard-drive-2022-01-01-failures.csv.zip",
                "file_date": "2022-03-13",
                "brand": "TOSHIBA",
                "rank": 1,
                "primary_key": "2d79ebea27b0f4822f2c747b0e7f7d36ef2deb11",
            },
        ]
        assert report_dict == expected_report_dict
    finally:
        shutil.rmtree(temp_reports_dir)


if __name__ == "__main__":
    test_create_spark()
