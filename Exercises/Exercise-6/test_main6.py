import tempfile
import os

from main import perform_analysis
from pyspark.sql import SparkSession


def test_combination_of_analysis():
    """
    Test the combination of analysis functions.

    This test function creates temporary directories for analysis paths,
    initializes the SparkSession, performs analysis on the data,
    compares the expected and actual paths, and asserts their equality.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create analysis paths for testing
        analysis_paths = [os.path.join(temp_dir, f"analysis_{i}") for i in range(1, 7)]

        # Initialize SparkSession for testing
        spark = (
            SparkSession.builder.appName("TestExercise6")
            .enableHiveSupport()
            .getOrCreate()
        )

        # Perform analysis for testing
        perform_analysis(spark, "./data", analysis_paths)

        # Define expected paths for testing
        expected_paths = [f"analysis_{i}" for i in range(1, 7)]

        # Get actual paths for testing
        actual_paths = [
            os.path.basename(path)
            for path in os.listdir(temp_dir)
            if os.path.isdir(os.path.join(temp_dir, path))
        ]

        # Assert the equality of actual and expected paths
        assert actual_paths == expected_paths


if __name__ == "__main__":
    test_combination_of_analysis()
