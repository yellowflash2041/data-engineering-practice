import os
import zipfile
import tempfile

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    to_date,
    col,
    unix_timestamp,
    month,
    rank,
    count,
    desc,
    row_number,
    avg,
)
from datetime import timedelta


def create_dataframes(spark: SparkSession, directory_path: str):
    """
    This function creates a list of Spark DataFrames from a directory of zip files.

    Args:
        spark (SparkSession): The Spark session.
        directory_path (str): The path to the directory containing the zip files.

    Returns:
        list: A list of Spark DataFrames.
    """
    # Create an empty list to store the DataFrames
    dataframes = []

    # Iterate over the files in the directory
    for file_name in os.listdir(directory_path):
        # Check if the file is a zip file
        if file_name.endswith(".zip"):
            # Construct the full path to the zip file
            zip_file_path = os.path.join(directory_path, file_name)
            # Extend the list of DataFrames with the DataFrames from the zip file
            dataframes.extend(_create_dataframes_from_zip(spark, zip_file_path))

    # Return the list of DataFrames
    return dataframes


def _create_dataframes_from_zip(spark: SparkSession, zip_file_path: str):
    """
    This function creates a list of Spark DataFrames from a zip file.

    Args:
        spark (SparkSession): The Spark session.
        zip_file_path (str): The path to the zip file.

    Returns:
        list: A list of Spark DataFrames.
    """
    # Create an empty list to store the DataFrames
    dataframes = []

    # Open the zip file in read mode
    with zipfile.ZipFile(zip_file_path, "r") as zip_file:
        # Iterate over the files in the zip file
        for file_info in zip_file.infolist():
            # Check if the file is a csv file and not a macosx file
            if not file_info.filename.startswith(
                "__MACOSX/"
            ) and file_info.filename.endswith(".csv"):
                # Create a DataFrame from the csv file and append it to the list
                df = _create_dataframe_from_csv(spark, zip_file, file_info)
                dataframes.append(df)

    # Return the list of DataFrames
    return dataframes


def _create_dataframe_from_csv(
    spark: SparkSession, zip_file: zipfile.ZipFile, file_info
):
    """
    Create a Spark DataFrame from a CSV file in a zip file.

    Args:
        spark (SparkSession): The Spark session.
        zip_file (ZipFile): The zip file containing the CSV file.
        file_info (ZipInfo): The information about the CSV file in the zip file.

    Returns:
        DataFrame: The Spark DataFrame created from the CSV file.
    """
    # Create a temporary file to store the CSV data
    with tempfile.NamedTemporaryFile(delete=False) as temp_csv_file:
        # Open the CSV file in the zip file and write its contents to the temporary file
        with zip_file.open(file_info) as csv_file:
            temp_csv_file.write(csv_file.read())

    # Read the CSV file from the temporary file into a Spark DataFrame
    return spark.read.csv(temp_csv_file.name, header=True, inferSchema=True)


def output_result(result, output_path: str, name: str):
    """
    Writes the result DataFrame to a CSV file in the specified output path.

    Args:
        result (DataFrame): The DataFrame to be written to a CSV file.
        output_path (str): The path to the directory where the CSV file will be created.
        name (str): The name of the CSV file.
    """
    try:
        # Write the result DataFrame to a CSV file in the output path
        result.coalesce(1).write.option("header", "true").csv(output_path)

        # Rename the created CSV file to the specified name
        files = os.listdir(output_path)
        for file in files:
            if file.endswith(".csv"):
                csv_file_path = os.path.join(output_path, file)
                new_csv_file_path = os.path.join(output_path, name)
                os.rename(csv_file_path, new_csv_file_path)
            else:
                # Delete any non-CSV files in the output path
                file_to_delete = os.path.join(output_path, file)
                os.remove(file_to_delete)

    except Exception as e:
        # If the file already exists, print a message indicating that
        if "exists" in str(e):
            print(f"!!!File {name} already exists!!!")
        # If any other exception occurs, print the error message
        else:
            print(e)


def avg_trip_duration_per_day(dataframes: list, output_path: str):
    """
    Calculates the average trip duration per day from the given dataframes and writes the result to a CSV file.

    Args:
        dataframes (list): A list of Spark DataFrames containing trip data.
        output_path (str): The path to the directory where the CSV file will be created.
    """
    # Initialize an empty combined DataFrame
    combined_df = None

    # Iterate over each dataframe
    for df in dataframes:
        # Rename the columns if they exist
        if "start_time" in df.columns:
            df = df.withColumnRenamed("start_time", "started_at")
            df = df.withColumnRenamed("from_station_name", "start_station_name")
            df = df.withColumnRenamed("end_time", "ended_at")

        # Extract the day from the started_at column and calculate the trip duration
        df = df.withColumn("day", to_date(col("started_at")))
        df = df.withColumn(
            "trip_duration", (unix_timestamp("ended_at") - unix_timestamp("started_at"))
        )
        # Select only the day and trip_duration columns
        df = df.select("day", "trip_duration")

        # Union the DataFrames if combined_df is not None
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    # Calculate the average trip duration per day and sort the result by day
    not_sorted_result = combined_df.groupBy("day").agg({"trip_duration": "avg"})
    result = not_sorted_result.orderBy("day")

    # Write the result to a CSV file and rename it
    output_result(result, output_path, "trip_duration.csv")


def trips_per_day(dataframes: list, output_path: str):
    """
    Calculates the number of trips per day from the given dataframes and writes the result to a CSV file.

    Args:
        dataframes (list): A list of Spark DataFrames containing trip data.
        output_path (str): The path to the directory where the CSV file will be created.
    """
    combined_df = None

    # Iterate over each dataframe
    for df in dataframes:
        # Rename the columns if they exist
        if "started_at" in df.columns:
            df = df.withColumnRenamed("started_at", "start_time")
        df = df.withColumn("date", to_date(col("start_time")))
        df = df.select("date")

        # Union the DataFrames if combined_df is not None
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    # Calculate the number of trips per day and order the result by date
    not_sorted_result = (
        combined_df.groupBy("date").count().withColumnRenamed("count", "trip_count")
    )
    result = not_sorted_result.orderBy("date")

    # Write the result to a CSV file
    output_result(result, output_path, "trips_per_day.csv")


def popular_month_station(dataframes: list, output_path: str):
    """
    Calculate the most popular station per month based on the given dataframes.

    Args:
        dataframes (list): A list of Spark DataFrames containing trip data.
        output_path (str): The path to the directory where the CSV file will be created.
    """
    # Initialize combined_df to None
    combined_df = None

    # Iterate over each dataframe
    for df in dataframes:
        # Rename the columns if they exist
        if "started_at" in df.columns:
            df = df.withColumnRenamed("started_at", "start_time")
            df = df.withColumnRenamed("start_station_name", "from_station_name")

        # Extract the month from the start_time column
        df = df.withColumn("month", month(col("start_time")))

        # Group by month and from_station_name and count the trips
        df = df.groupBy("month", "from_station_name").agg({"*": "count"})
        df = df.withColumnRenamed("count(1)", "trip_count")

        # Union the DataFrames
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    # Calculate the rank of each station within each month
    window_spec = Window.partitionBy("month").orderBy(col("trip_count").desc())
    popular_station_by_month = combined_df.withColumn("rank", rank().over(window_spec))

    # Filter the most popular station for each month
    popular_station_by_month = popular_station_by_month.filter(col("rank") == 1)
    popular_station_by_month = popular_station_by_month.select(
        "month", "from_station_name", "trip_count"
    )

    # Order the result by month
    result = popular_station_by_month.orderBy("month")

    # Write the result to a CSV file
    output_result(result, output_path, "popular_station_per_month.csv")


def filter_last_two_weeks(dataframe, start_time_column: str):
    """
    This function filters the dataframe to include only the data from the last two weeks based on the start_time_column.

    Args:
        dataframe (DataFrame): The input dataframe containing the data to be filtered.
        start_time_column (str): The name of the column representing the start time of each record.

    Returns:
        DataFrame: The filtered dataframe containing data from the last two weeks.
    """
    try:
        # Find the latest start time
        latest_start_time = dataframe.agg({start_time_column: "max"}).collect()[0][0]
        two_weeks_ago = latest_start_time - timedelta(days=14)

        # Filter the dataframe for data within the last two weeks
        filtered_dataframe = dataframe.filter(
            (col(start_time_column) >= two_weeks_ago)
            & (col(start_time_column) <= latest_start_time)
        )

        return filtered_dataframe

    except Exception:
        return dataframe


def top_station_by_day(dataframes: list, output_path: str):
    """
    Calculates the top 3 stations for each day in the given dataframes
    and writes the result to a CSV file.

    Args:
        dataframes (list): A list of Spark DataFrames containing trip data.
        output_path (str): The path to the directory where the CSV file will be created.
    """
    # Initialize an empty combined DataFrame
    combined_df = None

    # Iterate over each dataframe
    for df in dataframes:
        # Rename the columns if they exist
        if "start_time" in df.columns:
            df = df.withColumnRenamed("start_time", "started_at")
            df = df.withColumnRenamed("from_station_name", "start_station_name")

        # Filter the data for the last two weeks
        df = filter_last_two_weeks(df, "started_at")

        # Select the required columns and convert started_at to date
        df = df.select("started_at", "start_station_name")
        df = df.withColumn("started_at", to_date(df["started_at"]))

        # Group by date and start station, count the trips, and rank the stations
        df = df.groupBy("started_at", "start_station_name").agg(
            count("*").alias("trip_count")
        )
        window_spec = Window.partitionBy("started_at").orderBy(desc("trip_count"))
        ranked_df = df.withColumn("station_rank", row_number().over(window_spec))
        df = ranked_df.filter(col("station_rank") <= 3)

        # Union the DataFrames if combined_df is not None
        combined_df = df if combined_df is None else combined_df.union(df)

    # Order the result by date
    result = combined_df.orderBy("started_at")

    # Write the result to a CSV file
    output_result(result, output_path, "top_station_by_day.csv")


def calculate_longest_trips(dataframes: list, output_path: str):
    """
    Calculates the average trip duration per gender and writes the result to a CSV file.

    Args:
        dataframes (list): A list of Spark DataFrames containing trip data.
        output_path (str): The path to the directory where the CSV file will be created.
    """
    # Iterate over each dataframe
    for df in dataframes:
        try:
            # Select the required columns and calculate the trip duration
            df = df.select("start_time", "end_time", "gender")
            df = df.withColumn(
                "trip_duration",
                (unix_timestamp("end_time") - unix_timestamp("start_time")),
            )

            # Calculate the average trip duration per gender
            average_trip_duration = df.groupBy("gender").agg(avg("trip_duration"))
        except Exception:
            # Continue to the next dataframe if an error occurs
            continue

    # Write the result to a CSV file
    output_result(average_trip_duration, output_path, "longest_trips.csv")


def top_ages(dataframes: list, output_path: str):
    """
    Calculates the top 10 ages with the longest and shortest trip durations and writes the result to a CSV file.

    Args:
        dataframes (list): A list of Spark DataFrames containing trip data.
        output_path (str): The path to the directory where the CSV file will be created.
    """
    # Iterate over each dataframe
    final_result = None
    for df in dataframes:
        try:
            # Select required columns and calculate trip duration
            df = df.select("start_time", "end_time", "birthyear")
            df = df.withColumn(
                "trip_duration",
                (unix_timestamp("end_time") - unix_timestamp("start_time")),
            )
            df = df.select("birthyear", "trip_duration")
            df = df.filter(col("trip_duration") >= 0)
            df = df.filter(col("birthyear").cast("double").isNotNull())

            # Sort the DataFrame by trip duration and select the top and bottom 10 rows
            df = df.orderBy("trip_duration", ascending=False)
            top_10 = df.limit(10)
            df = df.orderBy("trip_duration")
            bottom_10 = df.limit(10)

            # Union the DataFrames if final_result is not None
            final_result = (
                top_10.union(bottom_10)
                if final_result is None
                else final_result.union(top_10.union(bottom_10))
            )
        except Exception:
            # Continue to the next dataframe if an error occurs
            continue

    # Write the result to a CSV file
    output_result(final_result, output_path, "top_ages.csv")


def perform_analysis(spark: SparkSession, directory_path: str, output_paths: list):
    """
    Perform analysis on the dataframes and write the results to CSV files.

    Args:
        spark (SparkSession): The SparkSession object.
        directory_path (str): The path to the directory containing the dataframes.
        output_paths (list): The list of paths to the directories where the CSV files will be created.
    """
    # Create dataframes from the data in the directory
    dataframes = create_dataframes(spark, directory_path)

    # Iterate over the list of functions and output paths
    for function, output_path in zip(
        [
            avg_trip_duration_per_day,
            trips_per_day,
            popular_month_station,
            top_station_by_day,
            calculate_longest_trips,
            top_ages,
        ],
        output_paths,
    ):
        # Call the function with the dataframes and output path
        function(dataframes, output_path)
        # Add a comment to indicate that the function has been called
        # with the corresponding dataframes and output path


def main():
    """
    The main function to execute the analysis.

    This function initializes the SparkSession, performs analysis on the data,
    and writes the results to CSV files in the reports directory.
    """
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    # your code here
    perform_analysis(
        spark,
        "./data",
        [
            "reports/analysis_1",
            "reports/analysis_2",
            "reports/analysis_3",
            "reports/analysis_4",
            "reports/analysis_5",
            "reports/analysis_6",
        ],
    )


if __name__ == "__main__":
    main()
