from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
import zipfile
import tempfile


def create_dataframe(directory_path: str, spark_session: SparkSession):
    """
    Creates a single dataframe from all csv files in a directory of zip files.

    Args:
        directory_path (str): The path to the directory containing zip files.
        spark_session (SparkSession): The SparkSession object to use.

    Returns:
        pyspark.sql.DataFrame: A dataframe containing data from all csv files.
    """
    dataframe = None

    # Iterate over all files in the directory
    for file_name in os.listdir(directory_path):
        if file_name.endswith(".zip"):
            zip_file_path = os.path.join(directory_path, file_name)

            # Open the zip file
            with zipfile.ZipFile(zip_file_path, "r") as zip_file:
                # Iterate over all files in the zip file
                for file_info in zip_file.infolist():
                    # Check if the file is a csv file and not a macosx file
                    if (
                        file_info.filename.endswith(".csv")
                        and "__MACOSX" not in file_info.filename
                    ):
                        # Create a temporary file to store the csv data
                        with tempfile.NamedTemporaryFile(delete=False) as temp_csv_file:
                            # Open the csv file in the zip file and write its contents to the temporary file
                            with zip_file.open(file_info) as csv_file:
                                temp_csv_file.write(csv_file.read())

                        # Read the csv file from the temporary file into a dataframe
                        current_dataframe = spark_session.read.csv(
                            temp_csv_file.name, header=True, inferSchema=True
                        )

                        # Add the name of the source file to the dataframe
                        current_dataframe = current_dataframe.withColumn(
                            "source_file", F.lit(file_name)
                        )

                        # Append the dataframe to the existing dataframe
                        if dataframe is None:
                            dataframe = current_dataframe
                        else:
                            dataframe = dataframe.union(current_dataframe)

    return dataframe


def extract_data_from_source_file(dataframe):
    """
    Extracts information from source file and adds it to the dataframe.

    Args:
        dataframe (pyspark.sql.DataFrame): The dataframe containing source file data.

    Returns:
        pyspark.sql.DataFrame: The modified dataframe with additional columns.
    """
    date_pattern = r"(\d{4}-\d{2}-\d{2})"

    dataframe = (
        dataframe.withColumn(
            "file_date",
            F.to_date(F.regexp_extract(dataframe["source_file"], date_pattern, 1)),
        )
        .withColumn(
            "brand",
            F.when(
                dataframe["model"].contains(" "),
                F.split(dataframe["model"], " ")[0],
            ).otherwise("unknown"),
        )
        .withColumn(
            "primary_key",
            F.sha1(F.concat_ws("_", *dataframe[["serial_number", "failure", "model"]])),
        )
    )

    capacity_bytes = (
        dataframe.select("capacity_bytes").distinct().orderBy("capacity_bytes")
    )

    window_spec = Window.orderBy(F.desc("capacity_bytes"))
    capacity_bytes = capacity_bytes.withColumn("rank", F.dense_rank().over(window_spec))
    capacity_bytes = capacity_bytes.dropDuplicates(["rank"])

    dataframe = (
        dataframe.join(
            capacity_bytes,
            dataframe["capacity_bytes"] == capacity_bytes["capacity_bytes"],
            "inner",
        )
        .drop("capacity_bytes")
        .drop("rank")
    )

    return dataframe


def write_dataframe_to_csv(dataframe, output_directory: str, output_filename: str):
    """
    Writes a DataFrame to a CSV file in the specified output directory.

    Args:
        dataframe (DataFrame): The DataFrame to write to a CSV file.
        output_directory (str): The path to the directory where the CSV file will be created.
        output_filename (str): The name of the CSV file.

    Raises:
        Exception: If the file already exists.
    """
    try:
        dataframe.coalesce(1).write.option("header", True).csv(output_directory)

        csv_files = [
            file for file in os.listdir(output_directory) if file.endswith(".csv")
        ]

        if csv_files:
            os.rename(
                os.path.join(output_directory, csv_files[0]),
                os.path.join(output_directory, output_filename),
            )

        other_files = [
            file for file in os.listdir(output_directory) if not file.endswith(".csv")
        ]
        for file in other_files:
            file_path = os.path.join(output_directory, file)
            os.remove(file_path)

    except Exception as error:
        if "exists" in str(error):
            raise Exception(f"File {output_filename} already exists")
        else:
            raise


def create_spark(spark: SparkSession, directory_path: str, output_path: str, name: str):
    """Creates a Spark dataframe from a directory of zip files and writes it to a CSV file.

    Args:
        spark (SparkSession): The Spark session.
        directory_path (str): The path to the directory containing the zip files.
        output_path (str): The path to the directory where the CSV file will be created.
        name (str): The name of the CSV file.
    """
    df = create_dataframe(directory_path, spark)
    df = extract_data_from_source_file(df)
    write_dataframe_to_csv(df, output_path, name)


def main():
    """
    The main function to execute the analysis.

    This function initializes the SparkSession, performs analysis on the data,
    and writes the results to a CSV file.
    """
    # Initialize the SparkSession
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()

    # Set the directory paths
    data_directory = "./data"
    result_directory = "./result"
    output_filename = "exercise7.csv"

    # Create the Spark dataframe and write it to a CSV file
    create_spark(spark, data_directory, result_directory, output_filename)


if __name__ == "__main__":
    main()
