import polars as pl


def read_data(file_path: str) -> pl.DataFrame:
    """
    Reads the data from the given file path and returns a polars DataFrame.

    Args:
        file_path (str): The path to the file.

    Returns:
        pl.DataFrame: The DataFrame containing the data.
    """
    # Read the CSV file and specify the data types for each column.
    # The dtypes dictionary maps column names to polars data types.
    return pl.read_csv(
        file_path,
        columns=["started_at"],
        dtypes={
            "ride_id": pl.Utf8,  # Unicode type for ride_id column
            "rideable_type": pl.Categorical,  # Categorical type for rideable_type column
            "started_at": pl.Datetime,  # Datetime type for started_at column
            "ended_at": pl.Datetime,  # Datetime type for ended_at column
            "start_station_name": pl.Utf8,  # Unicode type for start_station_name column
            "start_station_id": pl.Int32,  # Integer type for start_station_id column
            "end_station_name": pl.Utf8,  # Unicode type for end_station_name column
            "end_station_id": pl.Int32,  # Integer type for end_station_id column
            "start_lat": pl.Float32,  # Float type for start_lat column
            "start_lng": pl.Float32,  # Float type for start_lng column
            "end_lat": pl.Float32,  # Float type for end_lat column
            "end_lng": pl.Float32,  # Float type for end_lng column
            "member_casual": pl.Categorical  # Categorical type for member_casual column
        }
    )



def rides_per_day(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the number of rides per day from the given DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame containing the trip data.

    Returns:
        pl.DataFrame: The DataFrame with the number of rides per day.
    """
    # Add a new column "started_at_date" which contains the date part of the "started_at" column
    # The .dt.date() function extracts the date part from the "started_at" column
    # The .alias() function renames the new column to "started_at_date"
    df = df.with_columns(pl.col("started_at").dt.date().alias("started_at_date"))

    # Group the DataFrame by "started_at_date" and calculate the count of rows
    # The .count() function counts the number of rows in each group
    return df.group_by("started_at_date").count()


def aggregates_per_week(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the aggregates (mean, min, max) of the number of rides per week.

    Args:
        df (pl.DataFrame): The DataFrame containing the trip data.

    Returns:
        pl.DataFrame: The DataFrame containing the aggregates.
    """
    weekly_rides = (
        df.with_columns(pl.col("started_at").dt.week().alias("week"))
        .group_by("week")
        .agg(pl.col("started_at").count().alias("rides"))
    )
    return weekly_rides.select(
        pl.mean("rides").alias("mean"),
        pl.min("rides").alias("min"),
        pl.max("rides").alias("max")
    )


def diff_to_last_week(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the difference in number of rides between the current week and the previous week.

    Args:
        df (pl.DataFrame): The DataFrame containing the trip data.

    Returns:
        pl.DataFrame: The DataFrame with the number of rides, the number of rides from the previous week, and the difference.
    """
    # Sort the DataFrame by the "started_at_date" column
    # Add a new column "count_last_week" which contains the count of rides from the previous week
    # Subtract the count from the previous week from the count of rides from the current week
    # Return the resulting DataFrame
    return (
        df.sort("started_at_date")
        .with_columns(
            pl.col("count").shift(7).alias("count_last_week")
        )
        .select(
            pl.col("count"),  # Number of rides from the current week
            pl.col("count_last_week"),  # Number of rides from the previous week
            (pl.col("count") - pl.col("count_last_week")).alias("diff_to_last_week")  # Difference in number of rides
        )
    )


def main() -> None:
    """Main entry point for script."""
    trips_df = read_data("data/202306-divvy-tripdata.csv")
    daily_trips_df = rides_per_day(trips_df)
    print(aggregates_per_week(trips_df))
    print(diff_to_last_week(daily_trips_df).with_columns(pl.col("count").cast(pl.Int32)))


if __name__ == "__main__":
    main()
