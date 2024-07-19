import duckdb

from dataclasses import dataclass, field


@dataclass
class TableConfiguration:
    path: str
    table_name: str
    schema: dict
    kwargs: dict = field(default_factory=dict)


table_cfg = TableConfiguration(
    path="data/Electric_Vehicle_Population_Data.csv",
    table_name="ev_population",
    schema={
        "VIN (1-10)": "VARCHAR",
        "County": "VARCHAR",
        "City": "VARCHAR",
        "State": "VARCHAR",
        "Postal Code": "BIGINT",
        "Model Year": "BIGINT",
        "Make": "VARCHAR",
        "Model": "VARCHAR",
        "Electric Vehicle Type": "VARCHAR",
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "VARCHAR",
        "Electric Range": "BIGINT",
        "Base MSRP": "BIGINT",
        "Legislative District": "BIGINT",
        "DOL Vehicle ID": "BIGINT",
        "Vehicle Location": "VARCHAR",
        "Electric Utility": "VARCHAR",
        "2020 Census Tract": "BIGINT",
    },
    kwargs={"header": True},
)


def create_table_from_csv(
    table_cfg: TableConfiguration, connection: duckdb.DuckDBPyConnection = duckdb
):
    csv_content = connection.read_csv(
        table_cfg.path, dtype=table_cfg.schema, **table_cfg.kwargs
    )
    connection.sql(f"CREATE TABLE {table_cfg.table_name} AS SELECT * FROM csv_content")


def query_electric_cars_per_city() -> str:
    """
    Query to count the number of electric cars per city.
    """
    return """
    SELECT city, COUNT(*) AS num_electric_cars
    FROM ev_population
    GROUP BY city
    """


def query_top_3_most_popular_electric_vehicles() -> str:
    """
    Query to find the top 3 most popular electric vehicles.
    """
    return """\
    SELECT model, COUNT(*) AS count
    FROM ev_population
    GROUP BY model
    ORDER BY count DESC
    LIMIT 3
    """


def answer_question3() -> str:
    """
    Query to find the most popular electric vehicle in each postal code.

    Returns:
        str: The SQL query to find the most popular electric vehicle in each postal code.
    """
    return """
    -- Find the count of electric vehicles for each model in each postal code
    WITH max_counts AS (
        SELECT
            "Postal Code",  -- The postal code
            Model,  -- The electric vehicle model
            COUNT(*) AS count  -- The count of electric vehicles
        FROM ev_population
        GROUP BY "Postal Code", Model
    )
        
    -- Find the most popular electric vehicle in each postal code
    SELECT
        "Postal Code",  -- The postal code
        Model  -- The electric vehicle model
    FROM
    (
        SELECT
            "Postal Code",  -- The postal code
            Model,  -- The electric vehicle model
            RANK() OVER (PARTITION BY "Postal Code" ORDER BY count DESC) AS rank  -- The rank of the electric vehicle in the postal code
        FROM max_counts
    )
    WHERE rank = 1  -- Only select the electric vehicles with the highest count in each postal code
    """


def main() -> None:
    """Main entry point for the script."""
    create_table_from_csv(table_cfg)
    queries = [
        query_electric_cars_per_city(),
        query_top_3_most_popular_electric_vehicles(),
        answer_question3(),
    ]
    for query in queries:
        duckdb.sql(query).show()


if __name__ == "__main__":
    main()
