import psycopg2
import os

from collections import namedtuple

Table = namedtuple("Table", ["create_query", "index_column"])

accounts = Table(
    """
CREATE TABLE IF NOT EXISTS accounts (
    customer_id integer PRIMARY KEY,
    first_name text,
    last_name text,
    address_1 text,
    address_2 text,
    city text,
    state text,
    zip_code integer,
    join_date date
)
""",
    "customer_id",
)

products = Table(
    """
CREATE TABLE IF NOT EXISTS products (
    product_id integer PRIMARY KEY,
    product_code text,
    product_description text
)
""",
    "product_id",
)

transactions = Table(
    """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id text PRIMARY KEY,
    transaction_date date,
    product_id integer,
    product_code text,
    product_description text,
    quantity integer,
    account_id integer,
FOREIGN KEY(product_id)
    REFERENCES products(product_id),
FOREIGN KEY(account_id)
    REFERENCES accounts(customer_id)
)
""",
    "transaction_id",
)

table_dict = {"accounts": accounts, "products": products, "transactions": transactions}


def drop_table_if_exists(table_name: str) -> str:
    """Generate SQL command to drop a table if it exists, with cascade option."""
    return f"DROP TABLE IF EXISTS {table_name} CASCADE;"


def copy_data_from_csv(
    csv_file_name: str, table_name: str, cursor: psycopg2.extensions.cursor
) -> None:
    """
    Copy data from a CSV file into a PostgreSQL table.

    Args:
        csv_file_name (str): The name of the CSV file to copy data from.
        table_name (str): The name of the PostgreSQL table to copy data into.
        cursor (psycopg2.extensions.cursor): The cursor to use for executing SQL statements.
    """
    csv_file_path = os.path.join("data", csv_file_name)
    with open(csv_file_path, "r") as csv_file:
        next(csv_file)  # Skip the header row
        cursor.copy_from(csv_file, table_name, sep=",")


def create_index(index_name: str, table_name: str, column_names: list[int] | int):
    """Create an index on a table based on a list of column names."""
    column_name_list = [str(column_name) for column_name in column_names]
    index_query = (
        f"CREATE INDEX {index_name} ON {table_name} ({', '.join(column_name_list)})"
    )
    return index_query


def main():
    host = "localhost"
    database = "postgres"
    user = "postgres"
    password = "104729"

    with psycopg2.connect(
        host=host, database=database, user=user, password=password
    ) as connection:
        with connection.cursor() as cursor:
            for csv_file in os.listdir("data"):
                table_name = csv_file.removesuffix(".csv")
                table = table_dict[table_name]

                cursor.execute(drop_table_if_exists(table_name))
                cursor.execute(table.create_query)
                copy_data_from_csv(csv_file, table_name, cursor)
                create_index(f"{table_name}_idx", table_name, table.index_column)


def copy_data(
    csv_file_name: str, table_name: str, cursor: psycopg2.extensions.cursor
) -> None:
    """
    Copy data from a CSV file into a PostgreSQL table.

    Args:
        csv_file_name (str): The name of the CSV file to copy data from.
        table_name (str): The name of the PostgreSQL table to copy data into.
        cursor (psycopg2.extensions.cursor): The cursor to use for executing SQL statements.
    """
    csv_file_path = os.path.join("data", csv_file_name)
    with open(csv_file_path, "r") as csv_file:
        next(csv_file)  # Skip the header row
        cursor.copy_from(csv_file, table_name, sep=",")


if __name__ == "__main__":
    main()
