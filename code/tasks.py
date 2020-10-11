import sqlite3
import datetime
import logging
import csv
import pandas as pd


def get_claims(file_name):
    """Read claims data from a CSV file
    Parameters:
        file_name (str): A file name

    Returns:
        get_claims (list): Listed rows of claims data
    """
    with open(file_name, "r") as claims_csv:
        claims_reader = csv.reader(claims_csv)
        return list(claims_reader)


def get_table_specification(file_name):
    """Read table specification data from a CSV file
    Parameters:
        file_name (str): A file name

    Returns:
        get_table_specification (list): List in dictionary rules or 
          specifications for how to read the CSV files
    """    
    clean_schema = []
    with open(file_name, "r") as spec_file:
        schema_specification = list(csv.DictReader(spec_file))
        for row in schema_specification:
            data_definition = {}
            for key, value in row.items():
                new_key = key.strip().replace(" ", "_")
                data_definition[new_key] = value.strip()
            clean_schema.append(data_definition)
    return clean_schema


def connect_sqllite_database(db_name):
    """Creates a connection object to a local database file, or creates a
    new one with the database name as `db_name`.
    """
    logging.warning("Connecting to sqlite3 database")
    return sqlite3.connect(db_name)


def disconnect_sqllite_database(db_connection):
    """Closes a connection object.
    """
    logging.warning("Closing connection to sqlite3 database")
    return db_connection.close()


def _drop_table(db_cursor, db_connection, table_name):
    """Drop table function should likely not happen in production. This is
    reserved for development only.
    """
    logging.warning("Dropping existing Transactions table if exists")
    db_cursor.execute(f"""DROP TABLE {table_name}""")
    db_connection.commit()


def create_table(db_cursor, db_connection, table_name, clean_schema):
    """Creates a table from a provided schema.
    """
    schema_string = ""
    for item in clean_schema:
        field_name = item["field_name"]
        data_type = item["datatype"]

        schema_string += f"""
      , {field_name} {data_type}
      """

    create_table_string = f"""CREATE TABLE {table_name}
    ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT {schema_string}
      , created_at timestamp
      , source_file_name text
    )"""

    logging.warning("Creating table if not exists")

    db_cursor.execute(create_table_string)

    db_connection.commit()


def append_to_table(db_connection, table_name, data):
    """Appenda to data a table.
    """
    df = pd.DataFrame(data)
    df.to_sql(name=table_name, con=db_connection, if_exists="append", index=None)
