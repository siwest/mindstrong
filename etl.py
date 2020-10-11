from code import tasks
from validations.validator import Validator
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description="ETL Job")
parser.add_argument("--spec", type=str, help="Input specification file name")
parser.add_argument("--datafile", type=str, help="Input data file name")
parser.add_argument("--target", type=str, help="Database connection string")
args = parser.parse_args()

spec = args.spec
datafile = args.datafile
target = args.target
table_name = "claims"


db_connection = tasks.connect_sqllite_database(target)
db_cursor = db_connection.cursor()

claims_data = tasks.get_claims(datafile)

schema = tasks.get_table_specification(spec)
schema_validator = Validator(schema)

tasks._drop_table(db_cursor, db_connection, table_name)
tasks.create_table(db_cursor, db_connection, table_name, schema)


clean_claims = []

header_list = []
for row_index, row in enumerate(claims_data):
    # Clean and check data headers
    if row_index == 0:
        for header in row:
            valid_header = schema_validator.check_header(header)

            if not valid_header:
                header = header.strip().replace(" ", "_")
                valid_header = schema_validator.check_header(header)

            # Tracking valid headers
            if valid_header:
                header_list.append(header)

    else:
        # Clean and check rest of data rows
        data_row = {}
        for column_index, data_value in enumerate(row):

            data_header = header_list[column_index]

            is_valid_record_length = schema_validator.check_record_length(
                data_header, data_value
            )
            if not is_valid_record_length:
                data_value, other_unpacked_value = schema_validator.split_record(
                    data_header, data_value
                )

            is_valid_value = schema_validator.check_type(data_header, data_value)
            if not is_valid_value:
                data_value = schema_validator.apply_type(data_header, data_value)

            data_row[data_header] = data_value

        # Special handling only if we find data concatenated
        if (
            column_index < len(header_list)
            and not is_valid_record_length
        ):

            other_data_header = header_list[column_index + 1]

            is_valid_value = schema_validator.check_type(
                other_data_header, other_unpacked_value
            )
            if not is_valid_value:
                data_value = schema_validator.apply_type(
                    other_data_header, other_unpacked_value
                )

            data_header = header_list[column_index + 1]

            data_row[data_header] = data_value

        clean_claims.append(data_row)

tasks.append_to_table(db_connection, table_name, clean_claims)
tasks.disconnect_sqllite_database(db_connection)
