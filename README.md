# MindStrong Health

## ETL to Ingest a Flat File

This repo provides the ETL code to ingest a flat file with a specified layout and sample data into a SQLite database.

The ETL program can be run as follows:

`python etl.py --spec specs/claimsspec1.txt --datafile data/claimsdata_2020_01_01.txt --target mindstrong`

The program provides basic validation of data to a specification file to ensure the data is loaded correctly. Validations include the following:

 - `check_header` - Checks that headers in data file match headers in specification file.
 - `check_type` - Checks that a given data value corresponds to the correct type for a given header in the schema
 - `check_record_length` - Checks that given data value is longer than the specified width in the schema.

While processing each file, the program provides logging of warnings to help developers debug errors that may occur during processing.

Unit test coverage is provided in the `/tests` directory. Tests can be run on the command line as follows.

`python -m unittest tests/test.py`

