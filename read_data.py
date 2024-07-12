import pandas as pd
import dask.dataframe as dd
import modin.pandas as mpd
import ray
import yaml
import gzip
import os
import time

def read_with_modin(file_path):
    return mpd.read_csv(file_path)

def clean_column_names(df):
    df.columns = df.columns.str.replace(r'[^\w\s]', '', regex=True).str.replace(' ', '')
    return df

def load_schema(yaml_path):
    with open(yaml_path, 'r') as file:
        schema = yaml.safe_load(file)
    return schema

def validate_columns(df, schema):
    expected_columns = [col['name'] for col in schema['file']['columns']]
    if list(df.columns) != expected_columns:
        raise ValueError("Column names or order do not match schema")

def write_to_gzip(df, output_path, separator):
    with gzip.open(output_path, 'wt', encoding='utf-8') as gzfile:
        df.to_csv(gzfile, sep=separator, index=False)

def generate_summary(file_path):
    df = pd.read_csv(file_path)
    total_rows = df.shape[0]
    total_columns = df.shape[1]
    file_size = os.path.getsize(file_path)
    return total_rows, total_columns, file_size

if __name__ == "__main__":
    file_path = "training_data.csv"  # Replace with your input file path
    yaml_path = "schema.yaml"  # Replace with your schema file path
    output_path = "output.txt.gz"  # Output gzipped file path

    # Load the schema
    schema = load_schema(yaml_path)
    separator = schema['file']['separator']

    # Read the file using different methods
    print("reading with pandas")
    start = time.time()
    df_pandas = pd.read_csv(file_path)
    end = time.time()
    print("took",end-start,"seconds")

    print()

    print("reading with dask")
    start = time.time()
    df_dask = dd.read_csv(file_path).compute()
    end = time.time()
    print("took", end-start, "seconds")

    print()

    print("reading with modin")
    start = time.time()
    df_modin = mpd.read_csv(file_path)
    end=time.time()
    print("took",end-start,"seconds")

    print()

    # Choose one dataframe for further processing
    print("choosing pandas dataframe for processing")
    df = df_pandas  # or df_dask or df_modin

    # Clean the column names
    print("cleaning column names")
    df = clean_column_names(df)

    # Validate columns against the schema
    print("validating columns")
    validate_columns(df, schema)

    # Write the cleaned and validated data to a gzipped file
    print("writing to output file")
    write_to_gzip(df, output_path, separator)

    # Generate and print summary
    total_rows, total_columns, file_size = generate_summary(file_path)
    print(f"Total number of rows: {total_rows}")
    print(f"Total number of columns: {total_columns}")
    print(f"File size: {file_size} bytes")