import duckdb
import os


def remove_empty_columns_duckdb(input_path: str, output_path: str):
    """
    Uses DuckDB to efficiently remove completely empty columns from a large CSV file.

    This method is memory-efficient as it streams the data rather than loading
    the entire file into memory, making it suitable for very large files.

    Args:
        input_path (str): The path to the input CSV file.
        output_path (str): The path where the cleaned CSV file will be saved.
    """
    try:
        # Ensure the output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        print(f"Processing '{os.path.basename(input_path)}' with DuckDB...")

        # Connect to an in-memory DuckDB database
        con = duckdb.connect(database=":memory:", read_only=False)

        # 1. Use SUMMARIZE to get column statistics in a single pass.
        # The 'null_percentage' column in the summary represents the percentage of non-null values.
        # read_csv_auto is used to automatically detect CSV parameters.
        summary_df = con.execute(
            f"SUMMARIZE SELECT * FROM read_csv_auto('{input_path}')"
        ).fetchdf()

        # 2. Identify columns where the count of non-null values is 0
        all_columns = summary_df["column_name"].tolist()
        empty_cols = summary_df[summary_df["null_percentage"] == 100]["column_name"].tolist()

        if not empty_cols:
            print("\nNo completely empty columns found. File is already clean.")
            return

        print(f"\nFound completely empty columns to remove: {empty_cols}")

        # 3. Construct a SELECT statement with only the columns to keep
        cols_to_keep = [col for col in all_columns if col not in empty_cols]
        select_clause = ", ".join([f'"{col}"' for col in cols_to_keep])

        # 4. Use the COPY command to stream the result to a new CSV file
        copy_query = f"""
        COPY (SELECT {select_clause} FROM read_csv_auto('{input_path}'))
        TO '{output_path}' (HEADER, DELIMITER ',');
        """
        con.execute(copy_query)

        print(f"\nSuccessfully removed empty columns and saved to '{output_path}'")
        print("Cleaned columns:", cols_to_keep)

    except duckdb.Error as e:
        print(f"A DuckDB error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    INPUT_CSV = r"c:\dev\parse_avro_files\files\sample.csv"
    OUTPUT_CSV = r"c:\dev\parse_avro_files\files\sample_cleaned_duckdb.csv"
    remove_empty_columns_duckdb(INPUT_CSV, OUTPUT_CSV)