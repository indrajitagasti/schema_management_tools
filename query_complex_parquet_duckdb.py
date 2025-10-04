import duckdb
import os


def query_with_duckdb(parquet_path: str):
    """
    Uses DuckDB to query a complex Parquet file and demonstrates how to
    work with nested data structures.

    Args:
        parquet_path (str): Path to the complex Parquet file.
    """
    if not os.path.exists(parquet_path):
        print(f"File not found: {parquet_path}")
        print("Please run 'generate_complex_parquet.py' first to create the file.")
        return

    # DuckDB can query Parquet files directly without any setup.
    # We can use a variable in the query to reference the file path.
    print(f"--- Querying {parquet_path} with DuckDB ---")

    # 1. Basic query to see the structure
    print("\n1. Basic SELECT * to view data structure:")
    duckdb.sql(f"SELECT * FROM '{parquet_path}'").show()

    # 2. Querying 'simple_array' (Array)
    print("\n2. Querying 'simple_array' (an array of strings):")
    print("   a) Flattening the array with UNNEST:")
    # UNNEST creates a new row for each element in the array.
    # This uses the lateral join syntax, which is clear and powerful.
    duckdb.sql(f"""
        SELECT
            pq.id,
            tag
        FROM '{parquet_path}' AS pq, UNNEST(pq.simple_array) AS t(tag)
    """).show()

    # 3. Querying 'array_of_arrays' (Array of Arrays)
    print("\n3. Querying 'array_of_arrays' (a list of lists of integers):")
    print("   a) Flattening the nested lists using chained UNNEST (lateral joins):")
    # The first UNNEST creates a row for each inner list.
    # The second UNNEST operates on the result of the first, flattening the inner lists.
    duckdb.sql(f"""
        SELECT
            pq.id,
            number
        FROM '{parquet_path}' AS pq,
            UNNEST(pq.array_of_arrays) AS inner_arrays(list),
            UNNEST(inner_arrays.list) AS numbers(number)
        WHERE number IS NOT NULL
    """).show()

    # 4. Querying 'array_of_structs' (Array of Structs)
    print("\n4. Querying 'array_of_structs' (a list of coordinates):")
    print("   a) Unnesting the list and accessing struct fields:")
    # UNNEST creates a row for each struct in the list.
    # We can then access the struct's fields using dot notation.
    duckdb.sql(f"""
        SELECT
            pq.id,
            point.x,
            point.y
        FROM '{parquet_path}' AS pq,
            UNNEST(pq.array_of_structs) AS t(point)
    """).show()

    # 5. Querying 'struct_of_arrays' (Struct of Arrays)
    print("\n5. Querying 'struct_of_arrays' (a struct containing lists):")
    print("   a) Accessing the arrays inside the struct:")
    # Access struct fields directly with dot notation. The result is the array itself.
    duckdb.sql(f"""
        SELECT
            id,
            struct_of_arrays.timestamps,
            struct_of_arrays.values
        FROM '{parquet_path}'
    """).show()

    print("   b) Unnesting the parallel arrays to correlate values (time series):")
    # DuckDB's UNNEST can take multiple arguments, creating a row for each
    # corresponding element from the lists. This is not supported in all versions.
    # A more compatible way is to generate a sequence of indices and use them to access array elements.
    duckdb.sql(f"""
        SELECT
            pq.id,
            pq.struct_of_arrays.timestamps[idx] AS timestamp,
            pq.struct_of_arrays.values[idx] AS value
        FROM '{parquet_path}' AS pq,
            UNNEST(generate_series(1, array_length(pq.struct_of_arrays.timestamps))) AS s(idx)
    """).show()


if __name__ == "__main__":
    # You will need to install duckdb:
    # pip install duckdb
    COMPLEX_PARQUET_FILE = r"c:\dev\parse_avro_files\files\complex_data.parquet"

    query_with_duckdb(COMPLEX_PARQUET_FILE)