import pyarrow as pa
import pyarrow.parquet as pq
import os
import pathlib
from datetime import datetime


def generate_complex_parquet(output_path: str):
    """
    Generates a Parquet file with complex nested data types including arrays,
    arrays of arrays, arrays of structs, and structs of arrays.

    Args:
        output_path (str): The path to save the output Parquet file.
    """
    # 1. Define the schema with complex types using pyarrow
    # This explicitly defines the structure of our data.
    schema = pa.schema([
        pa.field('id', pa.int64(), nullable=False),

        # An array of simple types (e.g., list of strings)
        pa.field('simple_array', pa.list_(pa.string())),

        # An array of arrays (e.g., a matrix of integers)
        pa.field('array_of_arrays', pa.list_(pa.list_(pa.int32()))),

        # An array of structures (e.g., a list of coordinate points)
        pa.field('array_of_structs', pa.list_(
            pa.struct([
                pa.field('x', pa.int64()),
                pa.field('y', pa.int64())
            ])
        )),

        # A structure containing arrays (e.g., a time series object)
        pa.field('struct_of_arrays', pa.struct([
            pa.field('timestamps', pa.list_(pa.timestamp('ms'))),
            pa.field('values', pa.list_(pa.float64()))
        ]))
    ])

    # 2. Create sample data that conforms to the schema
    data = [
        {
            'id': 1,
            'simple_array': ['tag1', 'tag2'],
            'array_of_arrays': [[1, 2], [3, 4, 5]],
            'array_of_structs': [{'x': 10, 'y': 20}, {'x': 15, 'y': 25}],
            'struct_of_arrays': {
                'timestamps': [datetime(2023, 1, 1, 12, 0), datetime(2023, 1, 1, 13, 0)],
                'values': [100.5, 102.3]
            }
        },
        {
            'id': 2,
            'simple_array': None,  # Example of a null list
            'array_of_arrays': [[10], [20, 30], []],
            'array_of_structs': [],  # Example of an empty list of structs
            'struct_of_arrays': {
                'timestamps': [], # Example of empty lists within a struct
                'values': []
            }
        }
    ]

    # 3. Convert the Python dictionary data to a PyArrow Table
    table = pa.Table.from_pylist(data, schema=schema)

    # 4. Write the PyArrow Table to a Parquet file
    print(f"Writing complex Parquet file to: {output_path}")
    pq.write_table(table, output_path)
    print("Write complete.")


def read_and_print_complex_parquet(file_path: str):
    """Reads the complex Parquet file and prints its contents."""
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"\nReading complex Parquet file from: {file_path}")
    table = pq.read_table(file_path)

    print("\n--- Schema ---")
    print(table.schema)

    print("\n--- Data (converted to Pandas DataFrame for display) ---")
    # Converting to pandas is just for easy and readable printing
    df = table.to_pandas()
    print(df.to_string())


def demonstrate_schema_evolution(output_dir: pathlib.Path):
    """
    Generates two Parquet files with different schemas to demonstrate
    schema evolution and merging.
    """
    print("\n--- Demonstrating Schema Evolution ---")

    # 1. Define the V1 schema and data
    schema_v1 = pa.schema([
        pa.field('id', pa.int64(), nullable=False),
        pa.field('simple_array', pa.list_(pa.string()))
    ])
    data_v1 = [{'id': 1, 'simple_array': ['tag1', 'tag2']}]
    table_v1 = pa.Table.from_pylist(data_v1, schema=schema_v1)

    # 2. Write the V1 file
    path_v1 = output_dir / "data_v1.parquet"
    print(f"Writing V1 data to: {path_v1}")
    pq.write_table(table_v1, path_v1)

    # 3. Define the V2 schema (add a new column) and data
    schema_v2 = pa.schema([
        pa.field('id', pa.int64(), nullable=False),
        pa.field('simple_array', pa.list_(pa.string())),
        # ADDED a new column
        pa.field('metadata', pa.string())
    ])
    data_v2 = [{'id': 2, 'simple_array': ['tag3'], 'metadata': 'new_field_value'}]
    table_v2 = pa.Table.from_pylist(data_v2, schema=schema_v2)

    # 4. Write the V2 file
    path_v2 = output_dir / "data_v2.parquet"
    print(f"Writing V2 data to: {path_v2}")
    pq.write_table(table_v2, path_v2)

    # 5. Read the entire directory using ParquetDataset for schema merging
    print(f"\nReading both files from directory: {output_dir}")
    # ParquetDataset inspects all files and merges their schemas
    dataset = pq.ParquetDataset(output_dir)
    merged_table = dataset.read()

    print("\n--- Merged Schema ---")
    print("Notice the 'metadata' field has been added to the unified schema.")
    print(merged_table.schema)

    print("\n--- Merged Data (as Pandas DataFrame) ---")
    print("Notice the V1 row has 'None' (null) for the 'metadata' column.")
    df = merged_table.to_pandas()
    print(df.to_string())


if __name__ == "__main__":
    # You will need to install pyarrow and pandas:
    # pip install pyarrow pandas

    # --- Original Example ---
    print("--- Generating Original Complex File ---")
    complex_file_path = r"c:\dev\parse_avro_files\files\complex_data.parquet"
    complex_file_dir = os.path.dirname(complex_file_path)
    os.makedirs(complex_file_dir, exist_ok=True)
    generate_complex_parquet(complex_file_path)
    read_and_print_complex_parquet(complex_file_path)
    print("-" * 40)

    # --- Schema Evolution Example ---
    # We will create a new, separate directory for this demonstration
    # to avoid mixing it with the single complex file.
    evolution_dir = pathlib.Path(r"c:\dev\parse_avro_files\files\evolution_demo")
    if not evolution_dir.exists():
        os.makedirs(evolution_dir, exist_ok=True)

    demonstrate_schema_evolution(evolution_dir)