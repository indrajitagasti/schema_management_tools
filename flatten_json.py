
import ijson
import csv
import json
import logging
from pathlib import Path
from decimal import Decimal
import duckdb
from fastavro import writer, parse_schema

def setup_logging():
    """Set up logging to file and console."""
    log_file = Path(__file__).parent / 'data_processing.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)

# Custom object hook for JSON decoding to convert numbers to Decimal
def decimal_object_hook(obj):
    for key, value in obj.items():
        if isinstance(value, float):
            obj[key] = Decimal(str(value))
    return obj

def flatten_json_stream(json_file_path, csv_file_path):
    """
    Flattens a large JSON or NDJSON file to a CSV file using a streaming approach.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            first_char = json_file.read(1)
            json_file.seek(0)

            if first_char == '[':
                logging.info(f"Detected JSON array format. Processing '{json_file_path}' with ijson...")
                with open(json_file_path, 'rb') as json_file_binary:
                    items = ijson.items(json_file_binary, 'item')
                    _write_to_csv(items, csv_file_path)
            else:
                logging.info(f"Detected NDJSON/JSONL format. Processing '{json_file_path}' line by line...")
                items = (json.loads(line, object_hook=decimal_object_hook) for line in json_file)
                _write_to_csv(items, csv_file_path)

        logging.info(f"Successfully flattened {json_file_path} to {csv_file_path}")

    except FileNotFoundError:
        logging.error(f"The file {json_file_path} was not found.")
    except Exception as e:
        logging.error(f"An error occurred during flattening: {e}", exc_info=True)

def _write_to_csv(items, csv_file_path):
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = None
        header_written = False

        for i, item in enumerate(items):
            if not isinstance(item, dict):
                logging.warning(f"Skipping item #{i} as it's not a dictionary: {item}")
                continue

            if not header_written:
                headers = item.keys()
                writer = csv.DictWriter(csv_file, fieldnames=headers)
                writer.writeheader()
                header_written = True
            
            row = {k: str(v) if isinstance(v, Decimal) else v for k, v in item.items()}
            writer.writerow(row)

def infer_avro_schema_from_duckdb(con, table_name):
    """Infers a basic Avro schema from a DuckDB table description."""
    description = con.execute(f"DESCRIBE {table_name}").fetchall()
    type_mapping = {
        'VARCHAR': 'string',
        'BIGINT': 'long',
        'DOUBLE': 'double',
        'BOOLEAN': 'boolean',
        'DATE': {"type": "int", "logicalType": "date"},
        'DECIMAL': {"type": "bytes", "logicalType": "decimal"}
    }
    fields = []
    for col_name, col_type, _, _, _, _ in description:
        avro_type = type_mapping.get(col_type.split('(')[0], "string")
        if col_type.startswith('DECIMAL'):
            avro_type['precision'] = 18
            avro_type['scale'] = 2
        fields.append({"name": col_name, "type": ["null", avro_type]})
    
    return {
        "type": "record",
        "name": "InferredSchema",
        "fields": fields
    }

def convert_ndjson_to_formats(ndjson_path, avro_path, parquet_path):
    """Converts an NDJSON file to Avro and Parquet formats using DuckDB."""
    logging.info(f"Converting {ndjson_path} to Avro and Parquet using DuckDB...")
    logging.info(f"Using DuckDB version: {duckdb.__version__}")
    try:
        con = duckdb.connect(database=':memory:', read_only=False)
        
        table_name = "data_table"
        logging.info(f"Reading '{ndjson_path}' into DuckDB table '{table_name}'...")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{ndjson_path}')")

        logging.info(f"Writing to {parquet_path}...")
        con.execute(f"COPY {table_name} TO '{parquet_path}' (FORMAT 'PARQUET')")

        logging.info(f"Writing to {avro_path}...")
        avro_schema = infer_avro_schema_from_duckdb(con, table_name)
        for field in avro_schema["fields"]:
            if isinstance(field["type"][1], dict) and field["type"][1].get("logicalType") == "decimal":
                field["type"][1] = "string"
        parsed_schema = parse_schema(avro_schema)

        result = con.execute(f"SELECT * FROM {table_name}")
        with open(avro_path, 'wb') as avro_file:
            records = result.fetchdf().to_dict('records')
            avro_records = [
                {k: str(v) if isinstance(v, Decimal) else v for k, v in r.items()}
                for r in records
            ]
            writer(avro_file, parsed_schema, avro_records)

        logging.info(f"Successfully converted to {avro_path} and {parquet_path}")

    except FileNotFoundError:
        logging.error(f"The file {ndjson_path} was not found.")
    except Exception as e:
        logging.error(f"An error occurred during conversion: {e}", exc_info=True)
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    setup_logging()
    script_dir = Path(__file__).parent
    
    # 1. Process a standard JSON file to CSV
    json_input_path = script_dir / 'data.json'
    csv_output_path_json = script_dir / 'data_from_json.csv'
    logging.info(f"--- Processing {json_input_path} ---")
    flatten_json_stream(json_input_path, csv_output_path_json)

    logging.info("\n" + "="*30 + "\n")

    # 2. Process a newline-delimited JSON file to CSV
    ndjson_input_path = script_dir / 'data.ndjson'
    csv_output_path_ndjson = script_dir / 'data_from_ndjson.csv'
    if not ndjson_input_path.exists():
        logging.info("Creating dummy data.ndjson for demonstration...")
        with open(ndjson_input_path, 'w', encoding='utf-8') as f:
            f.write('{"name": "Alice", "city": "Amsterdam", "isStudent": true, "admissionDate": "2023-01-15", "tuition": 16000.50}\n')
            f.write('{"name": "Bob", "city": "Berlin", "isStudent": false, "admissionDate": "2022-09-01", "tuition": 14000.00}\n')

    logging.info(f"--- Processing {ndjson_input_path} ---")
    flatten_json_stream(ndjson_input_path, csv_output_path_ndjson)

    logging.info("\n" + "="*30 + "\n")

    # 3. Convert NDJSON to Avro and Parquet
    avro_output_path = script_dir / 'data.avro'
    parquet_output_path = script_dir / 'data.parquet'
    logging.info(f"--- Converting {ndjson_input_path} to Avro and Parquet ---")
    convert_ndjson_to_formats(ndjson_input_path, avro_output_path, parquet_output_path)
