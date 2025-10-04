#!/usr/bin/env python
# coding: utf-8

import json
import argparse
import sys
import logging
from decimal import Decimal
import ijson

logger = logging.getLogger(__name__)

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal objects."""
    def default(self, o):
        if isinstance(o, Decimal):
            # Convert Decimal to string to preserve precision
            return str(o)
        return super(DecimalEncoder, self).default(o)

def setup_logging(log_level, log_file):
    """Configures logging to console and optionally to a file."""
    logger.setLevel(logging.DEBUG)

    # Avoid adding handlers if they already exist
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write('\n' + '=' * 100 + '\n')        
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

def convert_ndjson_to_json(input_file_path, output_file_path, batch_size):
    """
    Reads a large NDJSON file, converting it to a single JSON array using streaming and batching.
    """
    logger.info(f"Starting NDJSON to JSON conversion...")
    logger.debug(f"Input: {input_file_path}, Output: {output_file_path}, Batch Size: {batch_size}")
    object_count = 0

    try:
        with open(input_file_path, 'r', encoding='utf-8') as infile, \
            open(output_file_path, 'w', encoding='utf-8') as outfile:

            outfile.write('[\n')
            is_first_object = True
            batch = []

            for line_num, line in enumerate(infile, 1):
                if not line.strip():
                    logger.debug(f"Skipping empty line at position {line_num}")
                    continue

                try:
                    # Use parse_float=Decimal to preserve precision if needed, but we will handle it on dump
                    batch.append(json.loads(line))

                    if len(batch) >= batch_size:
                        logger.debug(f"Writing batch of {len(batch)} objects to disk.")
                        write_batch(outfile, batch, is_first_object)
                        is_first_object = False
                        object_count += len(batch)
                        batch = []

                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON on line {line_num}: {e}")
                    logger.error(f"Problematic line: {line.strip()}")

            if batch:
                logger.debug(f"Writing final batch of {len(batch)} objects.")
                write_batch(outfile, batch, is_first_object)
                object_count += len(batch)

            outfile.write('\n]')

    except FileNotFoundError:
        logger.error(f"Input file not found at '{input_file_path}'", exc_info=True)
        return
    except Exception as e:
        logger.critical(f"An unexpected error occurred during NDJSON to JSON conversion: {e}", exc_info=True)
        return

    logger.info(f"Conversion complete! Successfully processed {object_count} objects.")

def write_batch(outfile, batch, is_first_object):
    """Helper to write a batch of objects to the JSON array."""
    if not batch:
        return
    output_string = ',\n'.join(['    ' + json.dumps(obj, cls=DecimalEncoder) for obj in batch])
    if not is_first_object:
        outfile.write(',\n')
    outfile.write(output_string)

def convert_json_to_ndjson(input_file_path, output_file_path, batch_size):
    """
    Streams a large JSON file and converts it to NDJSON format using ijson and batching.
    """
    logger.info(f"Starting JSON to NDJSON conversion...")
    logger.debug(f"Input: {input_file_path}, Output: {output_file_path}, Batch Size: {batch_size}")
    object_count = 0

    try:
        with open(input_file_path, 'rb') as infile, \
            open(output_file_path, 'w', encoding='utf-8') as outfile:

            batch = []
            # Use `use_float=True` to avoid ijson creating Decimal objects
            for obj in ijson.items(infile, 'item', use_float=True):
                batch.append(obj)
                if len(batch) >= batch_size:
                    logger.debug(f"Writing batch of {len(batch)} objects to disk.")
                    for item in batch:
                        json.dump(item, outfile, cls=DecimalEncoder)
                        outfile.write('\n')
                    object_count += len(batch)
                    batch = []

            if batch:
                logger.debug(f"Writing final batch of {len(batch)} objects.")
                for item in batch:
                    json.dump(item, outfile, cls=DecimalEncoder)
                    outfile.write('\n')
                object_count += len(batch)

    except ijson.JSONError as e:
        logger.error(f"Error parsing JSON from '{input_file_path}': {e}", exc_info=True)
        return
    except FileNotFoundError:
        logger.error(f"Input file not found at '{input_file_path}'", exc_info=True)
        return
    except Exception as e:
        logger.critical(f"An unexpected error occurred during JSON to NDJSON conversion: {e}", exc_info=True)
        return

    logger.info(f"Conversion complete! Successfully processed {object_count} objects.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Convert between JSON and NDJSON formats using a streaming, batch-oriented approach.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "conversion_type",
        choices=['json-to-ndjson', 'ndjson-to-json'],
        help="The type of conversion to perform."
    )
    parser.add_argument(
        "input_file",
        help="The path to the input file."
    )
    parser.add_argument(
        "output_file",
        help="The path for the output file."
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help="Number of objects to process in each batch (default: 1000)."
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level for console output (default: INFO)."
    )
    parser.add_argument(
        '--log-file',
        default="convert_json_ndjson.log",
        help="Path to a log file. If not provided, logs are written in convert_json_ndjson.log and sent to the console."
    )

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    setup_logging(log_level, args.log_file)

    if args.conversion_type == 'json-to-ndjson':
        convert_json_to_ndjson(args.input_file, args.output_file, args.batch_size)
    elif args.conversion_type == 'ndjson-to-json':
        convert_ndjson_to_json(args.input_file, args.output_file, args.batch_size)