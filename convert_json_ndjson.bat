python .\convert_json_ndjson.py ndjson-to-json data.jsonl data1.json --batch-size 2 
python .\convert_json_ndjson.py json-to-ndjson data1.json data2.jsonl --batch-size 2 
python .\convert_json_ndjson.py json-to-ndjson 100.variants.json 100.variants.jsonl --batch-size 42 
python .\convert_json_ndjson.py ndjson-to-json 100.variants.jsonl 100.variants1.json --batch-size 42 