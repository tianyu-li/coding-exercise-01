import json
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# === CONFIGURATION ===
PROJECT_ID = 'tli-sample-01'
DATASET = 'fetch'

# File paths
FILES = {
    'receipts': 'data/receipts.jsonl',  
    'users': 'data/users.jsonl',  
    'brands': 'data/brands.jsonl' 
}

# Output paths
OUTPUT_JSON = {
    'receipts': 'output/json/receipts.json',
    'users': 'output/json/users.json',
    'brands': 'output/json/brands.json'
}

OUTPUT_JSONL = {
    'receipts': 'output/jsonl/receipts.jsonl',
    'users': 'output/jsonl/users.jsonl',
    'brands': 'output/jsonl/brands.jsonl'
}

# === HELPER FUNCTION: CLEAN NESTED JSON ===
def clean_json(record):
    cleaned_record = {}
    for key, value in record.items():
        if isinstance(value, dict) and len(value) == 1 and list(value.keys())[0].startswith('$'):
            new_key = key.replace('$', '')
            inner_key = list(value.keys())[0]

            if inner_key == '$oid':
                cleaned_record[new_key] = value[inner_key]
            elif inner_key == '$date' or 'date' in key.lower():
                try:
                    # Convert to datetime if it's a number, else leave as is
                    if isinstance(value[inner_key], (int, float)):
                        cleaned_record[new_key] = datetime.utcfromtimestamp(value[inner_key] / 1000).isoformat()
                    else:
                        cleaned_record[new_key] = value[inner_key]
                except Exception as e:
                    print(f"⚠️ Error converting date for key '{new_key}': {e}")
                    cleaned_record[new_key] = value[inner_key]
            elif inner_key == '$ref':
                cleaned_record[new_key] = value[inner_key]
        elif isinstance(value, dict) and '$id' in value and '$oid' in value['$id']:
            cleaned_record[f"{key}_id"] = value['$id']['$oid']
        else:
            if 'date' in key.lower() and isinstance(value, (int, float)):
                try:
                    cleaned_record[key.replace('$', '')] = datetime.utcfromtimestamp(value / 1000).isoformat()
                except Exception as e:
                    print(f"⚠️ Error converting date for key '{key}': {e}")
                    cleaned_record[key.replace('$', '')] = value
            else:
                cleaned_record[key.replace('$', '')] = value
    return cleaned_record

# === STEP 1: READ & FLATTEN JSONL ===
def read_and_flatten_jsonl(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            record = json.loads(line)
            cleaned_record = clean_json(record)
            flat_record = pd.json_normalize(cleaned_record)
            data.append(flat_record)

    return pd.concat(data, ignore_index=True)

# === STEP 2: CONVERT TO JSON === 
# this is not needed, I used it for debugging. It should be removed in future tickets.
def convert_to_json(df, output_path):
    df.to_json(output_path, orient='records', indent=4, force_ascii=False)
    print(f"✅ Converted to JSON: {output_path}")

# === STEP 3: CONVERT TO JSONL ===
def convert_to_jsonl(df, output_path):
    df.to_json(output_path, orient='records', lines=True, force_ascii=False)
    print(f"✅ Converted to JSONL: {output_path}")

# === STEP 4: LOAD JSONL INTO BIGQUERY ===
def load_to_bigquery(jsonl_file, table_name):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    with open(jsonl_file, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    load_job.result()
    print(f"✅ Loaded data into BigQuery: {table_id}")

# === MAIN PIPELINE ===
def main():
    for table_name, jsonl_file in FILES.items():
        df = read_and_flatten_jsonl(jsonl_file)
        #convert_to_json(df, OUTPUT_JSON[table_name])
        convert_to_jsonl(df, OUTPUT_JSONL[table_name])
        load_to_bigquery(OUTPUT_JSONL[table_name], table_name)

if __name__ == "__main__":
    main()
