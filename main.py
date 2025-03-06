from datetime import datetime
import json
import pandas as pd
from bson import ObjectId
import os
import sys
import pymongo
import time
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

mongo_connection_string = os.getenv("botit_mongo_connection_string")
if mongo_connection_string:
    mongo_client = pymongo.MongoClient(mongo_connection_string)
    mongo_db = mongo_client["botitprod"]
else:
    raise ValueError("MongoDB connection string is missing. Check environment variables.")

def aggregate_mongo(collection_name, pipeline):
    print(f"Fetching data from collection: {collection_name}...")
    data = list(mongo_db[collection_name].aggregate(pipeline))
    print(f"Fetched {len(data)} records.")
    return data

def load_pipeline(json_file, key_name):
    with open(json_file, "r", encoding="utf-8") as file:
        aggregation_data = json.load(file)
    pipeline_info = aggregation_data.get(key_name, {})
    return pipeline_info.get("collection", ""), pipeline_info.get("aggregation_pipeline", [])

def convert_objectid_to_str(df):
    print("Converting ObjectId fields to string...")
    for col in tqdm(df.columns, desc="Processing columns"):
        if df[col].apply(lambda x: isinstance(x, ObjectId)).any():
            df[col] = df[col].astype(str)
    return df

def add_date_filter(pipeline, start_date=None, end_date=None):
    if start_date and end_date:
        print(f"Filtering data from {start_date} to {end_date}...")
        return [{"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}}}] + pipeline
    return pipeline

def fetch_and_process_data(json_file, key_name, df_name, start_date=None, end_date=None):
    collection_name, pipeline = load_pipeline(json_file, key_name)
    if collection_name and pipeline:
        pipeline = add_date_filter(pipeline, start_date, end_date)

        print(f"Starting aggregation for: {key_name}")
        data = aggregate_mongo(collection_name, pipeline)

        df = pd.DataFrame(data)
        if not df.empty:
            df = convert_objectid_to_str(df)

        # Ensure 'CSVs' folder exists
        os.makedirs("CSVs", exist_ok=True)

        # Save CSV
        csv_path = os.path.join("CSVs", f"{df_name}.csv")
        df.to_csv(csv_path, index=False)
        print(f"Saved CSV at: {csv_path}")

        globals()[df_name] = df
    else:
        print(f"Skipping {key_name}: No collection or pipeline found.")

def main(start_date=None, end_date=None):
    start_time = time.time()

    print("ETL Process Started...")
    fetch_and_process_data("aggregation_pipelines.json", "bestselleritems", "bestseller_df", start_date, end_date)

    end_time = time.time()
    print(f"ETL Process Completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    start_date = None
    end_date = None

    if len(sys.argv) == 3:
        start_date = datetime.strptime(sys.argv[1], "%d/%m/%Y")
        end_date = datetime.strptime(sys.argv[2], "%d/%m/%Y")

    main(start_date, end_date)
