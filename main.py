from datetime import datetime
import json
import pandas as pd
from bson import ObjectId
import os
import sys
import pymongo
from dotenv import load_dotenv

load_dotenv()

mongo_connection_string = os.getenv("botit_mongo_connection_string")
if mongo_connection_string:
    mongo_client = pymongo.MongoClient(mongo_connection_string)
    mongo_db = mongo_client["botitprod"]
else:
    raise ValueError("MongoDB connection string is missing. Check environment variables.")

def aggregate_mongo(collection_name, pipeline):
    return list(mongo_db[collection_name].aggregate(pipeline))

def load_pipeline(json_file, key_name):
    with open(json_file, "r", encoding="utf-8") as file:
        aggregation_data = json.load(file)
    pipeline_info = aggregation_data.get(key_name, {})
    return pipeline_info.get("collection", ""), pipeline_info.get("aggregation_pipeline", [])

def convert_objectid_to_str(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, ObjectId)).any():
            df[col] = df[col].astype(str)
    return df

def add_date_filter(pipeline, start_date=None, end_date=None):
    if start_date and end_date:
        return [{"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}}}] + pipeline
    return pipeline

def fetch_and_process_data(json_file, key_name, df_name, start_date=None, end_date=None):
    collection_name, pipeline = load_pipeline(json_file, key_name)
    if collection_name and pipeline:
        pipeline = add_date_filter(pipeline, start_date, end_date)
        data = aggregate_mongo(collection_name, pipeline)
        df = pd.DataFrame(data)
        df = convert_objectid_to_str(df)
        df.to_csv(f"{df_name}.csv", index=False)
        globals()[df_name] = df

def main(start_date=None, end_date=None):
    fetch_and_process_data("aggregation_pipelines.json", "bestselleritems", "bestseller_df", start_date, end_date)

if __name__ == "__main__":
    start_date = None
    end_date = None

    if len(sys.argv) == 3:
        start_date = datetime.strptime(sys.argv[1], "%d/%m/%Y")
        end_date = datetime.strptime(sys.argv[2], "%d/%m/%Y")

    main(start_date, end_date)
