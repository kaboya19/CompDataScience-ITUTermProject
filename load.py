from pyspark.sql import SparkSession
from extract import extract_data
from pymongo import MongoClient
def load_data(data, collection_name):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['load_data'] 
    collection = db[collection_name]

    try:
        collection.insert_many(data)
        print(f"Data successfully written to MongoDB collection: {collection_name}")

    except Exception as e:
        print(f"An error occurred while writing to MongoDB: {e}")

    finally:
        client.close()

if __name__ == "__main__":
    ind_file = 'WLD_2023_SYNTH-SVY-IND-EN_v01_M.csv'
    hld_file = 'WLD_2023_SYNTH-SVY-HLD-EN_v01_M.csv'

    ind_data, hld_data = extract_data(ind_file,hld_file)

    load_data(ind_data, 'individual_collection')
    load_data(hld_data, 'household_collection')
