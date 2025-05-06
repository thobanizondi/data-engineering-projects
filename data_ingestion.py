from pymongo import MongoClient
import pandas as pd
import random

class DataIngestion:
    def __init__(self, uri="mongodb://localhost:27017/", db_name="DataAnalyticsDB", collection_name="AnalyticsData"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def generate_sample_data(self, num_records=200):
        """Generate random data for analytics"""
        data = []
        for i in range(num_records):
            record = {
                "user_id": i + 1,
                "name": f"User_{i+1}",
                "age": random.randint(18, 65),
                "country": random.choice(["South Africa", "USA", "India", "Germany", "Brazil"]),
                "purchase_amount": round(random.uniform(100, 5000), 2),
                "signup_date": f"2025-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            }
            data.append(record)
        return data

    def store_data_in_mongo(self, data):
        """Insert generated data into MongoDB"""
        if self.collection.count_documents({}) == 0:
            self.collection.insert_many(data)
            print(f"✅ {len(data)} records inserted into MongoDB!")
        else:
            print("⚠️ Data already exists in MongoDB. Skipping insertion.")

    def export_to_csv(self, filename="analytics_data.csv"):
        """Retrieve MongoDB data & save to CSV for Power BI"""
        data = list(self.collection.find({}, {"_id": 0}))
        if data:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False)
            print(f"✅ Data exported to {filename} for Power BI!")
        else:
            print("❌ No data found in MongoDB!")

    def close_connection(self):
        """Close MongoDB connection"""
        self.client.close()
        print("🔒 MongoDB connection closed.")

# Run the data ingestion process
if __name__ == "__main__":
    data_ingestor = DataIngestion()
    
    # Generate and insert data
    sample_data = data_ingestor.generate_sample_data()
    data_ingestor.store_data_in_mongo(sample_data)

    # Export to CSV
    data_ingestor.export_to_csv()

    # Close connection
    data_ingestor.close_connection()
