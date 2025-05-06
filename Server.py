from pymongo import MongoClient

def test_mongodb_connection():
    try:
        # Connect to MongoDB and specify the database
        client = MongoClient("mongodb://localhost:27017/")
        db = client["DataAnalyticsDB"]  # Define the database
        collection = db["TestCollection"]  # Create a collection

        # Verify the connection
        client.server_info()  # Check if MongoDB is running
        print("✅ Successfully connected to MongoDB!")

        # Insert sample data to create the database
        sample_data = {"name": "Test Entry", "status": "MongoDB Active"}
        collection.insert_one(sample_data)  # Insert a document

        # Check if the database exists now (after inserting data)
        try:
            db_list = client.list_database_names()  # Fetch list of databases
            if "DataAnalyticsDB" in db_list:
                print("✅ Database 'DataAnalyticsDB' has been created!")
            else:
                print("⚠️ Database 'DataAnalyticsDB' still not found. Try inserting more data.")
        except Exception as db_error:
            print(f"❌ Error retrieving database list: {db_error}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")

    finally:
        client.close()
        print("🔒 MongoDB connection closed.")

# Run the test function
test_mongodb_connection()

