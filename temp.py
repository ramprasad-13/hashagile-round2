from elasticsearch import Elasticsearch, helpers

# Initialize Elasticsearch client
client = Elasticsearch(
    "https://1b5a32a33ba345a087fc80b974fc1931.us-central1.gcp.cloud.es.io:443",
    api_key=("ZuUZUpIBkY2-3IoSAJP-", "SfIPb1lzQ6a2HWg3AMdhqg")  # Tuple with (id, api_key)
)

# Check connection
try:
    client.info()
    print("Successfully connected to Elasticsearch.")
except Exception as e:
    print(f"Error connecting to Elasticsearch: {e}")

# Documents to index
documents = [
    {"_index": "index_name", "_id": "9780553351927", "_source": {"name": "Snow Crash", "author": "Neal Stephenson", "release_date": "1992-06-01", "page_count": 470}},
    {"_index": "index_name", "_id": "9780441017225", "_source": {"name": "Revelation Space", "author": "Alastair Reynolds", "release_date": "2000-03-15", "page_count": 585}},
    {"_index": "index_name", "_id": "9780451524935", "_source": {"name": "1984", "author": "George Orwell", "release_date": "1985-06-01", "page_count": 328}},
    {"_index": "index_name", "_id": "9781451673319", "_source": {"name": "Fahrenheit 451", "author": "Ray Bradbury", "release_date": "1953-10-15", "page_count": 227}},
    {"_index": "index_name", "_id": "9780060850524", "_source": {"name": "Brave New World", "author": "Aldous Huxley", "release_date": "1932-06-01", "page_count": 268}},
    {"_index": "index_name", "_id": "9780385490818", "_source": {"name": "The Handmaid's Tale", "author": "Margaret Atwood", "release_date": "1985-06-01", "page_count": 311}},
]

# Index documents
try:
    helpers.bulk(client, documents)
    print("Documents indexed successfully.")
except Exception as e:
    print(f"Error indexing documents: {e}")

# Search for documents
try:
    search_result = client.search(index="index_name", body={
        "query": {
            "match": {
                "name": "snow"
            }
        }
    })
    print("Search results:", search_result['hits']['hits'])
except Exception as e:
    print(f"Error searching documents: {e}")
