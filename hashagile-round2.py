from elasticsearch import Elasticsearch, helpers
import pandas as pd

# Initialize the Elasticsearch client with your cloud credentials
client = Elasticsearch(
    "https://1b5a32a33ba345a087fc80b974fc1931.us-central1.gcp.cloud.es.io:443",
    api_key=("ZuUZUpIBkY2-3IoSAJP-", "SfIPb1lzQ6a2HWg3AMdhqg")  # Tuple with (id, api_key)
)

# Function Definitions

def create_collection(collection_name):
    if not client.indices.exists(index=collection_name):
        client.indices.create(index=collection_name)
        print(f"Collection '{collection_name}' created.")
    else:
        print(f"Collection '{collection_name}' already exists.")

def index_data(collection_name, exclude_column):
    # Load the employee data
    try:
        df = pd.read_csv('employee_data.csv', encoding='ISO-8859-1')  # Adjust encoding as needed
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    df = df.drop(columns=[exclude_column])  # Exclude the specified column
    df.fillna('', inplace=True)  # Replace NaNs with empty strings

    # Prepare documents for indexing
    documents = df.to_dict(orient='records')
    print(f"Preparing to index {len(documents)} documents in '{collection_name}'.")

    # Index data
    actions = []
    for i, doc in enumerate(documents):
        action = {
            '_op_type': 'index',
            '_index': collection_name,
            '_id': str(i),
            '_source': doc
        }
        actions.append(action)

    # Bulk index documents
    try:
        helpers.bulk(client, actions)
        print(f"Data indexed in '{collection_name}' excluding column '{exclude_column}'.")
    except helpers.BulkIndexError as e:
        print(f"Error indexing documents: {e.errors}")

def search_by_column(collection_name, column_name, column_value):
    query = {
        "query": {
            "match": {
                column_name: column_value
            }
        }
    }
    results = client.search(index=collection_name, body=query)
    return results['hits']['hits']

def get_emp_count(collection_name):
    count = client.count(index=collection_name)
    return count['count']

def del_emp_by_id(collection_name, employee_id):
    try:
        client.delete(index=collection_name, id=employee_id)
        print(f"Employee with ID '{employee_id}' deleted from '{collection_name}'.")
    except Exception as e:
        print(f"Error deleting employee: {e}")

def get_dep_facet(collection_name):
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    result = client.search(index=collection_name, body=query)
    return result['aggregations']['departments']['buckets']

# Function Executions
name_collection = 'hash_ram'
phone_collection = 'hash_2179'

create_collection(name_collection)
create_collection(phone_collection)

print("Employee Count in Name Collection:", get_emp_count(name_collection))
index_data(name_collection, 'Department')
index_data(phone_collection, 'Gender')

del_emp_by_id(name_collection, 'E02003')

print("Employee Count in Name Collection after deletion:", get_emp_count(name_collection))

print("Search results for 'IT' in Department:", search_by_column(name_collection, 'Department', 'IT'))
print("Search results for 'Male' in Gender:", search_by_column(name_collection, 'Gender', 'Male'))
print("Search results for 'IT' in Phone Collection:", search_by_column(phone_collection, 'Department', 'IT'))

print("Department Facet for Name Collection:", get_dep_facet(name_collection))
print("Department Facet for Phone Collection:", get_dep_facet(phone_collection))
