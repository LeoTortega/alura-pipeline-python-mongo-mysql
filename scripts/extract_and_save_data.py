from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

def connect_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    
    return client

def create_connect_db(client, db_name):
    db = client[db_name]

    print(f'Database {db_name} criado com sucesso')

    return db

def create_connect_collection(db, collection_name):
    collection = db[collection_name]

    print(f'Collection {collection_name} criado com sucesso')

    return collection

def extract_api_data(url):
    response = requests.get(url)
    response_json = response.json()
    print(f'Dados extraídos com sucesso')
    print(f'Quantidade: {len(response_json)}')

    return response_json

def insert_data(col, data):
    docs = col.insert_many(data)

    print('Dados inseridos com sucesso')

    return docs

def main():
    print('Pipeline iniciada')
    uri = "mongodb+srv://tortega:12345@cluster-pipeline.75htu.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline"

    client = connect_mongo(uri)

    db = create_connect_db(client, "db_produtos_script")
    collection = create_connect_collection(db, "produtos")

    data = extract_api_data("https://labdados.com/produtos")

    insert_data(collection, data)

    client.close()
    print('Pipeline finalizada')

if __name__ == '__main__':
    main()