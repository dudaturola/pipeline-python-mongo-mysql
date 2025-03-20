from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import os
from dotenv import load_dotenv
load_dotenv()
client = os.getenv("MONGODB_URI")

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
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name]
    return collection

import requests

def extract_api_data(url):
    response = requests.get(url)
    
    print(f"Status Code: {response.status_code}")  # Exibe o status da requisição
    print(f"Resposta da API (primeiros 500 caracteres): {response.text[:500]}")  # Exibe o conteúdo da resposta

    if response.status_code != 200:  # Verifica se a resposta foi bem-sucedida
        raise Exception(f"Erro ao acessar API: {response.status_code}")
    
    try:
        return response.json()  # Tenta converter para JSON
    except requests.exceptions.JSONDecodeError:
        raise Exception("Erro ao decodificar JSON, resposta vazia ou inválida.")



def insert_data(col, data):
    docs = col.insert_many(data)
    n_docs_inseridos = len(docs.inserted_ids)
    return n_docs_inseridos

if __name__ == "__main__":

    connect = connect_mongo(client)
    db = create_connect_db(connect,"db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    data = extract_api_data("https://labdados.com/produtos")
    print(f"\nQuantidade de dados extraidos: {len(data)}")

    n_docs = insert_data(col, data)
    print(f"\nDocumentos inseridos na colecao: {n_docs}")

    connect.close()

