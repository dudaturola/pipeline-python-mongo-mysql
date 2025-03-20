import kagglehub
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
import os
from dotenv import load_dotenv
load_dotenv()
client = os.getenv("MONGODB_URI")

# Baixar o dataset 
dataset_path = kagglehub.dataset_download("mahmoudelhemaly/students-grading-dataset")

# Ajuste o nome do arquivo conforme o dataset que vai importar
file_path = f"{dataset_path}/Students_Grading_Dataset.csv"

# Carregar o dataset via pandas
df = pd.read_csv(file_path)

print("Primeiras 5 linhas:", df.head())

# TRANSFORMANDO EM JSON E SALVANDO NA PASTA DATA_JSON
file_path_json = "../data/Students_Grading_Dataset.csv.json"
df.to_json(file_path_json, orient="records",indent=4)
print(f"Arquivo JSON salvo em: {file_path_json}")

def conect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))

    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def creat_connect_db(client,db_name):
    db = client[db_name]
    return db

def create_connect_collection(db,col_name):
    collection = db[col_name]
    return collection

#UTILIZAMOS PARA PEGAR URL
# def extract_api_data(url):
#     response = response.get(url)
#     print(f"Status Code: {response.status_code}")  # Exibe o status da requisição

def extract_api_data(file_path):
    with open(file_path,'r') as file:
        data = json.load(file)
    return data

def insert_data(col,data):
    docs = col.insert_many(data)
    n_docs_inseridos = len(docs.inserted_ids)
    return n_docs_inseridos

if __name__ == "__main__":
    connect = conect_mongo(client)
    db = creat_connect_db(connect,"Dados_Studients")
    col = create_connect_collection(db,"Studients")
    data = extract_api_data("../data_json/Students_Grading_Dataset.csv.json")
    print(f"\nQuantidade de dados extraidos: {len(data)}")
    n_docs = insert_data(col,data)
    print(f"\nDocumentos inseridos na colecao: {n_docs}")

conect_mongo.close()