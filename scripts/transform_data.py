from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd


client = connect_mongo("mongodb+srv://dudaturola:Dudaduda1997*@cluster-pipeline.3kxud.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline")
db = create_connect_db(client,"db_produtos_desafio")
col = create_connect_collection(db,"Produtos_desafio")
    

def visualize_collection(col):
    for doc in col.find():  # Percorre todos os documentos da coleção
        print(doc)  # Imprime cada documento


def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {col_name:new_name}})


def select_category(col, category):
    query = {"Categoria do Produto": category}
    lista_category = []
    for doc in col.find(query):
        lista_category.append(doc)
        
    return lista_category

def make_regex(col, field, regex):
    query = {field:{"$regex":regex}}
    lista_produtos = []

    for doc in col.find(query):
        lista_produtos.append(doc)
    return lista_produtos

def create_dataframe(lista):
    df_produtos = pd.DataFrame(lista)

    return df_produtos

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"],format="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")

    return df

def save_csv(df, path):
    df.to_csv(path)
    return df


if __name__ == "__main__":
    visualize_collection(col)

    rename_column(col,"lat","Latitude")

    rename_column(col,"lon","Longitude")

    select_cat_livros = select_category(col,"livros")

    filtro_prod_vend_maior_2021 = make_regex(col,"Data da Compra","/202[1-9]")

    df_prod_2021 = create_dataframe(filtro_prod_vend_maior_2021)
   
    salvar_csv = save_csv(df_prod_2021,"./df_prod_2021")