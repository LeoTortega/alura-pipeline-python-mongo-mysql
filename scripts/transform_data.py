from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd

def visualize_collection(col):
    print('Imprimindo todos documentos da coleção: ')
    for doc in col.find():
        print(doc)

def rename_collumn(col, col_name, new_name):
    print(f'Renomeando coluna "{col_name}" para "{new_name}"')
    col.update_many(
        {}, 
        {'$rename': {col_name: new_name}}
    )
    return col

def select_category(col, category):
    print(f'Filtrando categoria "{category}"')
    query = {"Categoria do Produto": category}
    lista_filtrada = []

    for doc in col.find(query):
        lista_filtrada.append(doc)

    return lista_filtrada

def create_dataframe(lista):
    print("Criando dataframe")
    return pd.DataFrame(lista)

def format_date(df):
    print('Formatando coluna de data')
    df['Data da Compra'] = pd.to_datetime(df['Data da Compra'], format='%d/%m/%Y')
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')
    
    df.head()

    return df

def make_regex(col, regex):
    print('Filtrando por regex')

    query = {"Data da Compra": {"$regex": f"{regex}"}}

    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    
    return lista_regex

def save_csv(df, path):
    print('Salvando dataframe')
    df.to_csv(path, index=False)

def main():
    print('Pipeline iniciada')
    uri = "mongodb+srv://tortega:12345@cluster-pipeline.75htu.mongodb.net/?retryWrites=true&w=majority&appName=cluster-pipeline"

    client = connect_mongo(uri)

    db = create_connect_db(client, "db_produtos_script")
    collection = create_connect_collection(db, "produtos")

    visualize_collection(collection)

    collection = rename_collumn(collection, 'lat', 'latitude')
    collection = rename_collumn(collection, 'lon', 'longitude')

    # salvando os dados da categoria livros
    lista_livros = select_category(collection, 'livros')
    df_livros = create_dataframe(lista_livros)
    df_livros_formatado = format_date(df_livros)
    save_csv(df_livros_formatado, 'data/tabela_livros_script.csv')

    # salvando os dados dos produtos vendidos a partir de 2021
    lista_produtos = make_regex(collection, "/202[1-9]")
    df_produtos = create_dataframe(lista_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "data/tb_produtos_script.csv")


if __name__ == "__main__":
    main()