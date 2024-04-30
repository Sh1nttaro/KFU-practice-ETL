from dotenv import load_dotenv
import os
import pandas as pd
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine, MetaData, Table


def download_kaggle_dataset(dataset_name, output_path):
    # получение API и zip файла
    api = KaggleApi()

    try:
        api.authenticate()  # аутентификация с помощью API ключа
    except Exception as ex:
        print(f'Ошибка аутентификации: {ex}')

    try:
        kaggle.api.dataset_download_files(dataset_name, path=output_path, unzip=True)
        print(f'Файл успешно скачан и распакован в {output_path}')
    except Exception as ex:
        print(f'Ошибка скачивания файла: {ex}')


def connect_postgresql(dbname, user, password, host, port):
    try:
        db_connection = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
        return create_engine(db_connection)
    except Exception as ex:
        print(f'Ошибка создания соединения с PostreSQL: {ex}')


def save_data_postgresql(df, engine, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    except Exception as ex:
        print(f'Ошибка сохранения данных в PostreSQL: {ex}')


def get_data_postgresql(engine, table_name):
    try:
        return pd.read_sql(f'''SELECT * FROM {table_name}''', engine)
    except Exception as ex:
        print(f'Ошибка получения данных из PostreSQL: {ex}')


def df_drop_empty_data(df):
    df = df.loc[:, ~data.columns.str.contains('^Unnamed')]
    df = df.fillna(0)
    df = df.replace(0, pd.NA)
    df = df.dropna(how='any')
    return df


def connect_clickhouse(dbname, user, password, host, port):
    try:
        db_connection = f'clickhouse://{user}:{password}@{host}:{port}/{dbname}'
        return create_engine(db_connection)
    except Exception as ex:
        print(f'Ошибка создания соединения с Clickhouse: {ex}')


def get_data_clickhouse(engine, table_name):
    try:
        metadata = MetaData(bind=engine)
        table = Table(table_name, metadata, autoload=True)
        query = table.select()
        return engine.execute(query)
    except Exception as ex:
        print(f'Ошибка получения данных из Clickhouse: {ex}')


load_dotenv()
kaggle_api_key = os.getenv('KAGGLE_API_KEY')

download_kaggle_dataset('rahulvyasm/netflix-movies-and-tv-shows', 'netflix_dataset.zip')
data = pd.read_csv('netflix_dataset.zip/netflix_titles.csv', encoding='latin1', delimiter=',')

dbname = os.getenv("dbname")
user_postgres = os.getenv("user_postgres")
password_postgres = os.getenv("password_postgres")
host_postgres = os.getenv("host_postgres")
port_postgres = os.getenv("port_postgres")
engine = connect_postgresql(dbname, user_postgres, password_postgres, host_postgres, port_postgres)

save_data_postgresql(data, engine, 'netflix_film')

data = get_data_postgresql(engine, 'netflix_film')
data = data.drop('show_id', axis=1)
data = df_drop_empty_data(data)

proc_data = data

proc_data.to_csv('netflix_film_proc.csv', index=False, sep=';', encoding='utf-8')

# подключение к clickhouse
dbname = os.getenv("dbname")
user_clickhouse = os.getenv("user_clickhouse")
password_clickhouse = os.getenv("password_clickhouse")
host_clickhouse = os.getenv("host_clickhouse")
port_clickhouse = os.getenv("port_clickhouse")
engine = connect_clickhouse(dbname, user_clickhouse, password_clickhouse, host_clickhouse, port_clickhouse)

data = get_data_clickhouse(engine, 'netflix_film')
