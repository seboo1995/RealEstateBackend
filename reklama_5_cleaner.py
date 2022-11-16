from pymongo import MongoClient
import pandas as pd
from dotenv import dotenv_values


config = dotenv_values(".env")
conn_mongo = config.get('conn_mongo')
print(conn_mongo)


def price_is_mkd(price):
    try:
        if 'MKD' in price or 'МКД' in price:
            return None
        else:
            return price
    except:
        return None


def claen_price(price):
    try:
        t = price.strip().replace('\r\n', '').replace(
            ' ', '').replace('.', '').replace('€', '')
        return (t)
    except:
        return None


def get_collection():
    client = MongoClient(conn_mongo)
    database = client['reklama5data']
    collection = database['apartments']
    return collection


def get_df_from_database():
    collection = get_collection()
    df = pd.DataFrame.from_records(collection.find())
    return df


def clean_area(area):
    return int(area.replace('m²', ''))


def calculate_price(row):
    if row.price < 2000:
        return row.price * row.area
    else:
        return row.price


def clean_reklama_5_data():
    df = get_df_from_database()
    df.drop('_id', axis=1, inplace=True)
    df.price = df.price.apply(claen_price)
    df.price = df.price.apply(price_is_mkd)
    df = df[df.price.notna()]
    df.price = df.price.astype(int)
    df = df[df.price > 500]
    df.area = df.area.apply(clean_area)
    df['price_calc'] = df.apply(calculate_price, axis=1)
    df['location'] = df.location.str.split('/').apply(lambda x: x[0])
    return df
