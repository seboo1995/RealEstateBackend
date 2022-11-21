import pandas as pd
import re
import sqlalchemy
from dotenv import dotenv_values
import os
#config = dotenv_values(".env")
dsn = os.environ.get("postgre_dsn")
#print(dsn)
#dsn = os.getenv('dsn')


def create_engine():
    return sqlalchemy.create_engine(url=dsn)


def read_from_database():
    engine = create_engine()
    df = pd.read_sql_table('pazar3data', con=engine)
    return df


def get_location_from_title(row):
    res = [ele for ele in list_with_mun if (
        ele.lower() in row.title.lower().strip())]
    print(row.title)
    print(res)
    print('----------------------------')
    if len(res) != 0:
        return (res[0].title())
    else:
        return row.location
        print('Nothing')
    print('-----------------------------------')


def check_area_in_title(title):
    temp = re.search('[0-9]*m2', title)
    if temp != None:
        start = temp.start()
        end = temp.end()
        return title[start:end-2]
    else:
        return None


def clean_price(price):
    try:
        return int(price.replace('\r\n ', '').replace('mk', '').replace('ЕУР', '').replace(' ', ''))
    except ValueError:
        return None


list_with_mun = [
    'Centar',
    'Aerodrom',
    'Kisela Voda',
    'Ǵorče Petrov',
    'Karpoš',
    'Karpos'
    'Čair',
    'Gazi Baba',
    'Butel',
    'Sopište',
    'Vodno',
    'Cair',
    'Lisice',
    'Taftalidze',
    'Gorce Petrov',
    'Kapistec'
    'Debar Maalo',
    'Karpos',
    'Vlae',
    'Avtokomanda',
    'Lisice',
    'Debar',
    'Kapistec',
    'Gjorce Petrov',
    'Hrom',
    'CEVAHIR',
    'Taftalidže',
    'Kozle',
    'D.Maalo',
    'Madzari',
    'K.voda',
    'Sever'
]


def clean_data():
    df = read_from_database()
    df = df.groupby('link').first().reset_index()
    df['area'] = df.title.apply(check_area_in_title)
    df['location'] = df.apply(get_location_from_title, axis=1)
    maping = {
        'Karpoš': 'Karpos',
        'Čair': 'Cair',
        'Taftalidže': 'Taftalidze',
        'D.Maalo': 'Debar Maalo',
        'Ǵorče Petrov': "Gorce Petrov",
        'K.Voda': 'Kisela Voda',
        'Gjorce Petrov': 'Gorce Petrov'

    }
    df.location = df.location.replace(maping)
    df.price = df.price.apply(clean_price)
    df = df[df.area.notna() & df.price.notna()]
    return df


clean_data()
