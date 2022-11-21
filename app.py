from pazar_3_cleaner import clean_data
from reklama_5_cleaner import clean_reklama_5_data
from fastapi import FastAPI
import json

app = FastAPI()


@app.get('/pazar_3_data')
async def pazar_3_data():
    pazar_data = clean_data()
    res = pazar_data.to_json(orient='records')
    parsed = json.loads(res)
    return parsed


@app.get('/reklama_5_data')
async def reklama_5_data():
    pazar_data = clean_reklama_5_data()
    res = pazar_data.to_json(orient='records')
    parsed = json.loads(res)
    return parsed

@app.get('/')
def say_hello():
    return {
        'message':'Hello World'
    }
