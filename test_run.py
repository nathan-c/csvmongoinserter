from os import replace, path
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient
from run import csv_to_mongo


def test_csv_to_mongo():
    collection = MongoClient('localhost', 27017).test.test

    with open('ABN.csv') as f:
        csv_to_mongo(f, 'ABN', collection)

def test_mongo():
    collection = MongoClient('localhost', 27017).test.test
    data = [{'num':x, 'date':datetime.utcnow()} for x in range(50000)]
    collection.insert_many(data)
