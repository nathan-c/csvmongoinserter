from pymongo import MongoClient


def csv_to_mongo(csv_file, ccy):
    collection = MongoClient('localhost', 27017).crypto.crypto
    columns = []
    for i, line in enumerate(csv_file):
        if i == 0:
            columns = line.split(',')
        else:
            item = {'ccy': ccy}
            for colNo, x in enumerate(line.split(',')):
                item[columns[colNo]] = x
            collection.insert_one(item)


with open('ABN.csv') as f:
    csv_to_mongo(f, 'ABN')
