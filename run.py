import argparse
from os import path
from pathlib import Path
from multiprocessing import pool
from pymongo import MongoClient
from dateutil.parser import parse as dateparse


def csv_to_mongo(csv_file, tag, collection):
    entries = []
    columns = []
    for i, line in enumerate(csv_file):
        if i == 0:
            columns = [x.lower().replace(' ', '_').strip()
                       for x in line.split(',')]
        else:
            item = {'ccy': tag}
            for colNo, x in enumerate(line.split(',')):
                col_name = columns[colNo]
                item[col_name] = parse(x.strip(), col_name)
            entries.append(item)
    collection.insert_many(entries)


parsers = {
    'date': dateparse,
    'open': lambda x: float(x) if x else 0,
    'close': lambda x: float(x) if x else 0,
    'high': lambda x: float(x) if x else 0,
    'low': lambda x: float(x) if x else 0,
    'market_cap': lambda x: (int(float(x)) if float(x) == int(float(x)) else float(x)) if x else 0,
    'volume': lambda x: (int(float(x)) if float(x) == int(float(x)) else float(x)) if x else 0,
    'ccy': str
}


def parse(x, col_name):
    return parsers[col_name](x)


def get_files(paths):
    if len(paths) == 1:
        p = paths[0]
        if path.exists(p) and not path.isfile(p) and path.isdir(p):
            return [str(p) for p in Path(p).glob('**/*.csv')]

    return paths


def main():
    parser = argparse.ArgumentParser(description='Dump CSV files to mongodb')
    parser.add_argument('file_paths', metavar='F', type=str, nargs='+',
                        help='csv file path')
    parser.add_argument('--hostname', dest='hostname', default='localhost',
                        help='MongoDb hostname')
    parser.add_argument('--port', dest='port', default=27017,
                        help='MongoDb port')
    parser.add_argument('--db', dest='db', help='MongoDb db', required=True)
    parser.add_argument('--collection', dest='collection',
                        help='MongoDb collection', required=True)

    args = parser.parse_args()

    files = [(args, x) for x in get_files(args.file_paths)]
    pl = pool.Pool()
    pl.map(process, files)


def process(p):
    a = p[0]
    file = p[1]
    collection = MongoClient(a.hostname, a.port)[a.db][a.collection]
    name = path.splitext(path.split(file)[1])[0].split('-')[0]
    with open(file) as f:
        csv_to_mongo(f, name, collection)


if __name__ == '__main__':
    main()
