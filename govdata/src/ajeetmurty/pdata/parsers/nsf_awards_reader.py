import os
import platform
import logging.config
import configparser
from pymongo import MongoClient

logging.config.fileConfig('logging.conf')
logr = logging.getLogger('pylog')
config = configparser.ConfigParser()
config.read('config/mongodb_mongolab.ini')
MONGODB_URI = config.get('connection_params', 'uri')
MONGODB_HOST = config.get('connection_params', 'host')
MONGODB_PORT = config.get('connection_params', 'port')
MONGODB_DATABASE = config.get('connection_params', 'database')
MONGODB_COLLECTION = config.get('connection_params', 'collection')
MONGODB_TOKEN01 = config.get('connection_params', 'token01')
MONGODB_TOKEN02 = config.get('connection_params', 'token02')
input_data_file = 'data/exportAwards-2013.csv'

def main():
    logr.info('start')
    try:
        print_sys_info()
        test_mongodb_conn()
        # get_data()
        get_data_by_state('MA')
    except Exception: 
        logr.exception('Exception')
    logr.info('stop')

def print_sys_info():
    logr.info('login|hostname|os|python : {0}|{1}|{2}|{3}.'.format(os.getlogin(), platform.node() , platform.system() + '-' + platform.release() , platform.python_version()))

def test_mongodb_conn():
    conn_string = MONGODB_URI.format(MONGODB_TOKEN01, MONGODB_TOKEN02, MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE)
    logr.info('connecting uri : ' + conn_string)
    client = MongoClient(conn_string)
    logr.info('mongodb server info: ' + str(client.server_info()))
    db = client[MONGODB_DATABASE]
    coll = db[MONGODB_COLLECTION]
    logr.info('total document count in collection: ' + str(coll.count()))
    client.close()

def get_data():
    # connect to db.
    conn_string = MONGODB_URI.format(MONGODB_TOKEN01, MONGODB_TOKEN02, MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE)
    logr.info('connecting uri : ' + conn_string)
    client = MongoClient(conn_string)
    db = client[MONGODB_DATABASE]
    coll = db[MONGODB_COLLECTION]
    logr.info('\n---------------------------\n ')
    results = coll.find()
    counter = 0
    logr.info('document count: ' + str(results.count()))
    for record in results:
        counter += 1
        print(str(counter) + '-->\t' + str(record))
    logr.info('\n---------------------------\n ')

def get_data_by_state(award_state):
    # connect to db.
    conn_string = MONGODB_URI.format(MONGODB_TOKEN01, MONGODB_TOKEN02, MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE)
    logr.info('connecting uri : ' + conn_string)
    client = MongoClient(conn_string)
    db = client[MONGODB_DATABASE]
    coll = db[MONGODB_COLLECTION]
    logr.info('\n---------------------------\n ')
    results = coll.find({'primary_state': award_state})
    counter = 0
    logr.info('document count: ' + str(results.count()))
    for record in results:
        counter += 1
        print(str(counter) + '-->\t' + str(record['primary_state'] +' , '+ record['estimated_total_award_amount']+' , '+record['award_title_description']))
    logr.info('\n---------------------------\n ')

if __name__ == '__main__':
    main()
