import os
import platform
import logging.config
import configparser
from pymongo import MongoClient
import csv

logging.config.fileConfig('logging.conf')
logr = logging.getLogger('pylog')
config = configparser.ConfigParser()
config.read('config/mongodb_umlinux.ini')
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
        # insert_data()
        # test_mongodb_conn
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
    logr.info('document count: ' + str(coll.count()))
    client.close()

def insert_data():
    # read csv input file.
    ifile = open(input_data_file)
    reader = csv.reader(ifile, delimiter=',')
    
    # connect to db.
    conn_string = MONGODB_URI.format(MONGODB_TOKEN01, MONGODB_TOKEN02, MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE)
    logr.info('connecting uri : ' + conn_string)
    client = MongoClient(conn_string)
    db = client[MONGODB_DATABASE]
    coll = db[MONGODB_COLLECTION]

    # insert to db.
    logr.info('\n---------------------------\n ')
    
    for line in reader:
        dict_temp = {}
        dict_temp['Awardee'] = line[0]
        dict_temp['Doing Business As Name'] = line[1]
        dict_temp['PD/PI Name'] = line[2]
        dict_temp['PD/PI Phone'] = line[3]
        dict_temp['PD/PI Email'] = line[4]
        dict_temp['Co-PD(s)/co-PI(s)'] = line[5]
        dict_temp['Award Date'] = line[6]
        dict_temp['Estimated Total Award Amount'] = line[7]
        dict_temp['Funds Obligated to Date'] = line[8]
        dict_temp['Award Start Date'] = line[9]
        dict_temp['Award Expiration Date'] = line[10]
        dict_temp['Transaction Type'] = line[11]
        dict_temp['Agency'] = line[12]
        dict_temp['Awarding Agency Code'] = line[13]
        dict_temp['Funding Agency Code'] = line[14]
        dict_temp['CFDA Number'] = line[15]
        dict_temp['Primary Program Source'] = line[16]
        dict_temp['Award Title or Description'] = line[17]
        dict_temp['Federal Award ID Number'] = line[18]
        dict_temp['DUNS ID'] = line[19]
        dict_temp['Parent DUNS ID'] = line[20]
        dict_temp['Program'] = line[21]
        dict_temp['Program Officer Name'] = line[22]
        dict_temp['Program Officer Phone'] = line[23]
        dict_temp['Program Officer Email'] = line[24]
        dict_temp['Awardee Street'] = line[25]
        dict_temp['Awardee City'] = line[26]
        dict_temp['Awardee State'] = line[27]
        dict_temp['Awardee ZIP'] = line[28]
        dict_temp['Awardee County'] = line[29]
        dict_temp['Awardee Country'] = line[30]
        dict_temp['Awardee Cong District'] = line[31]
        dict_temp['Primary Organization Name'] = line[32]
        dict_temp['Primary Street'] = line[33]
        dict_temp['Primary City'] = line[34]
        dict_temp['Primary State'] = line[35]
        dict_temp['Primary ZIP'] = line[36]
        dict_temp['Primary County'] = line[37]
        dict_temp['Primary Country'] = line[38]
        dict_temp['Primary Cong District'] = line[39]
        dict_temp['Abstract at Time of Award'] = line[40]
        dict_temp['Publications Produced as a Result of this Research'] = line[41]
        dict_temp['Publications Produced as Conference Proceedings'] = line[42]
        dict_temp['ProjectOutcomesReport'] = line[43]
        obj_id = coll.insert(dict_temp)
        logr.info('insertion successful : ' + str(obj_id) + ' : ' + str(dict_temp))
    
    logr.info('\n---------------------------\n ')
    client.close()



if __name__ == '__main__':
    main()
