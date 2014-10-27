import os
import platform
import logging.config
import configparser
from pymongo import MongoClient
import csv
import re

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
#         test_mongodb_conn()
#         insert_data()
#         test_mongodb_conn
        clean_data()
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

def clean_data():
    # read csv input file.
    ifile = open(input_data_file)
    reader = csv.reader(ifile, delimiter=',')
    
    # insert to db.
    logr.info('\n---------------------------\n ')
    
#     for line in reader:
#         m = re.match("(.+)(\d\d/\d\d/\d\d\d\d)(.+)", line[6])
#         if m:
#             logr.info('award date: ' + m.group(2))
#         else:
#             logr.error('NO match')

#     for line in reader:
#         m = re.match("(=\"\$)([\w,]+)(\")", line[7])
#         if m:
#             logr.info('award date: ' + m.group(2))
#         else:
#             logr.error('NO match')

        
#     for line in reader:
#         logr.info('out: ' + line[7])    
    
    
    logr.info('\n---------------------------\n ')


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
        dict_temp['awardee'] = line[0]
        dict_temp['doing_business_as_name'] = line[1]
        dict_temp['pdpi_name'] = line[2]
        dict_temp['pdpi_phone'] = line[3]
        dict_temp['pdpi_email'] = line[4]
        dict_temp['copd_copi'] = line[5]
        dict_temp['award_date'] = line[6]
        dict_temp['estimated_total_award_amount'] = line[7]
        dict_temp['funds_obligated_to_date'] = line[8]
        dict_temp['award_start_date'] = line[9]
        dict_temp['award_expiration_date'] = line[10]
        dict_temp['transaction_type'] = line[11]
        dict_temp['agency'] = line[12]
        dict_temp['awarding_agency_code'] = line[13]
        dict_temp['funding_agency_code'] = line[14]
        dict_temp['cfda_number'] = line[15]
        dict_temp['primary_program_source'] = line[16]
        dict_temp['award_title_description'] = line[17]
        dict_temp['federal_award_id_number'] = line[18]
        dict_temp['duns_id'] = line[19]
        dict_temp['parent_duns_id'] = line[20]
        dict_temp['program'] = line[21]
        dict_temp['program_officer_name'] = line[22]
        dict_temp['program_officer_phone'] = line[23]
        dict_temp['program_officer_email'] = line[24]
        dict_temp['awardee_dtreet'] = line[25]
        dict_temp['awardee_city'] = line[26]
        dict_temp['awardee_state'] = line[27]
        dict_temp['awardee_zip'] = line[28]
        dict_temp['awardee_county'] = line[29]
        dict_temp['awardee_country'] = line[30]
        dict_temp['Awardee_cong_district'] = line[31]
        dict_temp['primary_organization_name'] = line[32]
        dict_temp['primary_street'] = line[33]
        dict_temp['primary_city'] = line[34]
        dict_temp['primary_state'] = line[35]
        dict_temp['primary_zip'] = line[36]
        dict_temp['primary_county'] = line[37]
        dict_temp['primary_country'] = line[38]
        dict_temp['primary_cong_district'] = line[39]
        dict_temp['abstract_at_time_of_award'] = line[40]
        dict_temp['publications_produced_as_result_of_research'] = line[41]
        dict_temp['publications_produced_as_conference_proceedings'] = line[42]
        dict_temp['project_outcomes_report'] = line[43]
        obj_id = coll.insert(dict_temp)
        logr.info('insertion successful : ' + str(obj_id) + ' : ' + str(dict_temp))
    
    logr.info('\n---------------------------\n ')
    client.close()



if __name__ == '__main__':
    main()
