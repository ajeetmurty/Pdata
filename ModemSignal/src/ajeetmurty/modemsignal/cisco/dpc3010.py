import logging.config
import os
import platform
import re
from datetime import datetime
from requests import Request, Session
from bs4 import BeautifulSoup

logging.config.fileConfig('logging.conf')
logr = logging.getLogger('pylog')
google_url = 'http://192.168.100.1/Docsis_system.asp'

def main():
    logr.info('start modem')
    try:
        print_sys_info()
        parsing_output_dict = do_parsing()
        logr.info('parsing output: ' + str(parsing_output_dict))
    except Exception: 
        logr.exception('Exception')
    logr.info('stop')

def print_sys_info():
    logr.info('login|hostname|os|python : {0}|{1}|{2}|{3}.'.format(os.getlogin(), platform.node() , platform.system() + '-' + platform.release() , platform.python_version()))

def do_parsing():
    logr.info('get info from: ' + google_url)
    session = Session()
    prepped = Request('GET', google_url).prepare()
    response = session.send(prepped, stream=True, verify=False, timeout=10)
    content = response.raw.read().decode()

    if response:
        if(response.status_code == 200):
            logr.info('response: ' + content.replace('\n', ' '))
            soup = BeautifulSoup(content)
            output_dict = {}
#             output_dict['url'] = google_url
#             output_dict['request_timestamp'] = datetime.utcnow()
#             output_dict['model'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='Model').contents))
#             output_dict['vendor'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='Vendor').contents))
#             output_dict['hardware-revision'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='HardwareRevision').contents))
#             output_dict['mac-address'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='MACAddress').contents))
#             output_dict['boot-loader-revision'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='BootloaderRevision').contents))
#             output_dict['current-software-revision'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='CurrentSoftwareRevision').contents))
#             output_dict['firmware-name'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='FirmwareName').contents))
#             output_dict['firmware-build-time'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='FirmwareBuildTime').contents))
#             output_dict['cable-modem-status'] = re.sub('[\\\(n)\s\t]+', ' ', str(soup.find('td', headers='CableModemStatus').contents))

            output_dict['channel_1 ch_pwr'] = str(soup.find('td', headers='channel_1 ch_pwr').contents)



            
            return output_dict
        else:
            raise Exception('fail http response code: ' + str(response.status_code))
    else:
        raise Exception('null response object')        
    
if __name__ == '__main__':
    main()
