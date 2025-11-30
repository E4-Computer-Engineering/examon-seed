"""
DISCLAIMER:
This is an UNOFFICIAL tool not affiliated with Enel X.
It is developed for scientific research and interoperability purposes only.
The authors are not responsible for any liability, data loss, or account bans
resulting from the use of this software. Use at your own risk and in compliance
with applicable laws and Terms of Service.
"""
from __future__ import print_function

import sys
import json

from examon.plugin.examonapp import ExamonApp
from examon.plugin.sensorreader import SensorReader
from enelxrd import EnelXClient, DataTransformer

from datetime import datetime, timedelta

   

def read_data(sr):
       
    today = datetime.now().strftime('%d/%m/%Y')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')

    start_date =  yesterday #01/01/2025'
    end_date = today

    # Get mote dictionary from config
    if 'MOTE_DICT' not in sr.conf:
        raise Exception("MOTE_DICT configuration is required")
    mote_dict = json.loads(sr.conf['MOTE_DICT'])
    # Extract mote list for power and energy consumption
    mote_list = list(mote_dict.values())
    # Create devices dict for carbon emissions
    devices = {name: mote_id.split('-')[0] for name, mote_id in mote_dict.items()}
    device_ids = list(devices.values())

    raw_packet = []

    try:
        # Get power consumption data
        power_data = sr.sensor.get_power_consumption(start_date, end_date, mote_list)
        transformed_power_data = DataTransformer.transform_json(power_data, type='power', output_type='list')
        raw_packet.extend(transformed_power_data)

        # Get energy consumption data
        energy_data = sr.sensor.get_energy_consumption(start_date, end_date, mote_list)
        transformed_energy_data = DataTransformer.transform_json(energy_data, type='energy', output_type='list')
        raw_packet.extend(transformed_energy_data)

        # Get carbon emissions data
        carbon_data = sr.sensor.get_carbon_emissions(
            devices=device_ids,
            start_date=start_date,
            end_date=end_date,
            frequency='D'
        )
        transformed_carbon_data = DataTransformer.transform_carbon_json(
            carbon_data,
            devices_dict=devices,
            type='carbon',
            output_type='list'
        )
        raw_packet.extend(transformed_carbon_data)

    except Exception as e:
        sr.logger.error("An error occurred: {}".format(str(e)))

    # build the examon metric
    examon_data = []
    for raw_data in raw_packet:
        metric = {}
        metric['name'] = sr.add_tag_v(raw_data['name'])
        metric['value'] = sr.add_payload_v(raw_data['value'])
        metric['timestamp'] = raw_data['timestamp']
        metric['tags'] = sr.get_tags()
        # dynamically add new custom tags
        metric['tags']['type'] = sr.add_tag_v(raw_data['type'])
        metric['tags']['units'] = sr.add_tag_v(raw_data['units'])
        # build the final packet
        examon_data.append(metric)
        
    # worker id (string) useful for debug/log
    worker_id = 'enelx_pub'
      
    return (worker_id, examon_data,)



                
def worker(conf, tags):
    """
        Worker process code
    """
    # sensor instance 
    client = EnelXClient(conf['ENELX_USERNAME'], 
                         conf['ENELX_PASSWORD'], 
                         conf['ENELX_DEP_ID'], 
                         conf['ENELX_DEP_TOKEN'], 
                         conf['ENELX_ACCOUNT_ID'])
    
    if not client.login():
        raise Exception("Failed to login")
        sys.exit()
    
    # SensorReader app
    sr = SensorReader(conf, client)
    
    # add read_data callback
    sr.read_data = read_data  
    
    # set the default tags
    sr.add_tags(tags)
    
    # run the worker loop
    sr.run()

   
if __name__ == '__main__':

    # start creating an Examon app
    app = ExamonApp()

    app.cfg.parser.add_argument("--username", dest='ENELX_USERNAME', help="EnelX username")
    app.cfg.parser.add_argument("--password", dest='ENELX_PASSWORD', help="EnelX password")
    app.cfg.parser.add_argument("--organization", dest='ORGANIZATION', help="Organization")
    app.cfg.parser.add_argument("--site", dest='SITE', help="Site")
    app.cfg.parser.add_argument("--mote-dict", dest='MOTE_DICT', type=json.loads, 
                               help="Dictionary of mote names and IDs (JSON object)")

    app.parse_opt()
    # for checking
    print("Config:")
    print(json.dumps(app.conf, indent=4))

    # set default metrics tags
    tags = app.examon_tags()
    tags['org']      = app.conf['ORGANIZATION']
    tags['site']     = app.conf['SITE']
    tags['plugin']   = 'enelx_pub'
    tags['chnl']     = 'data'
  
    # add a worker
    app.add_worker(worker, app.conf, tags)
    
    # run!
    app.run()    