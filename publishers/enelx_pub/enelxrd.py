"""
DISCLAIMER:
This is an UNOFFICIAL tool not affiliated with Enel X.
It is developed for scientific research and interoperability purposes only.
The authors are not responsible for any liability, data loss, or account bans
resulting from the use of this software. Use at your own risk and in compliance
with applicable laws and Terms of Service.
"""
from __future__ import print_function

import os
import re
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import pytz

class EnelXClient:
    def __init__(self, username, password, dep_id, dep_token, account_id, base_url='https://ems.enelx.com'):
        self.username = username
        self.password = password
        self.base_url = base_url
        self.session = requests.Session()
        self.headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cache-Control": "max-age=0"
        }
        self.dep_id = dep_id
        self.dep_token = dep_token
        self.account_id = account_id

    def login(self):
        login_url = '{}/j_spring_security_check'.format(self.base_url)
        payload = {
            'j_username': self.username,
            'j_password': self.password,
            'j_action': '',
            'j_value': ''
        }
        response = self.session.post(login_url, data=payload, allow_redirects=True)
        return response.ok

    def get_power_consumption(self, start_date, end_date, mote_list, param='40205'):
        url = '{}/l_107710/analysis/demand/display.htm'.format(self.base_url)
        payload = {
            'selectedMoteList': mote_list,
            'initDate': start_date,
            'endDate': end_date
        }
        params = {
            'param': param,
            'date': start_date
        }
        response = self.session.post(url, data=payload, params=params, headers=self.headers, allow_redirects=True)

        if response.ok:
            return self.get_data(response)
        else:
            raise requests.RequestException("Failed to get power consumption data. Status code: {}".format(response.status_code))

    def extract_hash(self, content):
        match = re.search(r'createDefaultStockChart\("[^"]*", "([^"]*)"', str(content))
        return match.group(1) if match else None

    def get_consumption_data(self, hash_value):
        url = '{}/l_107710/analysis/export.json?hash={}'.format(self.base_url, hash_value)
        response = self.session.get(url, allow_redirects=True)
        return json.loads(response.content)

    def get_energy_consumption(self, start_date, end_date, mote_list, network_id='402', freq='QUARTER'):
        url = '{}/l_107710/analysis/consumption/loadChartData.htm'.format(self.base_url)
        payload = {
            'networkId': network_id,
            'serviceFrequency': freq,
            'showComment': '1',
            'graphType': 'line',
            'selectedMoteList': mote_list,
            'fromDate': start_date,
            'toDate': end_date,
            'periodAction': '',
        }
        response = self.session.post(url, data=payload, headers=self.headers, allow_redirects=True)

        if response.ok:
            return self.get_data(response)
        else:
            raise requests.RequestException("Failed to get energy consumption data. Status code: {}".format(response.status_code))

    def get_data(self, response):
            content = response.content
            hash_value = self.extract_hash(content)
            if hash_value:
                return self.get_consumption_data(hash_value)
            else:
                raise ValueError("Failed to extract hash from the response.")

    def get_carbon_session_id(self):
        """Get the carbon API session ID from the market view page"""
        url = '{}/l_107710/marketView/show/analysis/7799.htm'.format(self.base_url)
        
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,zh;q=0.5",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": url
        }
        
        response = self.session.get(url, headers=headers)
        
        if not response.ok:
            raise requests.RequestException("Failed to get market view page")
            
        # Extract session ID from iframe URL using regex
        match = re.search(r'session_id=([^"&]+)', response.text)
        if not match:
            raise ValueError("Could not find session ID in the response")
            
        return match.group(1)

    def get_carbon_emissions(self, devices, start_date, end_date, frequency='D', 
                           energy_source='ELECTRICITY', emission_type='CARBONDIOX'):
        """
        Get carbon emissions data for specified devices and date range.
        """
        # Get the session ID from the market view page
        session_id = self.get_carbon_session_id()

        url = 'https://carbon-api.enerapp.com/accounts/{}/carbonEmissions/_search'.format(self.account_id)
        
        payload = {
            "energySource": energy_source,
            "emissionType": emission_type,
            "frequency": frequency,
            "devices": devices,
            "range": {
                "fromDate": start_date,
                "toDate": end_date
            }
        }

        json_headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6,zh;q=0.5",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://carbon-app.enerapp.com",
            "Referer": "https://carbon-app.enerapp.com/",
            "x-dexma-dep-id": self.dep_id,
            "x-dexma-dep-token": self.dep_token,
            "x-dexma-session-id": session_id,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
        
        response = self.session.post(url, json=payload, headers=json_headers)
        
        if response.ok:
            return response.json()
        else:
            raise requests.RequestException(
                "Failed to get carbon emissions data. Status code: {}. Response: {}".format(response.status_code, response.text)
            )

class DataTransformer:
    @staticmethod
    def transform_json(data, type='', output_type='json', timezone='Europe/Rome'):
        # Parse the input JSON
        #data = json.loads(input_json)

        # Create a mapping of id to name and units
        series_map = {s['id']: {'name': s['name'], 'units': s['units']} for s in data['seriesList']}

        # Determine the local timezone
        local_tz = pytz.timezone(timezone)

        # Initialize the result list
        result = []

        # Process each chart element
        for element in data['chartElementList']:
            timestamp = datetime.strptime(element['timestamp'], '%Y/%m/%d %H:%M')
            timestamp = local_tz.localize(timestamp)
            # Convert datetime to epoch in Python 2.7 compatible way
            epoch = int((timestamp - datetime(1970, 1, 1, tzinfo=pytz.UTC)).total_seconds())*1000
            #epoch = int(timestamp.timestamp())*1000  #python3

            for series_id, value in element['values'].items():
                result.append({
                    "timestamp": epoch,
                    "name": series_map[series_id]['name'],
                    "value": value,
                    "units": series_map[series_id]['units'],
                    "type": type
                })

        if output_type == 'json':
            return json.dumps(result, indent=2)
        else:
            return result

    @staticmethod
    def transform_carbon_json(data, devices_dict=None, type='carbon', output_type='json', timezone='Europe/Rome'):
        """
        Transform carbon emissions data to match the standard format.
        
        Args:
            data (dict): Carbon emissions API response
            devices_dict (dict): Dictionary mapping device names to IDs
            output_type (str): Output format ('json' or 'dict')
            timezone (str): Timezone for timestamp conversion
        """
        # Determine the local timezone
        local_tz = pytz.timezone(timezone)

        # Create reverse mapping from device ID to name
        id_to_name = {}
        if devices_dict:
            id_to_name = {str(id): name for name, id in devices_dict.items()}

        # Initialize the result list
        result = []

        # Process each reading
        for reading in data['readings']:
            # Convert DD/MM/YYYY to datetime
            timestamp = datetime.strptime(reading['timestamp'], '%d/%m/%Y')
            timestamp = local_tz.localize(timestamp)
            # Convert to epoch milliseconds
            epoch = int((timestamp - datetime(1970, 1, 1, tzinfo=pytz.UTC)).total_seconds())*1000

            # Process each value in the reading
            for value_entry in reading['values']:
                device_id = str(value_entry['deviceId'])
                # Use the device name from dictionary if available, otherwise use device ID
                device_name = id_to_name.get(device_id, "Device {}".format(device_id))
                
                result.append({
                    "timestamp": epoch,
                    "name": device_name,
                    "value": value_entry['value'],
                    "units": data['unit'],
                    "type": type
                })

        if output_type == 'json':
            return json.dumps(result, indent=2)
        else:
            return result

def main():

    ########
    # Test #
    ########

    import ConfigParser
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('example_enelx_pub.conf')

    # Replace with your actual credentials
    username = config.get('ENELX', 'ENELX_USERNAME')
    password = config.get('ENELX', 'ENELX_PASSWORD')
    dep_id = config.get('ENELX', 'ENELX_DEP_ID')
    dep_token = config.get('ENELX', 'ENELX_DEP_TOKEN')
    account_id = config.get('ENELX', 'ENELX_ACCOUNT_ID')

    client = EnelXClient(username, password, dep_id, dep_token, account_id)

    if client.login():
        print("Login successful!")

        today = datetime.now().strftime('%d/%m/%Y')

        start_date = '01/02/2025'
        end_date = today

        # Get mote dictionary from configuration
        mote_dict = json.loads(config.get('ENELX', 'MOTE_DICT'))
        mote_list = mote_dict.values()

        try:
            # Get power consumption data
            power_data= client.get_power_consumption(start_date, end_date, mote_list)
            transformed_power_data = DataTransformer.transform_json(power_data, type='power')
            print("Power Consumption Data:")
            print(transformed_power_data)

            # Get energy consumption data
            energy_data = client.get_energy_consumption(start_date, end_date, mote_list)
            transformed_energy_data = DataTransformer.transform_json(energy_data, type='energy')
            print("\nEnergy Consumption Data:")
            print(transformed_energy_data)

            # Create dictionary mapping mote names to device IDs
            devices = {name: mote_id.split('-')[0] for name, mote_id in mote_dict.items()}
            # Extract just the device IDs for the API call
            device_ids = list(devices.values())
            
            carbon_data = client.get_carbon_emissions(
                devices=device_ids,
                start_date=start_date,
                end_date=end_date,
                frequency='D'
            )
            transformed_carbon_data = DataTransformer.transform_carbon_json(
                carbon_data, 
                devices_dict=devices,
                type='carbon'
            )
            print("\nCarbon Emissions Data:")
            print(transformed_carbon_data)

        except Exception as e:
            print("An error occurred: {}".format(str(e)))
    else:
        print("Login failed.")

if __name__ == "__main__":
    main()