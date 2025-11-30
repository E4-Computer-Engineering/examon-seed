#!/usr/bin/python
# -*- coding: utf-8 -*-
"""

    Bright Cluster Manager reader

    @author: francesco.beneventi@e4company.com
    
    (c) 2024 E4 Computer Engineering S.p.A.
        
    Return BCM Monitoring data in json format
    
"""

from __future__ import print_function

import sys
import time
import socket
import logging
from subprocess import Popen, PIPE

from examon.plugin.examonapp import ExamonApp
from sshutil import Ssh_Util

import pandas as pd
import re

logger = logging.getLogger('examon')

# Cache for metrics CSV to avoid repeated file reads
_metrics_cache = {}

def load_metrics_csv(metrics_csv_path):
    """Load and cache the metrics CSV file"""
    if metrics_csv_path not in _metrics_cache:
        logger.debug("Loading metrics CSV from: %s" % metrics_csv_path)
        metrics_df = pd.read_csv(metrics_csv_path, sep='|')
        _metrics_cache[metrics_csv_path] = metrics_df.set_index('Name')['Unit'].to_dict()
    return _metrics_cache[metrics_csv_path]


class CmdParser():
    """
        Local or remote host command parser
    """

    def __init__(self, tool_cmd, output_schema, host=None, username=None, password=None, timeout=30, pkey=None, port=22, sep=';', skipline=0):
        self.tool_cmd = tool_cmd
        self.schema = output_schema
        self.sep = sep
        self.skipline = skipline
        self.host = host
        self.this_hostname = socket.gethostname()
        self.local = False
        if self.host in [self.this_hostname, '127.0.0.1', 'localhost']:
            self.local = True
        self.ssh_client = None
        if self.host is not None:
            self.ssh_client = Ssh_Util(host,username,password=password, timeout=timeout, pkey=pkey, port=port) 
        self.logger = logging.getLogger('examon')
        
        
    def run_cmd(self):
        """
            Execute command and return the shell output (string)
        """
            
        output = ''
        retry = 6
        flag = False
        err = ''

        try:
            if self.local:
                self.logger.info("%s: executing command locally...%s" % (self.host,self.tool_cmd))
                child = Popen(self.tool_cmd, shell=True, stdout=PIPE)
                output = child.communicate()[0]
            else:
                while retry:
                    if self.ssh_client.connect():
                        flag, output, err = self.ssh_client.exec_command(self.tool_cmd)
                        self.ssh_client.close()
                        if flag:
                            break
                        else:
                            print(err)
                            retry -= 1
                    else:
                        retry -= 1
                    if retry == 0:
                        self.logger.warning('Max retry attempts reached! Flag: %s, Err: %s' % (str(flag), str(err)))
                        time.sleep(60)
                        retry = 6
        except:
            self.logger.exception('Uncaught exception in run_cmd()!')
        
        return output

        
    def parse_buffer_sep(self, buffer, schema, sep=';', skip_line_num=None):
        """Parse cmd output, schema based.
            
            Return a generator of sampled values (list of dict rows)
            - buffer = lines '\n' separated
            - schema = list of headers labels or dict.
            - sep = column separator
            - skip_line_num = number of buffer line to skip (headers ...)
            example:    
                - header = ['item0','item1', None, ..]
                    - to skip a column put None in the corresponding column.
                - header = {'item': [col_pos, datatype], 'item': [col_pos, datatype],...}
                    - to filter, enter only the desired column number.
        """ 
        def parse_list(schema, line):
            row = zip(schema, line)
            return dict(filter(lambda x: x[0] is not None, row))
        def parse_dict(schema, line):
            return {k:v[1](line[v[0]]) for k,v in schema.iteritems()}
            
        if isinstance(schema, list):
            parse_line = parse_list
        if isinstance(schema, dict):
            parse_line = parse_dict
        
        lines = buffer.split('\n')
        for line in lines:
            if skip_line_num:
                skip_line_num -= 1
                continue
            tmpline = line.split(sep)
            tmpline = [x.strip() for x in tmpline]
            if len(tmpline) == len(schema):
                yield parse_line(schema,tmpline)
                        
        
    def read(self):
        """return timestamp, values"""
        if self.schema:
            return (time.time(), self.parse_buffer_sep(self.run_cmd(), self.schema, skip_line_num=self.skipline, sep=self.sep))
        else:
            return (time.time(), self.run_cmd())

# Convert the "Age" column to milliseconds
def convert_age_to_ms(age_str):
    time_units = {
        'y': 365 * 24 * 60 * 60 * 1000,  # years to milliseconds
        'mo': 30 * 24 * 60 * 60 * 1000,  # months to milliseconds
        'w': 7 * 24 * 60 * 60 * 1000,    # weeks to milliseconds
        'd': 24 * 60 * 60 * 1000,        # days to milliseconds
        'h': 60 * 60 * 1000,             # hours to milliseconds
        'm': 60 * 1000,                  # minutes to milliseconds
        's': 1000,                       # seconds to milliseconds
        'ms': 1                          # milliseconds
    }

    total_ms = 0
    matches = re.findall(r'(\d+\.?\d*)([a-z]+)', age_str)
    for value, unit in matches:
        if unit in time_units:
            total_ms += int(float(value) * time_units[unit])
    logger.debug("Age_str %s = total_ms %f" % (age_str, total_ms) )
    return total_ms

def process_bcm_data(read_result, metrics_csv_path):
    timestamp, csv_data = read_result
    csv_data = csv_data.decode('utf-8')

    # Load the CSV data into a pandas dataframe
    payload_df = pd.read_csv(pd.compat.StringIO(csv_data), sep=';', header=None, names=[
        "Entity", "Measurable", "Parameter", "Type", "Value", "Age", "State", "Info"
    ])

    # Strip leading and trailing spaces from all elements
    payload_df = payload_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Get the unit_map 
    unit_map = load_metrics_csv(metrics_csv_path)

    # Add the unit column to the payload dataframe
    payload_df['Unit'] = payload_df['Measurable'].apply(lambda x: unit_map.get(x.strip(), None))

    payload_df['Age'] = payload_df['Age'].apply(convert_age_to_ms)

    # Add the "timestamp" column
    current_time_ms = int(timestamp * 1000)
    payload_df['timestamp'] = current_time_ms - payload_df['Age']

    payload_df['timestamp'] = payload_df['timestamp'].astype('int64')

    return payload_df

def get_hash(d):
    h = [x for x in d['tags'].items()]
    h.append(('name',d['name']))
    return hash(frozenset(h))       
            
if __name__ == '__main__':

   
    cmd_bcm = conf.get('BCM_SHELL', '/cm/local/apps/cmd/bin/cmsh')

    schema = None
    
    a  = CmdParser(cmd_bcm, schema, timeout=60, host=conf.get('BCM_HOST', 'localhost'), username=conf.get('BCM_USERNAME', None), password=None, pkey=None)
    
    payload_df = process_bcm_data(a.read(), './metrics.csv')

    print(payload_df)
