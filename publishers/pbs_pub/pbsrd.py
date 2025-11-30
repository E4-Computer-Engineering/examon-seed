#!/usr/bin/python
# -*- coding: utf-8 -*-
"""

    PBS reader
  
    @author: francesco.beneventi@e4company.com
    
    (c) 2024-2025 E4 Computer Engineering S.p.A.
        
    Return PBS data in json format
    
"""

from __future__ import print_function

import sys
import time
import socket
import logging
from subprocess import Popen, PIPE

from examon.plugin.examonapp import ExamonApp
from sshutil import Ssh_Util


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


            
            
if __name__ == '__main__':

   ########
   # Test #
   ########

   import ConfigParser
   config = ConfigParser.RawConfigParser(allow_no_value=True)
   config.read('pbs_pub.conf')

   # Replace with your actual credentials
   username = config.get('PBS', 'PBS_HOST_USER')
   password = config.get('PBS', 'PBS_HOST_PASSW')
   pkey = config.get('PBS', 'PBS_HOST_KEY')
   host = config.get('PBS', 'PBS_HOSTS')

   cmd_qselect = """ssh -q -t -t {username}@{host} 'qselect -x -tm.gt.$(date -d "5 minutes ago" "+%Y%m%d%H%M") -s QRF | xargs qstat -xfF json -J 2>&1'""".format(username=username, host=host)
   
   schema = None
   
   a  = CmdParser(cmd_qselect, schema, timeout=60, host=host, username=username, password=password, pkey=pkey)
   
   import json
   print(a.read())
