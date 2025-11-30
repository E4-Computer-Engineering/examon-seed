#!/usr/bin/python
# -*- coding: utf-8 -*-
"""

    BCM stream reader
    
    @author: francesco.beneventi@e4company.com
    
    (c) 2024 E4 Computer Engineering S.p.A.
    
   
"""

import os
import sys
import time
import socket
import logging
from subprocess import Popen, PIPE

import threading
from Queue import Queue, Empty


def listening_process(child, q):
    
    #global child
    
    #child = Popen(cmd.split(' '), stdin=PIPE, stdout=PIPE)

    with child.stdout:
        for line in iter(child.stdout.readline, ""):
            q.put(line)


class CmdParser():
    """
        Local or remote host command parser
    """

    def __init__(self, shell_cmd, tool_cmd, output_schema, column_num, stop_sequence, host=None, username=None, password=None, timeout=60, pkey=None, port=22, sep=';', skipline=0):
        self.shell_cmd = shell_cmd
        self.tool_cmd = tool_cmd
        self.schema = output_schema
        self.stop_sequence = stop_sequence
        self.column_num = column_num
        self.sep = sep
        self.skipline = skipline
        self.host = host
        self.this_hostname = socket.gethostname()
        self.timeout = timeout
        self.local = False
        if self.host in [self.this_hostname, '127.0.0.1', 'localhost']:
            self.local = True
        self.ssh_client = None            
        self.logger = logging.getLogger('examon')
        self.res_q = Queue()
        self.t = None
        self.child = None
        self.init_thread()
        


    def __del__(self):
        """
        Method executed when the object is garbage collected.
        """
        self.clean()

    def clean(self):
        self.logger.info("Cleaning...")
        self.child.stdin.write("exit\n\n")
        self.child.stdin.write("exit\n\n")
        self.t.join()
        self.child.terminate()
    
    def init_thread(self):


        self.logger.info("Creating the child process...")
        self.child = Popen(self.shell_cmd.split(' '), stdin=PIPE, stdout=PIPE)

        self.logger.info("Waiting for the Child...")
        while not self.child:
           time.sleep(1)
        self.logger.info("Done")

        self.logger.info("Creating the listening thread...")
        self.t = threading.Thread(target=listening_process, args=(self.child, self.res_q))
        self.t.daemon = True
        self.t.start()
        self.logger.info("Done")
        time.sleep(2)

        self.logger.info("Trying to get the prompt...")
        self.child.stdin.write("\n\n\n")
        self.logger.debug(self.res_q.get(timeout=3*self.timeout))
        self.logger.info("Done")

        self.logger.info("Setting 'device'...")
        self.child.stdin.write("device\n\n")
        time.sleep(1)

        self.logger.info("Setting 'events off'...")
        self.child.stdin.write("events off\n\n")
        time.sleep(1)

        self.logger.debug(self.res_q.get(timeout=10))
        while True:
            try:
                self.logger.debug(self.res_q.get_nowait())
            except Empty:
                break
        self.logger.info("Done")

        
        
    def run_cmd(self):
        """
            Execute command and return the shell output (string)
            Fails fast on pipe errors to allow worker restart
        """
        output = ''
        try:
            self.child.stdin.write(self.tool_cmd+"\n\n")  # double \n to get the prompt again
        except IOError:
            self.logger.exception("Failed to write to pipe")
            sys.exit(1)

        try:
            self.res_q.get() # prompt
            self.res_q.get(timeout=self.timeout) # first value row
            while True:
                try:
                    line = self.res_q.get(timeout=self.timeout)
                    if self.stop_sequence in line:
                        break
                    if len(line.split(self.sep)) == self.column_num:
                        output += line
                except Empty:
                    self.logger.warning("Empty Queue in run_cmd!")
                    break

            return output
        except Exception:
            self.logger.exception("Error reading from queue")
            sys.exit(1)

        
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



def printTable(myDict, colList=None, sep='\uFFFA', noheader=False):
    if not colList: colList = list(myDict[0].keys() if myDict else [])
    myList = [colList] # 1st row = header
    for item in myDict: myList.append([str(item[col] if item[col] is not None else '') for col in colList])
    colSize = [max(map(len,col)) for col in zip(*myList)]
    formatStr = ' | '.join(["{{:<{}}}".format(i) for i in colSize])
    myList.insert(1, ['-' * i for i in colSize]) # Seperating line
    if noheader:
        del myList[:2]
    for item in myList: print formatStr.format(*item)

            
            
if __name__ == '__main__':

    
    schema = None
    shell_cmd = conf.get('BCM_SHELL', '/cm/local/apps/cmd/bin/cmsh')
    tool_cmd = conf.get('BCM_TOOL_CMD', 'latestmonitoringdata -u -d ";" --raw -c compute,gpu\n')
    stop_sequence = '->device]%'

    a  = CmdParser(shell_cmd, tool_cmd, schema, 8, stop_sequence, host=conf.get('BCM_HOST', 'localhost'), username=conf.get('BCM_USERNAME', None), password=None, pkey=None, sep=';', skipline=1)

    print "Start main loop"

    TS = 10 # seconds
    first = 1
    try:
        while True:
            time.sleep(TS - (time.time() % TS))
            res = a.read()
            print res[1]
    except KeyboardInterrupt:
        print "Exit ..."

    del a

    sys.exit(0)

    

