"""

    Bright Cluster Manager Examon plugin
        
    @author: francesco.beneventi@e4company.com
    
    (c) 2024 E4 Computer Engineering S.p.A.

"""

__version__ = 'v0.1.0'

import re
import json
import time

import pandas as pd

from examon.plugin.examonapp import ExamonApp
from examon.plugin.sensorreader import SensorReader
from bcmrd import process_bcm_data
from bcmrd_stream import CmdParser
from cache import Cache

def read_data(sr):
    tr0 = time.time()
    resultdata = sr.sensor['sr'].read()
    tc0 = tr1 = time.time()
    examon_data = list(to_examon(sr, resultdata))
    tc1 = time.time()
    worker_id = str(sr.sensor['sr'].host)
    sr.logger.debug("Worker [%s] - Read time: %f sec, sensors#: %d, conv_time: %f sec, conv_rate: %f sens/sec" % \
                    (worker_id,
                    (tr1-tr0),
                    len(examon_data),
                    (tc1-tc0),
                    len(examon_data)/(tc1-tc0),)
                    )
    return (worker_id, examon_data,)

               
def to_examon(sr, resultdata):
    timestamp = 'timestamp'
    tag_list = ['node','parameter','type','unit']
    default_tags = sr.get_tags()

    def _sanitize_tags(tag):
        if tag:
            return str(tag).replace(' ','_').replace('+','_').replace('#','_').replace('/','|')
        else:
            return 'NA'
    
    df = process_bcm_data(resultdata, './metrics.csv')
    df = df.rename(columns={'Entity': 'node', 'Parameter': 'parameter','Type': 'type','Unit': 'unit'})

    res = df.reset_index().to_dict(orient='records')

    miss_cnt = 0
    for m in res:
        try:
            if "----" in m['Measurable']:
                continue
            metric = {}
            metric['name'] = _sanitize_tags(m['Measurable'])
            metric['value'] = m['Value']
            if type(timestamp) == str:
                metric['timestamp'] = m[timestamp]
            else:
                metric['timestamp'] = timestamp
            metric['tags'] = default_tags.copy()
            for t in tag_list:
                metric['tags'][t] = _sanitize_tags(m[t])           
            t = metric['timestamp'] 
            if (t >= sr.sensor['cache'].get(get_hash(metric),0) + 5000): # miss
                sr.sensor['cache'].set(get_hash(metric), t)
                miss_cnt += 1               
                yield metric
            else: # hit
                continue  
        except Exception as e:
            sr.logger.exception('Error in metric: %s' % str(metric))
            continue
    sr.logger.debug('[%s] Cache Misses: %d - Cache size: %d' % (sr.sensor['sr'].host, miss_cnt, len(sr.sensor['cache']._store)))
                      
def get_hash(d):
    h = [x for x in d['tags'].items()]
    h.append(('name',d['name']))
    return hash(frozenset(h))
            
def worker(conf, tags, cmd_bcm, schema=None):
    """
        Worker process code 
    """
    schema = None
    shell_cmd = cmd_bcm
    tool_cmd = conf.get('BCM_TOOL_CMD', 'latestmonitoringdata -u -d ";" --raw -c compute,gpu')
    stop_sequence =  '->device]%'

    cp  = CmdParser(shell_cmd, tool_cmd, schema, 8, stop_sequence, host='localhost', username=None, password=None, pkey=None, sep=';', skipline=1)
 
    cache_max_size = conf.get('CACHE_MAX_SIZE', 1000000)
    cache_timeout = conf.get('CACHE_TIMEOUT', 3600*24*7)
    cache = Cache(max_size=cache_max_size, timeout=cache_timeout)

    sr = SensorReader(conf, {'sr': cp, 'cache': cache})
    # add read_data callback
    sr.read_data = read_data
    # set the default tags
    sr.add_tags(tags)
    # run the worker loop
    sr.run()

   
if __name__ == '__main__':

    # start creating an Examon app instance with the basic options
    app = ExamonApp()
    # optionally, update the opt parser adding the extra parameters needed by this plugin
    app.cfg.parser.add_argument('--version', action='version', version=__version__)
    # finally, parse again the opt parameters
    app.parse_opt()
    # for checking
    print "Config:"
    print json.dumps(app.conf, indent=4)
    # get the opts for the workers

    tpc_l = app.conf['MQTT_TOPIC'].split('/')
    tpc_d = {tpc_l[i] : tpc_l[i+1] for i in range(0,len(tpc_l)-1, 2)}
    
    # set default metrics tags
    tags = app.examon_tags()
    tags['org'] = tpc_d['org']
    tags['cluster'] = tpc_d['cluster']
    tags['node'] = ''
    tags['plugin'] ='bcm_pub'
    tags['chnl'] ='data'


    cmd_bcm = app.conf.get('BCM_SHELL', '/cm/local/apps/cmd/bin/cmsh')
    if app.conf.get('BCM_HOST') and app.conf.get('BCM_USERNAME'):
        cmd_bcm = "ssh -q -tt {0}@{1} '{2}'".format(app.conf['BCM_USERNAME'], app.conf['BCM_HOST'], cmd_bcm)

    app.add_worker(worker, app.conf, tags, cmd_bcm)

    # run!
    app.run()    
