# -*- coding: utf-8 -*-
"""

    PBS Examon plugin
       
    @author: francesco.beneventi@e4company.com
    
    (c) 2024-2025 E4

"""

__version__ = 'v0.2.0' 

import json
import time
import pytz
import datetime
import logging
from multiprocessing.queues import SimpleQueue
import multiprocessing as mp
import threading
import os

from pbsrd import CmdParser
from examon.plugin.examonapp import ExamonApp
from examon.plugin.sensorreader import SensorReader

from cassandrawr import CassandraDb, KEYSPACE_PBS, job_states, load_schema, secondary_indexes, sanitize_table_data

from examon.examon import Client, ExamonQL
from job_energy import create_node_data_structure, get_energy_v2

from cache import Cache

from pbsstat import PBSRd, parse_concatenated_json, sanitize_json_payload
import sched_preproc as sp

from pbs_parser import PbsParser

logger = logging.getLogger('examon')


def timeout_handler():
    logger.error('[%s] Worker exceeded maximum execution time. Terminating process.', host)
    logger.debug('[%s] Process PID: %d', host, os.getpid())
    os._exit(1)   


"""
    Sinfo
"""


def read_data_sinfo(sr):
    tr0 = time.time()
    resultdata = sr.sensor.read()
    tr1 = time.time()
    tc0 = time.time()
    examon_data = list(to_examon_sinfo(sr, resultdata))
    tc1 = time.time()
    worker_id = str(sr.sensor.host)
    sr.logger.debug("Worker [%s] - Read time: %f sec, sensors: %d, time: %f sec, conversion_rate: %f sens/sec" % (worker_id,
                                                                                                                    (tr1-tr0),
                                                                                                                    len(examon_data),
                                                                                                                    (tc1-tc0),
                                                                                                                    len(examon_data)/(tc1-tc0),))
    return (worker_id, examon_data,)


def to_examon_sinfo(sr, resultdata):
    
    ret = resultdata
    default_tags = sr.get_tags()

    ps = PBSRd()
    ps.key = 'nodes'
    ps.data = sanitize_json_payload(ret[1])
     
    timestamp = long(ret[0]*1000)
    df = sp._res_to_pandas(ps.get().values())
    
    if df.empty:
        return

    df = sp.pbsnodes_preproc(df)

    groupby = ['Qlist']
    metric_prefix = 'v19.'
    
    df_mem = sp.get_totals_memory(df, groups=groupby, prefix=metric_prefix)
    metric_list = list(df_mem.columns)
    tag_list = groupby
    for k in sp._pandas_to_examon(df_mem, timestamp, metric_list, tag_list, default_tags):
        yield k  
    
    df_cpu = sp.get_totals_cpu(df, groups=groupby, prefix=metric_prefix)
    metric_list = list(df_cpu.columns)
    tag_list = groupby
    for k in sp._pandas_to_examon(df_cpu, timestamp, metric_list, tag_list, default_tags):
        yield k

    df_gpu = sp.get_totals_gpu(df, groups=groupby, prefix=metric_prefix)
    metric_list = list(df_gpu.columns)
    tag_list = groupby
    for k in sp._pandas_to_examon(df_gpu, timestamp, metric_list, tag_list, default_tags):
        yield k

    df_node = sp.get_totals_nodes(df, groups=groupby, prefix=metric_prefix)
    metric_list = list(df_node.columns)
    tag_list = groupby
    for k in sp._pandas_to_examon(df_node, timestamp, metric_list, tag_list, default_tags):
        yield k    
        
    df_util = sp.get_util(df_cpu, df_mem, df_gpu, prefix=metric_prefix)
    metric_list = list(df_util.columns)
    tag_list = groupby
    for k in sp._pandas_to_examon(df_util, timestamp, metric_list, tag_list, default_tags):
        yield k  

        
 
    
def worker_sinfo(conf, tags, cmd, schema, host, username, password, ts, pkey, skipline=0):
    """
        Worker process code
    """

    cp = CmdParser(cmd, schema, host=host, username=username, password=password, timeout=50, pkey=pkey, skipline=skipline)

    sr = SensorReader(conf, cp)
    # add read_data callback
    sr.read_data = read_data_sinfo 
    # set the default tags
    sr.add_tags(tags)
    # set the sampling rate
    sr.conf['TS'] = ts
    # run the worker loop
    sr.run()    

    
"""
    Squeue
""" 
def read_data_squeue(sr):
    tr0 = time.time()
    resultdata = sr.sensor.read()
    tr1 = time.time()
    tc0 = time.time()
    examon_data = list(to_examon_squeue_pbs(sr, resultdata))
    tc1 = time.time()
    worker_id = str(sr.sensor.host)
    sr.logger.debug("Worker [%s] - Read time: %f sec, sensors: %d, time: %f sec, conversion_rate: %f sens/sec" % (worker_id,
                                                                                                                    (tr1-tr0),
                                                                                                                    len(examon_data),
                                                                                                                    (tc1-tc0),
                                                                                                                    len(examon_data)/(tc1-tc0),))
    return (worker_id, examon_data,)


def to_examon_squeue(sr, resultdata):
    
    ret = resultdata
    default_tags = sr.get_tags()

    ps = PBSRd()
    ps.key = 'Jobs'
    ps.data = parse_concatenated_json(ret[1])  # support for concatenated json objects
    finished_jobs = ps.get().values()

    data_queue_index = sr.conf['data_queue']
    logger.info("Inserting data in queue: %d" % data_queue_index)
    data_queues[data_queue_index].put((ret[0], finished_jobs,))
    timestamp = long(ret[0]*1000)

    df = sp._res_to_pandas(ps.get().values())
    if df.empty:
        logger.warning("Empty data frame")
        return
    df = sp.qstat_preproc(df)
    df_job = sp.get_jobs(df, ret[0], prefix='v19.')
    metric_list = list(df_job.columns)
    tag_list = ['project', 'queue', 'job_state']
    for k in sp._pandas_to_examon(df_job, timestamp, metric_list, tag_list, default_tags):
        yield k       


# New efficient implementation
def to_examon_squeue_pbs(sr, resultdata):
    
    ret = resultdata
    default_tags = sr.get_tags()

    finished_jobs = ret[1]
    
    data_queue_index = sr.conf['data_queue']
    logger.info("Inserting data in queue: %d" % data_queue_index)
    data_queues[data_queue_index].put((ret[0], finished_jobs,))

    timestamp = long(ret[0]*1000)
    df = sp._res_to_pandas(finished_jobs)

    if df.empty:
       logger.warning("Empty data frame")
       return
    
    df = sp.qstat_preproc(df)

    df_job = sp.get_jobs(df, ret[0], prefix='v19.')
    metric_list = list(df_job.columns)
    tag_list = ['project', 'queue', 'job_state']
    for k in sp._pandas_to_examon(df_job, timestamp, metric_list, tag_list, default_tags):
        yield k


def worker_squeue(conf, tags, cmd, schema, host, username, password, ts, pkey, skipline, data_queues_index):
    """
        Worker process code
    """
    cp = PbsParser(cmd, schema, host=host, username=username, password=password, 
                  timeout=int(conf.get('PBS_PARSER_TIMEOUT', 180)), pkey=pkey, skipline=skipline)

    cp.cmd_timeout = int(conf.get('PBS_QSELECT_CMD_TIMEOUT', 60))
    cp.finished_jobs_history = int(conf.get('TS', 60)) + 10
    # controller
    cp.min_timeout = cp.cmd_timeout
    cp.current_timeout = cp.cmd_timeout
    cp.controller_target_time = int(conf.get('PBS_QSTAT_CMD_TARGET_TIME', 15.0))
    cp.controller_min_batch = int(conf.get('PBS_QSTAT_CMD_MIN_BATCH', 25))
    cp.controller_max_batch = int(conf.get('PBS_QSTAT_CMD_MAX_BATCH', 500))

    sr = SensorReader(conf, cp)
    # add read_data callback
    sr.read_data = read_data_squeue
    # set the default tags
    sr.add_tags(tags)
    # set the sampling rate
    sr.conf['TS'] = ts
    sr.conf['data_queue'] = data_queues_index
    # run the worker loop
    sr.run()       
 

def tags_to_topic(metric_name, tags):
    """
        Return MQTT topic from the examon tags
        
        tags := OrderedDict
    """
    topic = '/'.join([(val).encode('utf-8') for pair in tags.items() for val in pair])
    topic += '/' + (metric_name).encode('utf-8')
    topic = topic.replace(' ', '_').replace('+', '_').replace('#', '_')
    return (topic).decode('utf-8')


def get_utctmp(timestring, time_zone, format="%d-%m-%Y %H:%M:%S"):
    """Time string to utc epoch (ms)

    Parameters
    ----------
    timestring : str
        date in the  "%d-%m-%Y %H:%M:%S" format.
    time_zone : str
        timezone of the ``timestring`` date.
    format : str
        override input date format
    """
    tz = pytz.timezone(time_zone)
    dt = datetime.datetime.strptime(timestring, format)
    dt_epoch = (tz.normalize(tz.localize(dt)).astimezone(pytz.utc) - datetime.datetime(1970,1,1).replace(tzinfo=pytz.UTC)).total_seconds()

    return long(dt_epoch*1000)



"""
    Jobs logging
"""
def worker_job_table(conf, host, ks_name, ks_def, table_name, table_def, data_queues_index):        
    """
        Worker process code
    """

    def validate_and_clean_job_data(job, table_def):
        valid_fields = set(table_def.keys())
        job_fields = set(job.keys())
        
        new_fields = job_fields - valid_fields
        
        if new_fields:
            logger.warning("[%s] New elements found in job payload: %s" % (host, ', '.join(["%s: %s" % (field, job[field]) for field in new_fields])))
            for field in new_fields:
                del job[field]
        
        return job
    
    def table_def_to_dict(table_def):
        schema_dict = {}
        temp_list = table_def.split('(')
        
        for col in temp_list[1].split(','):
            if 'PRIMARY' in col:
                continue
            parts = col.strip().split(' ')
            if len(parts) >= 2:
                column_name = parts[0].strip(',')
                column_type = ' '.join(parts[1:]).strip(',')
                schema_dict[column_name] = column_type
        return schema_dict

    # create cassandra interface
    cass_host, cass_port = conf['CASS_HOST'].split(':')
    cass = CassandraDb((cass_host,), user=conf['CASS_USER'], password=conf['CASS_PASSW'])
    cass.session.default_timeout = int(conf['CASS_TIMEOUT'])

    """
        Set a local cache for max_size items and timeout seconds of validity.
        This helps to check if a job is alredy stored into the db, preserving db overhead.
        Currently we check finished jobs in the last TS seconds.
        Here we keep in cache data until CACHE_TIMEOUT seconds old
    """
    cache = Cache(max_size=int(conf['CACHE_MAX_SIZE']), timeout=int(conf['CACHE_TIMEOUT']))
    
    # init: create and set keyspaces and tables
    cass.db_init(ks_name, ks_def, table_name, table_def)
    
    # create secondary indexes on job_info
    for k in secondary_indexes:
        logger.debug('Creating index on "{0}"."{1}" '.format(ks_name, table_name))
        cass.session.execute('CREATE INDEX IF NOT EXISTS ON "{0}"."{1}" ({2})'.format(ks_name, table_name, k))

    # ceate prepared statememt
    cass.insert_stmt = cass.session.prepare('INSERT INTO "{0}" JSON ?'.format(table_name))
    
    # PBS timestamps timezone
    pbs_timezone = conf['PBS_TIMEZONE']
    
    # obtain the schema dict from CQL command
    schema_dict = table_def_to_dict(table_def)
    logger.debug('Schema dictionary: %s', json.dumps(schema_dict, indent=4))

    # samapling time
    TS = float(conf['TS'])

    # Set maximum execution time (e.g., 5 minutes)
    MAX_EXECUTION_TIME = 300  # seconds
    
    while True:
        # Start timeout timer for this iteration
        timer = threading.Timer(MAX_EXECUTION_TIME, timeout_handler)
        
        try:
            resultdata = data_queues[data_queues_index].get()
            
            logger.debug("Start timeout timer")
            timer.start()

            timestamp = resultdata[0]
            jobs = resultdata[1]
            jobs = [job for job in jobs if job['job_state'] in job_states]
            logger.info('[%s] Found %d terminated jobs', host, len(jobs))
            
            njobs = 0
            for job in jobs:
                try:
                    if not cache.get(job['Job_Id']):
                        job = sanitize_table_data(job, timezone_str=pbs_timezone, pbs_version=conf['PBS_VERSION'])
                        job = validate_and_clean_job_data(job, schema_dict)  
                        cass.put_metrics(job)
                        cache.set(job['Job_Id'], job)
                        njobs += 1
                    else:
                        continue
                except Exception:
                    logger.exception('[%s] Uncaught exception processing job!', host)
                    logger.error('[%s] Failed job data: %s', host, json.dumps(job, indent=4))
                    continue

            logger.info('[%s] Inserted %d new jobs', host, njobs)
            logger.debug('[%s] Cache size: %d', host, len(cache._store))
            
        except Exception:
            logger.exception('[%s] Uncaught exception in main loop!', host)
        finally:
            # Cancel the timer if we complete normally
            timer.cancel()
            logger.debug("Cancel timeout timer")
    # Clean up resources
    cass.close()


def worker_job_energy(conf, host, ks_name, ks_def, table_name, table_def, energy_queues_index):
    """
        Worker process code
    """
    # create cassandra interface
    cass_host, cass_port = conf['CASS_HOST'].split(':')
    cass = CassandraDb((cass_host,), user=conf['CASS_USER'], password=conf['CASS_PASSW'])
    cass.session.default_timeout = int(conf['CASS_TIMEOUT'])
    # ceate prepared statememt

    cass.session.set_keyspace(ks_name)

    update_statement = "UPDATE {0} SET energy = ? WHERE job_id = ? AND start_time = ? AND end_time = ?".format(table_name)
    prepared_update = cass.session.prepare(update_statement)

    # Connect to db
    ex = Client(conf['EXAMON_DB_IP'], port=conf['EXAMON_DB_PORT'], user=conf['EXAMON_USER'], password=conf['EXAMON_PWD'], verbose=False, proxy=True)
    sq = ExamonQL(ex)

    # build the full index (one entry per node)
    with open(conf['NODE_CONFIG_FILE'], 'r') as file:
        cluster_data = json.load(file)

    node_data = create_node_data_structure(cluster_data)

    # Define the parameter values
    energy_value = {
        "job_id": "NA", 
        "data_quality_(%)": 0, 
        "version": "v0.1", 
        "total_energy_consumption": "NA", 
        "message": "", 
        "unit": conf['JOB_ENERGY_UNIT']
    }

    while 1:
        
        job_id_value = None
        start_time_value = None
        end_time_value = None
        
        job = data_queues[energy_queues_index].get()
        
        try:
            # calculate the energy
            job_['job_id'] = job['Job_Id']
            job_['start_time'] = job['stime']
            job_['end_time'] = job['mtime']

            logger.info('[%s] Calculating energy for job: %s' % (host, str(job['job_id']),))

            df_job, df_res_energy, tot_energy, unit, perc, message = get_energy_v2(sq, job, node_data, unit=conf['JOB_ENERGY_UNIT'], convert_timestamp=False)
            logger.info('[%s] Job Energy worker return message: %s' % (host, message))
            # build the payload
            energy_value["job_id"] = job['job_id']
            energy_value["total_energy_consumption"] = tot_energy
            energy_value["message"] = message
            energy_value["data_quality_(%)"] = perc

            job_id_value = job['job_id']
            start_time_value = job['start_time']
            end_time_value = job['end_time']
            # update
            cass.session.execute(prepared_update, (json.dumps(energy_value), job_id_value, start_time_value, end_time_value))
        except Exception:
            logger.exception('[%s] Uncaught exception in worker_job_energy loop!', host)
            continue
    cass.close()


if __name__ == '__main__':

    # start creating an Examon app instance with the basic options
    app = ExamonApp()
    # optionally, update the opt parser adding the extra parameters needed by this plugin
    app.cfg.parser.add_argument("--pbs-hosts", dest='PBS_HOSTS', help="PBS hosts list, comma separated. Format: <cluster_name>:<login_hostname|IP address>")
    app.cfg.parser.add_argument("--pbs-timezone", dest='PBS_TIMEZONE', help="Timezone of PBS timestamps")
    app.cfg.parser.add_argument("--pbs-qselect-cmd", dest='PBS_QSELECT_CMD', help="qselect command on the PBS host to select running and finished jobs")
    app.cfg.parser.add_argument("--host-username", dest='PBS_HOST_USER', help="Username to login to the PBS host")
    app.cfg.parser.add_argument("--host-password", dest='PBS_HOST_PASSW', default=None, help="Password to login to the PBS host")
    app.cfg.parser.add_argument("--host-key", dest='PBS_HOST_KEY', default=None, help="Path to the private RSA key to be used for passwordless authentication on the PBS host")
    app.cfg.parser.add_argument("--cassandra-hosts", dest='CASS_HOST', help="Cassandra host. Format: <hostname|IP address>:<port>")
    app.cfg.parser.add_argument("--cassandra-username", dest='CASS_USER', help="Username to login to the Cassandra host")
    app.cfg.parser.add_argument("--cassandra-password", dest='CASS_PASSW', help="Password to login to the Cassandra host")    
    app.cfg.parser.add_argument('--version', action='version', version=__version__)
    # finally, parse again the opt parameters
    app.parse_opt()
    # for checking
    print "Config:"
    print json.dumps(app.conf, indent=4)
    # get the opts for the workers
    slist = app.conf['PBS_HOSTS'].split(',')
    username = app.conf['PBS_HOST_USER']
    password = app.conf['PBS_HOST_PASSW']
    key = app.conf['PBS_HOST_KEY']
    TS = int(app.conf['TS'])
    tpc_l = app.conf['MQTT_TOPIC'].split('/')
    pbs_version = app.conf['PBS_VERSION']
    ks_name = app.conf['CASS_KEYSPACE_NAME']

    #app.conf['LOG_LEVEL'] = 'DEBUG'
    app.conf['TIMEOUT'] = 1800 # seconds to help build the index when the number of jobs is high
    
    # Load the schema
    TABLE_JOB_INFO = load_schema("./pbs_schema_" + pbs_version)

    # Config for the cassandra db
    ks_def = KEYSPACE_PBS
    table_def = TABLE_JOB_INFO

    # create data queues
    data_queues = []
    data_queues.append(SimpleQueue())  # data
    data_queues.append(SimpleQueue())  # energy
    data_queues_index = 0
    energy_queues_index = 1

    qselect_cmd = app.conf['PBS_QSELECT_CMD']
    pbsnodes_cmd = """bash -c 'pbsnodes -a -F json'"""
    
    # get topics data
    tpc_d = {tpc_l[i]: tpc_l[i+1] for i in range(0, len(tpc_l)-1, 2)}
        
    # set default metrics tags
    tags = app.examon_tags()
    tags['org'] = tpc_d['org']
    tags['plugin'] = 'pbs_pub'
    tags['chnl'] = 'data'
    
    # define the schema for the command parser 
    schema = None
    
    """
        Start the workers
    """

    for s_server in slist:
        cluster, login = s_server.split(':')
        # set cluster specific tags
        tags['cluster'] = cluster
        tags['node'] = login

        # pbsnodes workers
        app.add_worker(worker_sinfo, app.conf, tags, pbsnodes_cmd, schema, login, username, password, TS, key, 0)
        # qstat workers
        app.add_worker(worker_squeue, app.conf, tags, qselect_cmd, schema, login, username, password, TS, key, 0, data_queues_index)
        # job_info workers
        table_name = 'job_info_' + cluster
        app.add_worker(worker_job_table, app.conf, login, ks_name, ks_def, table_name, table_def, data_queues_index)
        # job_energy workers
        app.add_worker(worker_job_energy, app.conf, login, ks_name, ks_def, table_name, table_def, energy_queues_index)

    # delayed start
    time.sleep(TS+5) 

    # run!
    app.run()    