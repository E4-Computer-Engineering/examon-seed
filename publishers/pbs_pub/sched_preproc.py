#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import datetime
import pytz
import logging
import json
import time
from collections import OrderedDict
import pandas as pd
import pandas_explode
pandas_explode.patch()

import numpy as np

from pbsstat import PBSRd
from pbsrd import CmdParser

logger = logging.getLogger('examon')

down_states = ['down,offline',
                'state-unknown,offline',
                'offline',
                'down',
                'state-unknown,down'
            ]

previous_df = None

def extract_number(s):
    try:
        return int(s)
    except:
        match = re.match(r'(\d+)', s)
        return int(match.group(1)) if match else None

# Function to extract field fron nested dict
def extract(d,field):
    return d.get(field, 0)


def pbsnodes_preproc(pbsnodes_df):

    pbsnodes_df['Qlist'] = pbsnodes_df['resources_available'].apply(lambda x: extract(x,'Qlist'))
    pbsnodes_df['resources_assigned.ncpus'] = pbsnodes_df['resources_assigned'].apply(lambda x: extract(x,'ncpus'))
    pbsnodes_df['resources_available.ncpus'] = pbsnodes_df['resources_available'].apply(lambda x: extract(x,'ncpus'))
    pbsnodes_df['resources_available.mem'] = pbsnodes_df['resources_available'].apply(lambda x: extract_number(extract(x,'mem')))
    pbsnodes_df['resources_assigned.mem'] = pbsnodes_df['resources_assigned'].apply(lambda x: extract_number(extract(x,'mem')))
    pbsnodes_df['resources_assigned.ngpus'] = pbsnodes_df['resources_assigned'].apply(lambda x: extract(x,'ngpus'))
    pbsnodes_df['resources_available.ngpus'] = pbsnodes_df['resources_available'].apply(lambda x: extract(x,'ngpus'))
    pbsnodes_df['resources_available.vnode'] = pbsnodes_df['resources_available'].apply(lambda x: extract(x,'vnode'))

    return pbsnodes_df


def qstat_preproc(qstat_df):

    qstat_df['Resource_List.nodect'] = qstat_df['Resource_List'].apply(lambda x: extract(x,'nodect'))
    qstat_df['Resource_List.nodect'] = qstat_df['Resource_List'].apply(lambda x: extract(x,'nodect'))

    return qstat_df

def _remove_hidden_partitions(df, rm_part=['NA','system']):

    df = df.explode('partitions')   # pandas >= 0.25

    df['partition'] =  df['partitions'].apply(lambda x: str(x))

    # state_flags as strings
    df['state_flags'] = df.state_flags.apply(lambda x: ';'.join(sorted(x)) if (type(x) == list and len(x) > 0) else 'NA')

    # # drop hidden partitions
    df = df[~df.partition.isin(rm_part)]

    return df

def date_to_utc_epoch(date_str, date_format="%a %b %d %H:%M:%S %Y", timezone_str="Europe/Rome"):
    """
    Convert a date string to UTC epoch time.

    This function takes a date string, its format, and the timezone of the date string as input and converts it to UTC epoch time in milliseconds.

    Args:
        date_str (str): The date string to convert.
        date_format (str): The format of the date string (default is "%a %b %d %H:%M:%S %Y").
        timezone_str (str): The timezone of the date string (default is "Europe/Rome").

    Returns:
        int: The UTC epoch time in milliseconds.
    """
    if type(date_str) in [str, unicode]:
        # Parse the date string into a datetime object
        dt = datetime.datetime.strptime(date_str, date_format)

        # Localize the datetime object to the given timezone
        local_tz = pytz.timezone(timezone_str)
        local_dt = local_tz.localize(dt, is_dst=None)

        # Convert the localized datetime object to UTC
        utc_dt = local_dt.astimezone(pytz.utc)

        # Convert the UTC datetime object to epoch time (millis)
        epoch = int((utc_dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds())*1000

    else:
        # Log a warning for non-string date_str values
        logger.debug("Non-string date_str value encountered: %s (type: %s)", 
                       str(date_str), type(date_str).__name__)
        epoch = np.nan

    return epoch

def get_totals_cpu(df, groups=['Qlist'], prefix='s21.'):
    """
        total cpu

        :df = sinfo --json data

        "cpus_alloc"
        "cpus_idle"
        "cpus_down"
        "cpus_config"
        "cpu_eligible"
    """

    # idle_cpus
    df['idle_cpus'] = df['resources_available.ncpus'] - df['resources_assigned.ncpus']
    df['alloc_cpus'] = df['resources_assigned.ncpus']
    df['cpus'] = df['resources_available.ncpus']

    # aggregate
    totals_cpu = df.groupby(groups).agg({
        'alloc_cpus': ['sum'],  # alloc
        'idle_cpus': ['sum'],   # idle
        'cpus': ['sum'],        # config
    })

    # drop level
    totals_cpu.columns = totals_cpu.columns.get_level_values(0)

    # rename
    totals_cpu.rename(
            columns={
                'alloc_cpus': 'cpus_alloc',
                'idle_cpus': 'cpus_idle',
                'cpus': 'cpus_config',
            },
            inplace=True,
        )

    # cpu_down
    totals_cpu['cpus_down'] = df[df.state.isin(down_states)] \
    .groupby(groups).agg({
        'cpus': ['sum']
    })
    totals_cpu['cpus_down'] = totals_cpu['cpus_down'].fillna(0).astype('int')

    # cpu_eligible
    totals_cpu['cpus_eligible'] = (totals_cpu['cpus_config'] - totals_cpu['cpus_down']).fillna(0)

    # rename
    totals_cpu.rename(
            columns={
                'cpus_alloc'    : prefix + 'totals.cpus_alloc',
                'cpus_idle'     : prefix + 'totals.cpus_idle',
                'cpus_config'   : prefix + 'totals.cpus_config',
                'cpus_down'     : prefix + 'totals.cpus_down',
                'cpus_eligible' : prefix + 'totals.cpus_eligible',
            },
            inplace=True,
        )

    return totals_cpu


def get_totals_memory(df, groups=['Qlist'], prefix='s21.'):
    """
        memory
        "memory_alloc": 0,
        "memory_idle": 0,
        "memory_down": 0,
        "memory_config": 0,

    """

    df['real_memory'] = df['resources_available.mem']

    # aggregate
    totals_mem = df.groupby(groups).agg({
        'real_memory': ['sum'],     #config
    })

    totals_mem.columns = totals_mem.columns.get_level_values(0)

    # rename
    totals_mem.rename(
            columns={
                'real_memory': 'memory_config',

            },
            inplace=True,
        )

    # memory_down
    totals_mem['memory_down'] = df[df.state.isin(down_states)].groupby(groups).agg({
        'real_memory': ['sum']
    })

    # memory alloc
    totals_mem['memory_alloc'] = df[df.state.isin(['job-busy'])].groupby(groups).agg({
        'real_memory': ['sum']
    })

    totals_mem['memory_alloc']= totals_mem['memory_alloc'].fillna(0).astype('int')
    totals_mem['memory_down'] = totals_mem['memory_down'].fillna(0).astype('int')

    # memory_eligible
    totals_mem['memory_eligible'] = (totals_mem['memory_config'] - totals_mem['memory_down']).fillna(0)

    # memory_idle
    totals_mem['memory_idle'] = (totals_mem['memory_config'] - totals_mem['memory_alloc']).fillna(0)

    # rename
    totals_mem.rename(
            columns={
                'memory_alloc'      : prefix + 'totals.memory_alloc',
                'memory_config'     : prefix + 'totals.memory_config',
                'memory_down'       : prefix + 'totals.memory_down' ,
                'memory_eligible'   : prefix + 'totals.memory_eligible',
                'memory_idle'       : prefix + 'totals.memory_idle',
            },
            inplace=True,
        )

    return totals_mem


def get_totals_nodes(df, groups=['Qlist'], prefix='s21.'):
    """
        totals memory

        "total_nodes_mixed"
        "total_nodes_alloc"
        "total_nodes_idle"
        "total_nodes_down"
        "total_nodes_config"
        "total_nodes_eligible"
    """

    node_states = ['down,offline', 'job-busy', 'free', 'state-unknown,offline', 'offline', 'down', 'state-unknown,down']

    df['node_hostname'] = df['resources_available.vnode']

    # aggregate
    totals_nodes = df.groupby(groups).agg({
        'node_hostname': ['count'],  # config
    })

    totals_nodes.columns = totals_nodes.columns.get_level_values(0)

    # rename
    totals_nodes.rename(
            columns={
                'node_hostname': 'total_nodes_config',
            },
            inplace=True,
        )

    # total_nodes_down
    totals_nodes['total_nodes_down'] = df[(df.state.isin(down_states))].groupby(groups).agg({
                                                'node_hostname': ['count']
                                            })
    totals_nodes['total_nodes_down'] = totals_nodes['total_nodes_down'].fillna(0).astype('int')

    # total_nodes_eligible
    totals_nodes['total_nodes_eligible'] = (totals_nodes['total_nodes_config'] - totals_nodes['total_nodes_down']).fillna(0)

    for s in node_states:
        totals_nodes[s] = df[df.state.isin([s])] \
        .groupby(groups).agg({
            'node_hostname': ['count']
        })
        totals_nodes[s] = totals_nodes[s].fillna(0).astype('int')

    # rename
    totals_nodes.rename(
            columns={
                'job-busy' :                prefix + 'totals.total_nodes_alloc',
                'down,offline':             prefix + 'totals.total_nodes_down_offline',
                'free'     :                prefix + 'totals.total_nodes_free',
                'state-unknown,offline' :   prefix + 'totals.total_nodes_state-unknown_offline',
                'offline' :                 prefix + 'totals.total_nodes_offline',
                'down':                     prefix + 'totals.total_nodes_state-down',
                'state-unknown,down':       prefix + 'totals.total_nodes_state-unknown_down',
                'total_nodes_config'   :    prefix + 'totals.total_nodes_config',
                'total_nodes_down'     :    prefix + 'totals.total_nodes_down',
                'total_nodes_eligible' :    prefix + 'totals.total_nodes_eligible',
            },
            inplace=True,
        )

    return totals_nodes


def get_util(totals_cpu, totals_mem, totals_gpu, prefix='s21.'):

    df = pd.DataFrame()
    df[prefix + "cluster_cpu_util"] = (totals_cpu[prefix + "totals.cpus_alloc"] * 100 / (totals_cpu[prefix + "totals.cpus_eligible"])).fillna(0)
    df[prefix + "cluster_mem_util"] = (totals_mem[prefix + "totals.memory_alloc"] * 100 / (totals_mem[prefix + "totals.memory_eligible"])).fillna(0)
    df[prefix + "cluster_gpu_util"] = (totals_gpu[prefix + "totals.gpus_alloc"] * 100 / (totals_gpu[prefix + "totals.gpus_eligible"])).fillna(0)

    return df

def get_jobs(df, now, groups=['project','queue','job_state'], prefix='s21.'):
    global previous_df

    if previous_df is not None:
        # Find finished jobs from previous_df
        finished_jobs = previous_df[previous_df['job_state'] == 'F']['Job_Id'].tolist()
        
        # Update previous_df with current df
        previous_df = df.copy()

        # Remove finished jobs from the new df
        df = df[~df['Job_Id'].isin(finished_jobs)]

        # Count the number of finished jobs in df
        finished_jobs_count = df[df['job_state'] == 'F']['Job_Id'].count()
    
        # Log the count of finished jobs
        logger.info("Number of new finished jobs: {}".format(finished_jobs_count))

    elif previous_df is None:
        previous_df = df.copy()


    df['num_nodes'] = df['Resource_List.nodect']
    df['ctime'] = df['ctime'].apply(date_to_utc_epoch)
    df['stime'] = df['stime'].apply(date_to_utc_epoch).fillna(0)
    df['qtime'] = df['qtime'].apply(date_to_utc_epoch)


    # node_hour
    df['time_elapsed_hours'] = (( now - (df['ctime'] / 1000.0)) / 3600.0).astype('float')
    df['node_hour'] =  (df['time_elapsed_hours'] * df['num_nodes']).astype('float')

    # wait_time
    df['wait_time_hour'] = (( (df['stime'] / 1000.0) - (df['ctime'] / 1000.0)) / 3600.0).astype('float')
    df['wait_time_hour_q90'] = df['wait_time_hour']
    df['wait_time_hour_q95'] = df['wait_time_hour']

    # Check for negative wait times for running jobs and log if found
    negative_wait_running = df[(df['job_state'] == 'R') & (df['wait_time_hour'] < 0)]
    if not negative_wait_running.empty:
        for _, row in negative_wait_running.iterrows():
             logger.warning("Negative wait time for running job: Job ID: {}, stime: {}, ctime: {}".format(
                row['Job_Id'], row['stime'], row['ctime']))
        
        # Set negative wait times to zero for running jobs
        df.loc[(df['job_state'] == 'R') & (df['wait_time_hour'] < 0), 'wait_time_hour'] = 0



    totals_jobs = df.groupby(groups).agg({
        'Job_Id': ['count'],
        'num_nodes': ['sum'],
        'node_hour': ['sum'],
        'wait_time_hour' : ['mean'],
        'Job_Owner' : ['nunique'],
        'wait_time_hour_q95' : [lambda x: x.quantile(0.95)]
    })

    totals_jobs.columns = totals_jobs.columns.get_level_values(0)


    #totals_jobs

    # rename
    totals_jobs.rename(
            columns={
                'Job_Id':               prefix + 'jobs.tot_jobs',
                'num_nodes' :           prefix + 'jobs.tot_nodes',
                'node_hour' :           prefix + 'jobs.tot_node_hour',
                'wait_time_hour':       prefix + 'jobs.avg_waiting_hour',
                'wait_time_hour_q95':   prefix + 'jobs.p95_waiting_hour',
                'Job_Owner' :           prefix + 'jobs.tot_users'
            },
            inplace=True,
        )
    
    return totals_jobs


def get_totals_gpu(df, groups=['Qlist'], prefix='s21.'):
    # total gpu
    #
    # "gpus_alloc"
    # "cpus_idle"
    # "cpus_config"

    df['gpus_config'] = df['resources_available.ngpus']
    df['gpus_alloc'] = df['resources_assigned.ngpus']


    totals_gpu = df.groupby(groups).agg({
        'gpus_alloc': ['sum'],  # alloc
        'gpus_config': ['sum']   # config
    })

    totals_gpu.columns = totals_gpu.columns.get_level_values(0)


    # gpu_down
    totals_gpu['gpus_down'] = df[(df.state.isin(down_states))].groupby(groups).agg({
        'gpus_config': ['sum']
    })
    totals_gpu['gpus_down'] = totals_gpu['gpus_down'].fillna(0).astype('int')




    # gpu_eligible
    totals_gpu['gpus_eligible'] = (totals_gpu['gpus_config'] - totals_gpu['gpus_down']).fillna(0)


    # gpus_idle
    totals_gpu['gpus_idle'] = (totals_gpu['gpus_eligible'] - totals_gpu['gpus_alloc']).fillna(0)


    # rename
    totals_gpu.rename(
            columns={
                'gpus_alloc'  : prefix + 'totals.gpus_alloc',
                'gpus_idle'   : prefix + 'totals.gpus_idle',
                'gpus_config' : prefix + 'totals.gpus_config',
                'gpus_eligible' : prefix + 'totals.gpus_eligible',
                'gpus_down'     : prefix + 'totals.gpus_down'
            },
            inplace=True,
        )

    return totals_gpu



def get_gpus(x):
    num_gpus = 0
    if x:
        for gres in x.split(','):
            if 'gres/gpu' in gres:
                num_gpus = int(gres.split('=')[1])
    return num_gpus

def _res_to_pandas(res):

    return pd.DataFrame(res)


def _pandas_to_examon(df, timestamp, metric_list, tag_list, default_tags):

    def _sanitize_tags(tag):
        if tag:
            return str(tag).replace(' ','_').replace('+','_').replace('#','_').replace('/','_')
        else:
            return 'NA'

    res = df.reset_index().to_dict(orient='records')

    for entry in res:
        for m in metric_list:
            metric = {}
            metric['name'] = _sanitize_tags(m)
            metric['value'] = entry[m]
            if type(timestamp) == str:
                metric['timestamp'] = entry[timestamp]
            else:
                metric['timestamp'] = timestamp
            metric['tags'] = default_tags.copy()
            for t in tag_list:
                metric['tags'][t] = _sanitize_tags(entry[t])
            yield metric






if __name__ == '__main__':


    ########
    # Test #
    ########

    import ConfigParser
    conf = ConfigParser.RawConfigParser(allow_no_value=True)
    conf.read('pbs_pub.conf')

    default_tags = OrderedDict()
    default_tags['org'] = conf['MQTT_TOPIC'].split('/')[1]
    default_tags['cluster'] = conf['PBS_HOSTS'].split(':')[0]
    default_tags['node'] = conf['PBS_HOSTS'].split(':')[1]
    default_tags['plugin'] ='pbs_pub'
    default_tags['chnl'] = 'data'

    # Replace with your actual credentials
    username = conf['PBS_HOST_USER']
    password = conf['PBS_HOST_PASSW']
    pkey = conf['PBS_HOST_KEY']
    host = conf['PBS_HOSTS']

    cmd_qstat = """ssh -q -t -t {username}@{host} 'qselect -x -tm.gt.$(date -d "5 minutes ago" "+%Y%m%d%H%M") -s QRF | xargs qstat -xfF json -J'""".format(username=username, host=host)
    cmd_pbsnodes = """ssh -tt {username}@{host} 'ssh {host} pbsnodes -a -F json'""".format(username=username, host=host)

    qstat = CmdParser(cmd_qstat, schema, host=host, username=username, password=password, pkey=pkey)
    pbsnodes = CmdParser(cmd_pbsnodes, schema, host=host, username=username, password=password, pkey=pkey)

    schema = None
    verbose = True

    while 1:

        ret = pbsnodes.read()

        ps = PBSRd()
        ps.key = 'nodes'
        ps.data = json.loads(ret[1])

        t0 = time.time()
        timestamp = long(ret[0]*1000)
        df = _res_to_pandas(ps.get().values())

        df = pbsnodes_preproc(df)

        groupby = ['Qlist']
        metric_prefix = 's23.'

        df_mem = get_totals_memory(df, groups=groupby, prefix=metric_prefix)
        metric_list = list(df_mem.columns)
        tag_list = groupby
        for k in _pandas_to_examon(df_mem, timestamp, metric_list, tag_list, default_tags):
            if verbose:
                print(json.dumps(k, indent=4))
            pass

        df_cpu = get_totals_cpu(df, groups=groupby, prefix=metric_prefix)
        metric_list = list(df_cpu.columns)
        tag_list = groupby
        for k in _pandas_to_examon(df_cpu, timestamp, metric_list, tag_list, default_tags):
            if verbose:
                print(json.dumps(k, indent=4))
            pass

        df_gpu = get_totals_gpu(df, groups=groupby, prefix=metric_prefix)
        metric_list = list(df_gpu.columns)
        tag_list = groupby
        for k in _pandas_to_examon(df_gpu, timestamp, metric_list, tag_list, default_tags):
            if verbose:
                print(json.dumps(k, indent=4))
            pass

        df_node = get_totals_nodes(df, groups=groupby, prefix=metric_prefix)
        metric_list = list(df_node.columns)
        tag_list = groupby
        for k in _pandas_to_examon(df_node, timestamp, metric_list, tag_list, default_tags):
            if verbose:
                print(json.dumps(k, indent=4))
            pass

        df_util = get_util(df_cpu, df_mem, df_gpu, prefix=metric_prefix)
        metric_list = list(df_util.columns)
        tag_list = groupby
        for k in _pandas_to_examon(df_util, timestamp, metric_list, tag_list, default_tags):
            if verbose:
                print(json.dumps(k, indent=4))
            pass
        print (time.time() -t0) / df.shape[0]

        ret = qstat.read()

        ps.key = 'Jobs'
        ps.data = json.loads(ret[1])
        if verbose:
            print(json.dumps(ps.get(),indent=4))
        

        t0 = time.time()
        timestamp = long(ret[0]*1000)
        df = _res_to_pandas(ps.get().values())

        df = qstat_preproc(df)

        df_job = get_jobs(df,ret[0])
        metric_list = list(df_job.columns)
        tag_list = ['project','queue','job_state']
        for k in _pandas_to_examon(df_job, timestamp, metric_list, tag_list, default_tags):
            print(json.dumps(k, indent=4))
            pass
        print (time.time() - t0) / df_job.shape[0]