# Init steps
import os
import json
import re
import itertools

import numpy as np
import pandas as pd

from examon.examon import Client, ExamonQL


def expand_nodes(nodelist, sep=',', alt_sep=';', range_sep='-', zfill=2):
    """ Expand string ranges: 
        r242n[09-11] ->  'r242n09','r242n10','r242n11'
    """
    
    def get_every_str_between(s, before, after):
      return (i.split(after)[0] for i in s.split(before)[1:] if after in i)
    
    def str2range(instr):
       out = list()
       for r in instr.split(alt_sep):
           r = r.split(range_sep)
           if len(r) == 2:
               out += range(int(r[0]), int(r[1])+1)
           elif len(r) == 1:
               out.append(int(r[0]))
       return out

    if '[' in nodelist:
        # replace sep in [ ] with alt_sep
        ranges = list(get_every_str_between(nodelist, '[', ']'))
        text_new = nodelist
        for r in ranges:
            if sep in r:
                t = r.replace(sep, alt_sep)
                text_new = text_new.replace(r,t)

        nodes = text_new.split(sep)

        exp_nodes = []
        for n in nodes:
            if '[' in n:
                rng_str = list(get_every_str_between(n, '[', ']'))
                tmp = n
                for s in rng_str:
                    tmp = tmp.replace('['+s+']','{:0' + str(zfill) +'d}')
                rng_list = map(str2range, rng_str)
                for i in itertools.product(*rng_list):
                    exp_nodes.append(tmp.format(*i))
            else:
                exp_nodes.append(n)
        return exp_nodes
    else:
        return nodelist.split(sep)


def create_node_data_structure(cluster_data):
    """
    Create a data structure for each node in the cluster.

    Args:
        cluster_data (list): A list of dictionaries containing information about each node.

    Returns:
        dict: A dictionary where each key is a node and the value is a dictionary of node data.
    """
    node_data = {}
    for data in cluster_data:
        for node in expand_nodes(','.join(data['nodes'])):
            if node not in node_data:
                node_data[node] = {}
            for key, value in data.items():
                if key != 'nodes':
                    node_data[node][key] = value
    return node_data


def get_energy_v2(sq, job, node_data, unit='J', convert_timestamp=True):
    """
    Calculate the energy consumption for a job.
    
    Args:
        sq (obj): An ExamonQL instance.
        job (dict): A dictionary containing information about the job.
        node_data (dict): A dictionary containing power metrics and total power formula for each node.
        unit (str, optional): The unit of energy to calculate [Wh|J]. Defaults to 'J'.
    
    Returns:
        tuple: A tuple containing the DataFrame of power values, the DataFrame of energy values per node, the total energy consumption, and the quality score.
    """
    message = ''

    if convert_timestamp:        
        start_epoch_time_ms = sq.ex.get_utctmp(job['start_time'], 'UTC', format="%Y-%m-%dT%H:%M:%S.%fZ")
        end_epoch_time_ms = sq.ex.get_utctmp(job['end_time'], 'UTC', format="%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        start_epoch_time_ms = job['start_time']
        end_epoch_time_ms = job['end_time']

    nodes = expand_nodes(job['nodes'])

    df_list = []
    for n in nodes:
        if n not in node_data.keys():
            df_list.append((n, pd.DataFrame()))
            message += "The node '%s' does not support energy measurements; " % n
            continue
        p_metrics = node_data[n]['power_metrics']
        p_eval = node_data[n]['total_power']

        if len(p_metrics) == 1:
            df = (sq.SELECT('node')
                    .FROM(p_metrics[0])
                    .WHERE(node=n)
                    .TSTART(int(start_epoch_time_ms))
                    .TSTOP(int(end_epoch_time_ms))
                    .execute().df_table)
        else:
            data = (sq.SELECT('node')
                .FROM(m)
                .WHERE(node=n)
                .TSTART(int(start_epoch_time_ms))
                .TSTOP(int(end_epoch_time_ms))
                .execute().df_table for m in p_metrics)

            sq.ex.df_table = pd.concat(list(data)) 

            if sq.ex.df_table.empty:
                df = pd.DataFrame()
            else: 
                sq.ex.df_ts = pd.pivot_table(sq.ex.df_table, index=["timestamp","node"], columns=['name'], values="value", aggfunc="first")
                sq.ex.df_ts['total_power'] = sq.ex.df_ts.eval(p_eval)
                df = sq.ex.df_ts['total_power'].to_frame()
                df = df.stack().reset_index(name='value').rename(columns={'level_1':'node','level_2':'name'})
        df_list.append((n, df))
       
    df_dict = dict(df_list)

    df_list_ok = []
    for n, d in df_dict.items():
        if not d.empty:
            df_list_ok.append(d)
        else:
            message += "Missing data for node: %s; " % n

    missing_nodes = len(nodes) - len(df_list_ok)
    percentage_nodes_present = (len(df_list_ok) / len(nodes)) * 100
    quality_score = percentage_nodes_present
    message += "Missing nodes (%%): %f; " % (100 - quality_score)
    message += "Quality score (%%): %f; " % quality_score

    if unit == 'J':
        energy_conversion = 1
    elif unit == 'Wh':
        energy_conversion = 1.0 / 3600
    else:
        unit = 'J'
        energy_conversion = 1
        message += "Invalid unit. Supported units are 'J' (Joules) and 'Wh' (Watt-hours). The default 'J' unit is used. "

    if not df_list_ok:
        return (pd.DataFrame(), pd.Series(), 'NA', unit, 0, message)

    sq.ex.df_table = pd.concat(df_list_ok)
    df = sq.ex.to_series(flat_index=True, interp='time', dropna=True, columns=['node']).df_ts
    df_res_energy = df.apply(lambda y: np.trapz(y, x=df.index.astype(np.int64) / 10.0 ** 9))
    tot_energy = df_res_energy.sum()    
    df_res_energy = df_res_energy * energy_conversion
    tot_energy = tot_energy * energy_conversion

    return (df, df_res_energy, tot_energy, unit, quality_score, message)


    

if __name__ == '__main__':

    ########
    # Test #
    ########

    # Config
    EXAMON_USER = 'examon'
    EXAMON_PWD = ''
    EXAMON_DB_IP = ''
    EXAMON_DB_PORT = '3000'
    NODE_CONFIG_FILE = 'node_config.json'
    JOB_TABLE_NAME = ''

    # Connect to db 
    ex = Client(EXAMON_DB_IP, port=EXAMON_DB_PORT, user=EXAMON_USER, password=EXAMON_PWD, verbose=False, proxy=True)
    sq = ExamonQL(ex)

    # build the full index (one entry per node)
    with open(NODE_CONFIG_FILE, 'r') as file:
        cluster_data = json.load(file)

    node_data = create_node_data_structure(cluster_data)

    # Setup
    if JOB_TABLE_NAME not in sq.jc.JOB_TABLES:
        sq.jc.JOB_TABLES.append(JOB_TABLE_NAME)

    data = sq.SELECT('job_id, user_name, partition, start_time, end_time, nodes, job_state, tres_alloc_str, state_reason') \
        .FROM(JOB_TABLE_NAME) \
        .WHERE(job_id='5325') \
        .TSTART('01-01-1971 00:00:00') \
        .execute()

    df_j = pd.DataFrame(json.loads(data))

    # test data
    job_data = df_j.query('job_id == 5325').to_dict('records')

    # calculate the energy
    unit = 'Wh'
    df_job, df_res_energy, tot_energy, unit, perc, message = get_energy_v2(sq, job_data[0], node_data, unit=unit)

    # build the return data
    ret = {
        "version": "v0.1",
        "job_id": job_data[0]['job_id'],
        "total_energy_consumption": tot_energy,
        "unit": unit,
        "data quality (%)": perc,
        "message": message
    }

    # results
    print(json.dumps(ret, indent=4))