"""

    Cassandra interface
       
    @author:francesco.beneventi@unibo.it
    
    (c) 2019 University of Bologna, [Department of Electrical, Electronic and Information Engineering, DEI]

"""

import datetime
import pytz
import json


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement


CASS_USER='cassandra'
CASS_PASSW='cassandra'
CASS_HOST='localhost'

KEYSPACE_PBS="CREATE KEYSPACE IF NOT EXISTS %s " + \
                    "WITH replication = {" + \
                    "'class' : 'SimpleStrategy', " + \
                    "'replication_factor' : 2}"

                    
# Test PBS data - v19.2.8
json_data = json.loads("""{
    "Job_Name":"example_job_48h.pbs",
    "Job_Owner":"user123@login01.cluster.example.com",
    "resources_used":{
        "cpupercent":1966,
        "cput":"420:16:02",
        "mem":"1622428kb",
        "ncpus":48,
        "vmem":"636708164kb",
        "walltime":"24:05:32"
    },
    "job_state":"F",
    "Job_Id":"4732778",
    "queue":"compute",
    "server":"login02",
    "Checkpoint":"u",
    "ctime":"Tue Mar 12 10:27:52 2024",
    "Error_Path":"login01.cluster.example.com:\/home\/user123\/workdir\/example_job_48h.pbs.e4730912",
    "exec_host":"node1299\/0*48",
    "exec_vnode":"(node1299:ncpus=48:tasks=0)",
    "Hold_Types":"n",
    "Join_Path":"oe",
    "Keep_Files":"oe",
    "Mail_Points":"ae",
    "Mail_Users":"user@example.com",
    "mtime":"Wed Mar 13 13:10:13 2024",
    "Output_Path":"login01.cluster.example.com:\/home\/user123\/workdir\/example_job_48h.pbs.o4730912",
    "Priority":0,
    "qtime":"Tue Mar 12 10:27:52 2024",
    "Rerunable":"False",
    "Resource_List":{
        "mem":"96gb",
        "mpiprocs":48,
        "ncpus":48,
        "nodect":1,
        "place":"free:shared",
        "select":"1:mem=96gb:mpiprocs=48:ncpus=48",
        "tasks":0,
        "walltime":"48:00:00"
    },
    "stime":"Tue Mar 12 13:04:41 2024",
    "session_id":30612,
    "jobdir":"\/home\/user123",
    "substate":92,
    "Variable_List":{
        "PBS_O_HOME":"\/home\/user123",
        "PBS_O_LANG":"en_US.UTF-8",
        "PBS_O_LOGNAME":"user123",
        "PBS_O_PATH":".:\/home\/user123\/bin:\/home\/user123\/scripts:\/usr\/local\/bin:\/usr\/bin:\/usr\/local\/sbin:\/usr\/sbin:\/opt\/pbs\/bin:\/home\/user123\/.local\/bin:\/home\/user123\/bin",
        "PBS_O_MAIL":"\/var\/spool\/mail\/user123",
        "PBS_O_SHELL":"\/bin\/bash",
        "PBS_O_WORKDIR":"\/home\/user123\/workdir",
        "PBS_O_SYSTEM":"Linux",
        "PBS_O_QUEUE":"compute",
        "PBS_O_HOST":"login01.cluster.example.com"
    },
    "comment":"Job run at Tue Mar 12 at 13:04 on (node1299:ncpus=48:tasks=0) and finished",
    "etime":"Tue Mar 12 10:27:52 2024",
    "run_count":1,
    "Exit_status":0,
    "Submit_arguments":"-k oe -M user@example.com -q compute \/home\/user123\/scripts\/example_job_48h.pbs",
    "history_timestamp":1710331813,
    "project":"_pbs_project_default",
    "Account_Name":null,
    "umask":null,
    "Stageout_status":null,
    "executable":null,
    "interactive":null,
    "forward_x11_port":null,
    "sandbox":null,
    "depend":null,
    "argument_list":null
}""")


# Timestamps to convert into epoch format 
timestamp_keys = [
    'ctime',
    'etime',
    'mtime',
    'qtime',
    'stime',
    'obittime'
    ]

# json keys to serialize
json_keys = [
    'resources_used',
    'Resource_List',
    'Variable_List'
]

# Jobs eligible to be stored by job state
job_states = [  
    'F'
    ]

millis_keys = [
    'history_timestamp'
]

secondary_indexes = [
    'Exit_status',
    'queue',
    'project',
    'Job_Owner'
]


def convert_timestamps_to_mills(dictionary, keys):
    """
    Convert the values of specified keys in a dictionary to strings.

    Args:
        dictionary (dict): The dictionary to update.
        keys (list): The list of keys to convert the values for.

    Returns:
        dict: The updated dictionary with converted values.
    """
    for key in keys:
        if key in dictionary:
            if dictionary[key] != 0:
                dictionary[key] = (dictionary[key]*1000)
            else:
                dictionary[key] = None
    return dictionary


def convert_values_to_json(dictionary, keys):
    """
    Convert the values of specified keys in a dictionary to serialized JSON text.

    Args:
        dictionary (dict): The dictionary to update.
        keys (list): The list of keys to convert the values for.

    Returns:
        dict: The updated dictionary with converted values.
    """
    for key in keys:
        if key in dictionary:
            dictionary[key] = json.dumps(dictionary[key])
    return dictionary


def convert_dates_to_epoch(dictionary, keys, timezone_str):
    """
    Convert the values of specified keys in a dictionary to serialized JSON text.

    Args:
        dictionary (dict): The dictionary to update.
        keys (list): The list of keys to convert the values for.
        timezone_str (str): The timezone string to use for conversion.

    Returns:
        dict: The updated dictionary with converted values.
    """
    for key in keys:
        if key in dictionary:
            dictionary[key] = date_to_utc_epoch(dictionary[key], timezone_str=timezone_str)
    return dictionary


def load_schema(file_path):
    """
    Load the slurm schema from a file and return it as a concatenated string.

    Args:
        file_path (str): The path to the file containing the slurm schema.

    Returns:
        str: The concatenated string of the slurm schema.
    """
    with open(file_path, "r") as file:
        lines = file.readlines()
        concatenated_string = " ".join([line.strip() for line in lines])
    return concatenated_string

def check_stime(json_data):
    """
    Check if the 'stime' key is in the json_data dictionary and set it to the value of 'mtime' if not. Returns the updated json_data dictionary.
    """
    if 'stime' not in json_data:
        json_data['stime'] = json_data['mtime']
    return json_data

# given a dictionary of arbitrary values, add it to the json_data dictionary
def add_to_json(json_data, items={}):
    """
    Given a dictionary of arbitrary values, add it to the json_data dictionary.

    Args:
        json_data (dict): The dictionary to add the items to.
        items (dict): The dictionary of items to add.

    Returns:
        dict: The updated json_data dictionary.
    """
    json_data.update(items)
    return json_data

def ensure_correct_type_for_key(data, key, expected_type, default_value=None):
    """
    Ensures that the value for a given key in a dictionary matches the expected type.
    Converts or replaces values that do not match the expected type.

    Args:
        data (dict): The dictionary containing the key to check.
        key (str): The key for which to ensure the value type.
        expected_type (type): The expected type for the value.
        default_value (Any, optional): The default value to use if conversion is not possible. Defaults to None.

    Returns:
        dict: The updated dictionary with the value for the given key ensured to be of the expected type.
    """
    value = data.get(key, None)
    if isinstance(value, expected_type):
        return data  # Value is already of the expected type

    if expected_type == int:
        if isinstance(value, str):
            if value.lower() == 'true':
                data[key] = 1
            elif value.lower() == 'false':
                data[key] = 0
            else:
                try:
                    data[key] = int(value)
                except ValueError:
                    data[key] = default_value
        else:
            data[key] = default_value
    elif expected_type == str and not isinstance(value, str):
        data[key] = str(value) if value is not None else default_value
    # Add more type checks as needed

    return data


def sanitize_table_data(json_data, timezone_str="Europe/Rome", pbs_version='19.2.8'):
    """
    Sanitizes the table data by converting values to JSON format, timestamps to milliseconds, and dates to epoch.

    Parameters:
        json_data (dict): The table data to be sanitized.

    Returns:
        dict: The sanitized table data.
    """
    json_data = convert_values_to_json(json_data, json_keys)
    json_data = convert_timestamps_to_mills(json_data, millis_keys)
    json_data = convert_dates_to_epoch(json_data, timestamp_keys, timezone_str)
    json_data = check_stime(json_data) # check if the jsond_data has stime defined aotherwise replace the value with the mtime
    json_data = add_to_json(json_data, {'pbs_version':pbs_version})
    json_data = ensure_correct_type_for_key(json_data, 'forward_x11_port', int, default_value=None)
    
    return json_data

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
    
    # Parse the date string into a datetime object
    dt = datetime.datetime.strptime(date_str, date_format)
    
    # Localize the datetime object to the given timezone
    local_tz = pytz.timezone(timezone_str)
    local_dt = local_tz.localize(dt, is_dst=None)
    
    # Convert the localized datetime object to UTC
    utc_dt = local_dt.astimezone(pytz.utc)
    
    # Convert the UTC datetime object to epoch time (millis)
    epoch = int((utc_dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds())*1000
    
    return epoch



class CassandraDb(object):
    """
        Cassandra Db interface
    """
    def __init__(self, servers, port=9042, user=None, password=None):
        self.servers = servers
        self.port = port
        self.user = user
        self.password = password
        self.auth_provider = PlainTextAuthProvider(username=self.user, password=self.password)
        self.cluster = Cluster(contact_points=self.servers, auth_provider=self.auth_provider)
        self.session = self.cluster.connect()
        #self.db_init()
        # prepared insert statement
        self.insert_stmt = None
        
    def __del__(self):
        self.cluster.shutdown()
        # print 'CassandraDb closed properly'
        
    def close(self):
        self.cluster.shutdown()
        
    def db_init(self, ks_name, ks_def, table_name, table_def):
        # preserve caps
        ksname = '"' + ks_name + '"'
        tablename = '"' + table_name + '"'

        ks = ks_def % (ksname)
        table = table_def % (tablename)
        
        # create keyspace if not exist
        self.session.execute(ks)
        
        # set the keyspace
        self.session.set_keyspace(ks_name)
    
        # create tables
        # for table in tables_def:
            # self.session.execute(table)
        self.session.execute(table)
        
    
    def put_metrics(self, json_data, comp=False):
        if self.insert_stmt:
            self.session.execute(self.insert_stmt, [json.dumps(json_data)])
        else:
            raise Exception("'insert_stmt' must be implemented!")
    



if __name__ == '__main__':


    if 0:
        # auth
        auth_provider = PlainTextAuthProvider(username=CASS_USER, password=CASS_PASSW)

        # connect to the cluster
        #cluster = Cluster(contact_points=(CASS_HOST,), auth_provider=auth_provider)

        # using context
        with Cluster(contact_points=(CASS_HOST,), auth_provider=auth_provider) as cluster:

            # create a session
            session = cluster.connect()

            # create keyspace if not exist
            session.execute(KEYSPACE_PBS)
            
            # set the keyspace
            session.set_keyspace('Test_PBS')

            # create table job_info
            session.execute(TABLE_JOB_INFO)

            # prepared insert statement
            stmt = session.prepare('INSERT INTO job_info JSON ?')
            
            # execute insert
            session.execute(stmt, [json.dumps(json_data)])
            
    if 1: 
        keyspace_name = 'Test_PBS'
        table_name = 'job_info_testcluster'
        pbs_version ='19.2.8'
        pbs_timezone = 'Europe/Rome'

        print "Creating Cassandra connection..."
        cass = CassandraDb((CASS_HOST,), user=CASS_USER, password=CASS_PASSW)

        print "Loading Table schema..."
        TABLE_JOB_INFO = load_schema("./pbs_schema_" + pbs_version)

        print "init: create and set keyspaces and tables..."
        cass.db_init(keyspace_name, KEYSPACE_PBS, table_name, TABLE_JOB_INFO)
        
        print "init: create secondary indexes on job_info..."
        for k in secondary_indexes:
            cass.session.execute('CREATE INDEX IF NOT EXISTS ON "{0}"."{1}" ({2})'.format(keyspace_name, table_name, k))
        
        print "init: ceate prepared statememt..."
        cass.insert_stmt = cass.session.prepare('INSERT INTO "{0}" JSON ?'.format(table_name))

        print "Preprocessing data..."        
        json_data = sanitize_table_data(json_data, timezone_str=pbs_timezone)

        print "Inserting data..."
        cass.put_metrics(json_data)
        #print json.dumps(json_data, indent=4)

        print "Closing Cassandra connection..."
        cass.close()
        
    

