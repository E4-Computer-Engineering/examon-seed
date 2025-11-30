# PBS plugin 

This plugin enables Examon to gather data from the PBS workload manager.

It istantiates a pool of workers, one for each PBS host (login node).
Every sampling interval, the worker reads the terminated jobs info as a JSON payload
and stores them in a database table (Cassandra).

## Prerequisites

This script is intended to be executed on the Examon server side. Moreover, it needs 
access to the cluster login node with simple user level privileges (no admin required).

## Configuration

Configure the `pbs_pub` plugin:
 ```bash
$ cp example_pbs_pub.conf pbs_pub.conf
```
Edit the `pbs_pub.conf` setting the following properties:

|Key |Description |Example |
|:--|:---|:---|
| MQTT_TOPIC | Root topic where to publish the collected data | org/\<organization_name\> |
| PBS_HOSTS | List of PBS servers | \<cluster_name0\>:\<PBS_server_address0\>,<br> \<cluster_name1\>:\<PBS_server_address1\>,<br>... |
| PBS_HOST_USER | Service account user name | \<username\> |
| PBS_HOST_PASSW | Service account password | \<password\> |
| PBS_HOST_KEY | Path to the RSA key in case of passwordless ssh access.<br> In this case the PBS_HOST_PASSW can be omitted. | /home/\<username\>/.ssh/id_rsa |
| PBS_VERSION | PBS version | 19.2.8 |
| PBS_TIMEZONE | Timezone of PBS timestamps | Europe/Rome |
| PBS_QSELECT_CMD | qselect command to select running and finished jobs | bash -c 'qselect -x' |
| PBS_PARSER_TIMEOUT | Timeout for PBS parser (seconds) | 180 |
| PBS_QSELECT_CMD_TIMEOUT | Timeout for qselect command (seconds) | 60 |
| PBS_QSTAT_CMD_TARGET_TIME | Target execution time for qstat command (seconds) | 15.0 |
| PBS_QSTAT_CMD_MIN_BATCH | Minimum batch size for qstat command | 25 |
| PBS_QSTAT_CMD_MAX_BATCH | Maximum batch size for qstat command | 500 |
| CASS_HOST | Cassandra host. Format: \<hostname|IP address\>:\<port\> | localhost:9042 |
| CASS_USER | Cassandra user name | cassandra |
| CASS_PASSW | Cassandra password | cassandra |
| CASS_KEYSPACE_NAME | Cassandra keyspace name | \<organization_name\>_PBS |
| CASS_TIMEOUT | Cassandra timeout (seconds) | 60 |
| CACHE_MAX_SIZE | Cache max size | 10000 |
| CACHE_TIMEOUT | Cache timeout (seconds) | 1000 |
| EXAMON_DB_IP | Examon database IP address | 127.0.0.1 |
| EXAMON_DB_PORT | Examon database port | 3000 |
| EXAMON_USER | Examon database user name | examon |
| EXAMON_PWD | Examon database password | examon |
| NODE_CONFIG_FILE | Node configuration file | node_config.json |
| JOB_TABLE_NAME | Job table name | job_info |
| JOB_ENERGY_UNIT | Job energy unit | Wh |
| TIMEOUT | Maximum execution time for building job index (seconds) | 1800 |
| TS | Sampling time (seconds) | 60 |
| LOG_FILENAME | Log file name | pbs_pub.log |
| PID_FILENAME | PID file name | pbs_pub.pid |


## Install using docker compose

**NOTE 1:** When installing in docker/docker compose mode, the RSA key will need to be generated within the container at the end of infrastructure creation. See the Post-installation section below.

**NOTE 2:** Setting the password at this step of the installation means saving it inside the docker image and exposing it in plain text. See the Post-installation section below.

Build the docker images and containers.

### Post-Installation

After the container is created, we need to complete the configuration procedure to set the password or enable passwordless authentication.

1. In case of using the password, enter the examon container and edit the pbs_pub.conf file:
 ```bash
$ docker exec -it examon-seed-examon-1 /bin/bash
root@16c4aa4d123e:/etc/examon_deploy/examon/scripts$ nano ../publishers/pbs_pub/pbs_pub.conf
```
2. In case of passwordless authentication, enter the examon container and follow the usual procedure:

 ```bash
$ docker exec -it examon-seed-examon-1 /bin/bash
root@16c4aa4d123e:/etc/examon_deploy/examon/scripts$ ssh-keygen -t rsa -b 4096
root@16c4aa4d123e:/etc/examon_deploy/examon/scripts$ ssh-copy-id -i ~/.ssh/id_rsa.pub <username>@<PBS_server_address0>
```
**NOTE:** It is required to run the last command for each PBS server in the configuration

Exit the container and restart to apply the configuration

## Install locally 

```bash
cd pbs_pub
pip install [--user] -r requirements.txt
```
	
## Options

```bash
	usage: pbs_pub.py [-h] [-b MQTT_BROKER] [-p MQTT_PORT] [-t MQTT_TOPIC] [-s TS]
					[-x PID_FILENAME] [-l LOG_FILENAME] [-d {mqtt,kairosdb}]
					[-f {csv,json,bulk}] [--compress]
					[--kairosdb-server K_SERVERS] [--kairosdb-port K_PORT]
					[--kairosdb-user K_USER] [--kairosdb-password K_PASSWORD]
					[--logfile-size LOGFILE_SIZE_B]
					[--loglevel {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [--dry-run]
					[--mqtt-user MQTT_USER] [--mqtt-password MQTT_PASSWORD]
					[--pbs-hosts PBS_HOSTS] [--pbs-timezone PBS_TIMEZONE]
					[--pbs-qseelect-cmd PBS_QSELECT_CMD]
					[--host-username PBS_HOST_USER]
					[--host-password PBS_HOST_PASSW] [--host-key PBS_HOST_KEY]
					[--cassandra-hosts CASS_HOST]
					[--cassandra-username CASS_USER]
					[--cassandra-password CASS_PASSW] [--version]
					{run,start,restart,stop}

	positional arguments:
	{run,start,restart,stop}
							Run mode

	optional arguments:
	-h, --help            show this help message and exit
	-b MQTT_BROKER        IP address of the MQTT broker
	-p MQTT_PORT          Port of the MQTT broker
	-t MQTT_TOPIC         MQTT topic
	-s TS                 Sampling time (seconds)
	-x PID_FILENAME       pid filename
	-l LOG_FILENAME       log filename
	-d {mqtt,kairosdb}    select where to send data (default: mqtt)
	-f {csv,json,bulk}    MQTT payload format (default: csv)
	--compress            enable payload compression (default: False)
	--kairosdb-server K_SERVERS
							kairosdb servers
	--kairosdb-port K_PORT
							kairosdb port
	--kairosdb-user K_USER
							kairosdb username
	--kairosdb-password K_PASSWORD
							kairosdb password
	--logfile-size LOGFILE_SIZE_B
							log file size (max) in bytes
	--loglevel {DEBUG,INFO,WARNING,ERROR,CRITICAL}
							log level
	--dry-run             Data is not sent to the broker if True (default:
							False)
	--mqtt-user MQTT_USER
							MQTT username
	--mqtt-password MQTT_PASSWORD
							MQTT password
	--pbs-hosts PBS_HOSTS
							PBS hosts list, comma separated. Format:
							<cluster_name>:<login_hostname|IP address>
	--pbs-timezone PBS_TIMEZONE
							Timezone of PBS timestamps
	--pbs-qseelect-cmd PBS_QSELECT_CMD
							qselect command on the PBS host to select running and
							finished jobs
	--host-username PBS_HOST_USER
							Username to login to the PBS host
	--host-password PBS_HOST_PASSW
							Password to login to the PBS host
	--host-key PBS_HOST_KEY
							Path to the private RSA key to be used for
							passwordless authentication on the PBS host
	--cassandra-hosts CASS_HOST
							Cassandra host. Format: <hostname|IP address>:<port>
	--cassandra-username CASS_USER
							Username to login to the Cassandra host
	--cassandra-password CASS_PASSW
							Password to login to the Cassandra host
	--version             show program's version number and exit
```

## Example 

Execute as:   

```bash
    python ./pbs_pub.py run|start
```

### Example:
```bash

	python ./pbs_pub.py start \
	--pbs-hosts <cluster_name:login_ip_address> \
	--host-username <login_user>  --host-password <login_pass>  \
	--cassandra-hosts <cassandra_ip>:9042 --cassandra-username <cass_user> --cassandra-password <cass_pass> \
	-s 60 --loglevel INFO
```

## Cassandra db tables

The plugin creates, for each cluster, a new table in the "CASS_KEYSPACE_NAME" Keyspace.
The table is conventionally named "job_info_<cluster_name>. The label is taken from the "PBS_HOSTS" 
configuration parameter.
	

This is a description of the fields of the job_info db table (PBS v2022.1.0)

[PBSReferenceGuide19.2.1.pdf](https://help.altair.com/2022.1.0/PBS%20Professional/PBSReferenceGuide2022.1.pdf)

	
| Column Name | Description |
|----------------------|-----------------------------------------------------------------------------|
| Account_Name | The name of the account associated with the job. |
| argument_list | The list of arguments passed to the job script or executable. |
| Checkpoint | The checkpointing option for the job (e.g., periodic, none). |
| comment | A comment associated with the job. |
| ctime | The creation time of the job. |
| depend | Specifies inter-job dependencies. |
| energy | The energy usage of the job, if available. |
| Error_Path | The path to the file where standard error output is redirected. |
| etime | The time when the job became eligible to run. |
| exec_host | The host(s) on which the job is executed. |
| exec_vnode | The virtual nodes on which the job is executed. |
| executable | JSDL-encoded listing of job’s executable. |
| Exit_status | The exit status of the job script or executable. Set to zero for successful execution. |
| forward_x11_port | Contains the number of the port being listened to by the port forwarder on the submission host. |
| Hold_Types | The types of holds placed on the job (e.g., user, system). |
| history_timestamp | Timestamp for when the job history is recorded. |
| interactive | Indicates if the job is interactive (true) or not (false). |
| Job_Id | The unique identifier for the job. |
| Job_Name | The name of the job. |
| Job_Owner | The login name on the submitting host of the user who submitted the batch job. |
| job_state | The current state of the job (e.g., queued, running, finished). |
| jobdir | The working directory of the job. |
| Join_Path | Indicates if standard output and standard error are combined. |
| Keep_Files | Specifies which files should be kept after job completion. |
| Mail_Points | Specifies when mail messages are sent about the job. |
| Mail_Users | The list of users to receive mail messages about the job. |
| mtime | Time that the job was last modified, changed state, or changed locations. |
| obittime | Time when job or subjob obit was sent. |
| Output_Path | The path to the file where standard output is redirected. |
| Priority | The priority of the job. Higher value indicates greater priority|
| project | The project associated with the job. |
| qtime | The time when the job was queued. |
| queue | The queue in which the job resides. |
| Rerunable | Indicates if the job can be rerun (y) or not (n). |
| Resource_List | The list of resources requested by the job. List is a set of \<resource name\>=\<value\> strings |
| resources_used | The amount of each resource used by the job. List is a set of \<resource name\>=\<value\> strings |
| run_count | The number of times the server thinks the job has been executed. |
| sandbox | Specifies type of location PBS uses for job staging and execution. |
| server | The server managing the job. |
| session_id | The session ID of the job. |
| Stageout_status | The status of the job's stage out operation. |
| Shell_Path_List | The shell that executes the job script. |
| stime | The start time of the job. Updated when job is restarted |
| substate | The substate code of the job. |
| Submit_arguments | Job submission arguments given on the qsub command line. Available for all jobs. |
| umask | The umask setting for the job. |
| Variable_List | The list of environment variables for the job. |
| pbs_version | The version of PBS that submitted the job. |
	

## Systemd

This script is intended to be used as a service under systemd. 
Execute with "run" and SIGINT should be used as the signal to cleanly 
stop/kill the running script.

Execute with "run" and SIGINT should be used as the signal to cleanly 
stop/kill the running script.


## Changelog
### Changelog

#### [Released] - 2025-11-28

- Initial release

#### [0.1.0] - 2024-03-12
- Initial setup and integration with PBS and Examon
- Added support for gathering job data from PBS
- Implemented Cassandra database storage for job data
- Added command line options for configuration


