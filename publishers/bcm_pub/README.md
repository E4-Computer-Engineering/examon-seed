# BCM plugin 

This plugin enables Examon to gather monitoring data from Bright Cluster Manager (BCM).

**NOTE:** This plugin is intended to be used when the Prometheus endpoint is not available. Otherwise, the `prometheus_pub` plugin should be used.

It reads real-time monitoring data from BCM using the `cmsh` command-line interface and publishes metrics to an MQTT broker or KairosDB database. The plugin uses a streaming approach to efficiently collect data from compute nodes and GPUs.

## Prerequisites

This script is intended to be executed on the Examon server side or on a node with access to the BCM `cmsh` shell. If running remotely, SSH access to the BCM management node is required.

## Configuration

Configure the `bcm_pub` plugin:
```bash
$ cp example_bcm_pub.conf bcm_pub.conf
```
Edit the `bcm_pub.conf` setting the following properties:

### MQTT Settings

|Key |Description |Example |
|:--|:---|:---|
| MQTT_BROKER | IP address of the MQTT broker | 127.0.0.1 |
| MQTT_PORT | Port of the MQTT broker | 1883 |
| MQTT_TOPIC | Root topic where to publish the collected data | org/\<organization\>/cluster/\<cluster\> |
| MQTT_USER | MQTT broker username | \<username\> |
| MQTT_PASSWORD | MQTT broker password | \<password\> |

### KairosDB Settings

|Key |Description |Example |
|:--|:---|:---|
| K_SERVERS | KairosDB server address | localhost |
| K_PORT | KairosDB port | 8080 |
| K_USER | KairosDB username | \<username\> |
| K_PASSWORD | KairosDB password | \<password\> |

### BCM Settings

|Key |Description |Example |
|:--|:---|:---|
| BCM_SHELL | Path to the BCM cmsh command | /cm/local/apps/cmd/bin/cmsh |
| BCM_HOST | BCM management node hostname/IP (leave empty if running locally) | bcm-mgt01 |
| BCM_USERNAME | Username for SSH access to BCM host | \<username\> |
| BCM_TOOL_CMD | BCM command to retrieve monitoring data | latestmonitoringdata -u -d ";" --raw -c compute,gpu |
| CACHE_MAX_SIZE | Maximum cache size for deduplication | 1000000 |
| CACHE_TIMEOUT | Cache entry timeout (seconds) | 604800 |

### Daemon Settings

|Key |Description |Example |
|:--|:---|:---|
| TS | Sampling time (seconds) | 60 |
| LOG_FILENAME | Log file name | bcm_pub.log |
| PID_FILENAME | PID file name | bcm_pub.pid |


## Install using docker compose

**NOTE 1:** When installing in docker/docker compose mode, ensure the container has access to the BCM `cmsh` command or can SSH to the BCM management node.

Build the docker images and containers.

### Post-Installation

After the container is created, we need to complete the configuration procedure to enable remote access to BCM.

1. In case of local access, ensure the BCM cmsh path is correctly configured:
```bash
$ docker exec -it examon-seed-examon-1 /bin/bash
root@16c4aa4d123e:/etc/examon_deploy/examon/scripts$ nano ../publishers/bcm_pub/bcm_pub.conf
```

2. In case of remote SSH access, enter the examon container and configure SSH keys:
```bash
$ docker exec -it examon-seed-examon-1 /bin/bash
root@16c4aa4d123e:/etc/examon_deploy/examon/scripts$ ssh-keygen -t rsa -b 4096
root@16c4aa4d123e:/etc/examon_deploy/examon/scripts$ ssh-copy-id -i ~/.ssh/id_rsa.pub <username>@<BCM_HOST>
```

Exit the container and restart to apply the configuration.

## Install locally 

```bash
cd bcm_pub
pip install [--user] -r requirements.txt
```

## Collected Metrics

The plugin collects a wide range of BCM monitoring metrics including:

| Metric Class | Description |
|:--|:---|
| CPU | CPU utilization, temperature, and frequency metrics |
| GPU | GPU utilization, memory, temperature, power, and health metrics |
| Memory | System memory usage statistics |
| Disk | Disk I/O, SMART health, and storage metrics |
| Network | Network interface statistics and health |
| Infiniband | Infiniband port counters and statistics |
| Power | Power consumption and energy metrics |
| OS | Operating system health checks |

The complete list of supported metrics is defined in `metrics.csv`.

## Options

```bash
usage: bcm_pub.py [-h] [-b MQTT_BROKER] [-p MQTT_PORT] [-t MQTT_TOPIC] [-s TS]
                  [-x PID_FILENAME] [-l LOG_FILENAME] [-d {mqtt,kairosdb}]
                  [-f {csv,json,bulk}] [--compress]
                  [--kairosdb-server K_SERVERS] [--kairosdb-port K_PORT]
                  [--kairosdb-user K_USER] [--kairosdb-password K_PASSWORD]
                  [--logfile-size LOGFILE_SIZE_B]
                  [--loglevel {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [--dry-run]
                  [--mqtt-user MQTT_USER] [--mqtt-password MQTT_PASSWORD]
                  [--version]
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
  --version             show program's version number and exit
```

## Example 

Execute as:   

```bash
python ./bcm_pub.py run|start
```

### Example (Local BCM access):
```bash
python ./bcm_pub.py \
    -b 127.0.0.1 -p 1883 \
    -t org/myorg/cluster/mycluster \
    -s 60 --loglevel INFO \
    run
```

Ensure the `BCM_HOST` and `BCM_USERNAME` are set in the configuration file for remote access.

## Data Tags

Each metric is published with the following tags:

| Tag | Description |
|:--|:---|
| org | Organization name (from MQTT topic) |
| cluster | Cluster name (from MQTT topic) |
| node | Node/entity name |
| parameter | Metric parameter (e.g., gpu0, sda) |
| type | Metric type/class (e.g., GPU, Disk) |
| unit | Measurement unit |
| plugin | Plugin identifier (bcm_pub) |
| chnl | Channel (data) |

## Systemd

This script is intended to be used as a service under systemd. 
Execute with "run" and SIGINT should be used as the signal to cleanly 
stop/kill the running script.


## Changelog

### [v0.1.0] - 2024

- Initial release
- Added support for gathering monitoring data from Bright Cluster Manager
- Implemented streaming data collection via cmsh shell
- Added caching for metric deduplication
- Support for compute nodes and GPU metrics
- MQTT and KairosDB output backends

