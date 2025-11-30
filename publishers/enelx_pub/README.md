# EnelX plugin

This plugin enables Examon to gather energy consumption data from the EnelX Energy Management System (EMS) platform.

It reads power consumption, energy consumption, and carbon emissions data from EnelX monitoring points (motes) and publishes metrics to an MQTT broker or KairosDB database. The plugin retrieves daily metrics at each sampling interval.

## ⚠️ IMPORTANT LEGAL DISCLAIMER

**Please read this notice carefully before using this software.**

### 1. Unofficial Software
This software is an **unofficial** research tool. It is **not** developed, maintained, sponsored, approved, or endorsed by **Enel X** or any of its subsidiaries or affiliates. All product names, logos, and brands are property of their respective owners. Use of the "Enel X" name is for identification and reference purposes only, to indicate interoperability.

### 2. Scientific & Interoperability Purpose
This code was developed as part of an **EU-funded scientific research project** ([SEED](https://www.leonardo.com/en/news-and-stories-detail/-/detail/leonardo-guida-seed-programma-di-ricerca-ue)).

The primary purpose of this tool is to achieve **interoperability** and enable **data portability** strictly for the user's own data, in alignment with:
* **GDPR Article 20:** Facilitating the user's right to receive their personal data in a structured, commonly used, and machine-readable format.
* **Directive 2009/24/EC (Software Directive):** Allowing reproduction of code/protocols where necessary to achieve interoperability with an independently created computer program.

### 3. "AS IS" Warranty & Liability
This software is provided "AS IS", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and non-infringement.

**In no event shall the authors, the project consortium, or the copyright holders be liable for any claim, damages, or other liability**, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.

### 4. User Responsibility & Terms of Service
By using this software, you acknowledge and agree that:
* **You act as the Data Controller:** You are solely responsible for how you access and process your own data.
* **Risk of Account Suspension:** Automated access to web services may violate the provider's Terms of Service. The authors accept no responsibility if your account is suspended, banned, or restricted by Enel X as a result of using this tool.
* **Responsible Usage:** You agree not to use this tool for malicious purposes, Distributed Denial of Service (DDoS) attacks, or to access data belonging to accounts you do not own or do not have explicit permission to manage.

### 5. Rate Limiting
This tool includes mechanisms to limit the request rate to avoid overloading the target servers. Users should not modify or remove these safeguards.

## Prerequisites

This script is intended to be executed on the Examon server side. It requires:
- Valid EnelX EMS account credentials
- List of mote IDs to monitor
- Internet access to the EnelX EMS platform

## Configuration

Configure the `enelx_pub` plugin:
```bash
$ cp example_enelx_pub.conf enelx_pub.conf
```
Edit the `enelx_pub.conf` setting the following properties:

### MQTT Settings

|Key |Description |Example |
|:--|:---|:---|
| MQTT_BROKER | IP address of the MQTT broker | 127.0.0.1 |
| MQTT_PORT | Port of the MQTT broker | 1883 |
| MQTT_TOPIC | Root topic where to publish the collected data | org/\<organization\>/site/\<site\> |
| MQTT_USER | MQTT broker username | \<username\> |
| MQTT_PASSWORD | MQTT broker password | \<password\> |

### KairosDB Settings

|Key |Description |Example |
|:--|:---|:---|
| K_SERVERS | KairosDB server address | localhost |
| K_PORT | KairosDB port | 8080 |
| K_USER | KairosDB username | \<username\> |
| K_PASSWORD | KairosDB password | \<password\> |

### EnelX Settings

|Key |Description |Example |
|:--|:---|:---|
| ENELX_URL | URL of the EnelX EMS platform | https://ems.enelx.com |
| ENELX_USERNAME | Username for EnelX EMS platform access | \<username\> |
| ENELX_PASSWORD | Password for EnelX EMS platform access | \<password\> |
| MOTE_DICT | JSON dictionary mapping device names to mote IDs | {"PDU_A": "12345-#red", "PDU_B": "67890-#blue"} |
| ENELX_DEP_ID | EnelX deployment ID | \<deployment_id\> |
| ENELX_DEP_TOKEN | EnelX deployment token | \<deployment_token\> |
| ENELX_ACCOUNT_ID | EnelX carbon account ID | \<account_id\> |

### Daemon Settings

|Key |Description |Example |
|:--|:---|:---|
| ORGANIZATION | Organization name for metric tagging | examon |
| SITE | Site name for metric tagging | examon_hq |
| TS | Sampling time (seconds) | 900 |
| LOG_LEVEL | Logging level | INFO |
| LOG_FILENAME | Log file name | enelx_pub.log |
| PID_FILENAME | PID file name | enelx_pub.pid |


## Install using docker compose

**NOTE:** When installing in docker/docker compose mode, ensure the container has internet access to reach the EnelX EMS platform.

Build the docker images and containers.

### Post-Installation

After the container is created, edit the configuration file to set the EnelX credentials:
```bash
$ docker exec -it examon-seed-examon-1 /bin/bash
root@16c4aa4d123e:/etc/examon_deploy/examon/scripts$ nano ../publishers/enelx_pub/enelx_pub.conf
```

Exit the container and restart to apply the configuration.

## Install locally 

```bash
cd enelx_pub
pip install [--user] -r requirements.txt
```

## Collected Metrics

The plugin collects the following metrics from EnelX:

| Metric Type | Description |
|:--|:---|
| Power | Real-time power consumption data (W) |
| Energy | Energy consumption data (Wh/kWh) |
| Carbon | Carbon emissions data (kgCO2) |

## Options

```bash
usage: enelx_pub.py [-h] [-b MQTT_BROKER] [-p MQTT_PORT] [-t MQTT_TOPIC]
                    [-s TS] [-x PID_FILENAME] [-l LOG_FILENAME]
                    [-d {mqtt,kairosdb}] [-f {csv,json,bulk}] [--compress]
                    [--kairosdb-server K_SERVERS] [--kairosdb-port K_PORT]
                    [--kairosdb-user K_USER] [--kairosdb-password K_PASSWORD]
                    [--logfile-size LOGFILE_SIZE_B]
                    [--loglevel {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                    [--dry-run] [--mqtt-user MQTT_USER]
                    [--mqtt-password MQTT_PASSWORD]
                    [--username ENELX_USERNAME] [--password ENELX_PASSWORD]
                    [--organization ORGANIZATION] [--site SITE]
                    [--mote-dict MOTE_DICT]
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
  --username ENELX_USERNAME
                        EnelX username
  --password ENELX_PASSWORD
                        EnelX password
  --organization ORGANIZATION
                        Organization
  --site SITE           Site
  --mote-dict MOTE_DICT
                        Dictionary of mote names and IDs (JSON object)
```

## Example 

Execute as:   

```bash
python ./enelx_pub.py run|start
```

### Example:
```bash
python ./enelx_pub.py \
    -b 127.0.0.1 -p 1883 \
    --username myuser --password mypass \
    --organization myorg --site mysite \
    --mote-dict '{"PDU_A": "12345-#red", "PDU_B": "67890-#blue"}' \
    -s 900 --loglevel INFO \
    run
```

## Data Tags

Each metric is published with the following tags:

| Tag | Description |
|:--|:---|
| org | Organization name |
| site | Site name |
| type | Metric type (power, energy, carbon) |
| units | Measurement unit |
| plugin | Plugin identifier (enelx_pub) |
| chnl | Channel (data) |

## Systemd

This script is intended to be used as a service under systemd. 
Execute with "run" and SIGINT should be used as the signal to cleanly 
stop/kill the running script.

## Rate Limiting

This tool is designed to respect the server's stability. It performs requests sequentially and allows for user-defined delays to minimize impact on the service provider.

## Changelog

### [v0.1.0] - 2024

- Initial release
- Added support for gathering energy data from EnelX EMS platform
- Power consumption metrics
- Energy consumption metrics
- Carbon emissions metrics
- MQTT and KairosDB output backends

