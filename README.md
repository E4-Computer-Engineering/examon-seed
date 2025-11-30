<p align="center">
    <img src="https://github.com/fbeneventi/panels/raw/main/logo3_trasp.png" alt="ExaMon" width="20%" style="margin-right: 20px;">
    <img src="https://e4company.com/wp-content/uploads/SEED_logo.svg" alt="SEED" width="20%">
</p>


# ExaMon - SEED Setup

This is a containerized version of [ExaMon](https://examonhpc.github.io/examon) developed for the Davinci-1 HPC cluster as part of the EU-funded SEED project.

Please find more information about the SEED project [here](https://www.leonardo.com/en/news-and-stories-detail/-/detail/leonardo-guida-seed-programma-di-ricerca-ue).

## Overview

A local version of the ExaMon framework was implemented to monitor the [Davinci-1](https://www.leonardo.com/it/innovation-technology/davinci-1?gad_source=1&gad_campaignid=16731314549&gbraid=0AAAAABtOW1M7S6FA7MYPQVnzDdia1da5D&gclid=Cj0KCQiA0KrJBhCOARIsAGIy9wBTpxgxvTu3KahuQngzSd0LLEH2dxP-tHlLV5sVfTJkm-zfTo2uoQYaAvRGEALw_wcB) supercomputer. Both the ExaMon frontend (message bus, database and visualization) and the backend (collectors) are installed entirely on the internal supercomputer services infrastructure.

## Out-of-band plugins

The following ExaMon plugins have been developed and installed for this project, they execute outside the compute nodes to collect the following data:

- **pbs_pub**: data related to the job scheduler (PBS) such as all the statistics about the jobs executed on the system.
- **enelx_pub**: data related to the energy consumption and carbon emissions of the cluster measured by the EnelX EMS platform.
- **bcm_pub**: data related to the monitoring of the cluster hardware from the Bright Cluster Manager (BCM) management framework.

## Getting Started

The guide below describes the installation of the ExaMon framework using Docker.

The installation procedure requires the following minimum prerequisites:
- Docker, Docker Compose or Podman and Podman-compose
- Any additional prerequisiste required by each plugin 

### Installation

1. Clone the repository

2. Build the docker images and containers:
 ```bash
$ cd examon-seed/
$ docker compose up -d
```
At the end of the installation process you will have 4 containers in UP state:

 ```bash
$ docker ps
CONTAINER ID   IMAGE                        COMMAND                  CREATED       STATUS       PORTS                                                 NAMES
16c4aa4d123e   examonhpc/examon:0.1.1       "./frontend_ctl.sh s…"   8 hours ago   Up 8 hours   0.0.0.0:1883->1883/tcp, :::1883->1883/tcp, 9001/tcp   examon-seed-examon-1
d08b57d9acdd   examonhpc/grafana:7.3.10     "/run.sh"                8 hours ago   Up 8 hours   0.0.0.0:3000->3000/tcp, :::3000->3000/tcp             examon-seed-grafana-1
f53757dbca1a   examonhpc/kairosdb:1.2.2     "/usr/bin/config-kai…"   8 hours ago   Up 8 hours   2003/tcp, 4242/tcp, 8083/tcp                          examon-seed-kairosdb-1
d6eb6bc3596f   examonhpc/cassandra:3.0.19   "docker-entrypoint.s…"   8 hours ago   Up 8 hours   7000-7001/tcp, 7199/tcp, 9042/tcp, 9160/tcp           examon-seed-cassandra-1
```

To verify that everything is working properly, the logs should look as follow:
 ```bash
$ docker logs  --tail=10 examon-seed_examon_1
DEBUG - 05/19/2024 03:50:19 PM - [Process-1] - [sensorreader.py] - examon.plugin.sensorreader - Start new loop
INFO - 05/19/2024 03:50:19 PM - [Process-2] - [pbs_pub.py] - examon - [xxx.xxx.x.x] Found 0 terminated jobs
INFO - 05/19/2024 03:50:19 PM - [Process-2] - [pbs_pub.py] - examon - [xxx.xxx.x.x] Inserted 0 new jobs
DEBUG - 05/19/2024 03:50:19 PM - [Process-2] - [pbs_pub.py] - examon - [xxx.xxx.x.x] Cache size: 1

==> /var/log/mqtt2kairosdb.log <==
INFO - 05/19/2024 03:50:27 PM - [MainProcess] - [executor.py] - examon - 5/5 workers alive

==> /var/log/pbs_pub.log <==
INFO - 05/19/2024 03:50:28 PM - [MainProcess] - [executor.py] - examon - 2/2 workers alive
 ```

**Note** Some of the plugins may require additional configuration to be fully functional. Please refer to the respective plugin readme file for further details.

## Configuration

### Configure Grafana

Log in to the Grafana server using your browser and the default credentials:

**NOTE:** This installation sets the default password to `GF_SECURITY_ADMIN_PASSWORD` in the `docker-compose.yml` file.

http://localhost:3000

Follow the normal procedure for adding a new data source:

[Add a Datasource](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/)

From the Grafana UI, add a new data source and select `KairosDB`.

Fill out the form with the following settings:

 - Name: `kairosdb` 
 - Url: http://kairosdb:8083 
 - Access: `Server`

Now you are ready to create or import dashboards.

### Configure the plugins

To configure ExaMon it is necessary to define all the properties of the `.conf` configuration file of the plugins with the appropriate values related to the local cluster options. 
The configuration files to be edited are located in the following folders:

| Plugin/tool   | Path                                                                                   |
|---------------|----------------------------------------------------------------------------------------|
| pbs_pub       | `/publishers/pbs_pub`                                                                  |
| enelx_pub     | `/publishers/enelx_pub`                                                                |
| bcm_pub       | `/publishers/bcm_pub`                                                                  |

Please refer to the respective plugin readme file (*Configuration* section) for further details.

### Manage the plugins

The plugins are managed by supervisord, which is the microservices manager for the examon container.

The majority of the commands follow the supervisorctl syntax:

```bash
supervisorctl <command> <plugin-name>
```

The most used commands are:

- `start`
- `stop`
- `restart`
- `status`
- `tail`

to see the full list of commands, you can use the following command:

```bash
docker exec -it <examon-container-name> supervisorctl help
```

To start the plugins, you need to run the following command:

```bash
docker exec -it <examon-container-name> supervisorctl start <plugin-name>
```
Example:

```bash
docker exec -it examon supervisorctl start plugins:random_pub
```

Or, if you want to start all the plugins, you can use the following command:

```bash
docker exec -it <examon-container-name> supervisorctl start plugins:*
```
As an alternative, you can open the supervisor shell to manage the plugins and start/stop them individually:

```bash
docker exec -it <examon-container-name> supervisorctl
```

### Check the logs

To check the logs of the plugins, you can use the following command:

```bash
docker exec -it <examon-container-name> supervisorctl tail [-f] <plugin-name>
```

### Enable/disable the plugins

Some plugins may be disabled by default and need to be started manually each time the examon container is started.

To enable and start the plugins automatically, you need to edit the supervisor configuration file for the examon service.

```bash
docker exec -it <examon-container-name> bash

vi /etc/supervisor/conf.d/supervisor.conf
```
Then, for each plugin, set the following parameters to true:

```bash
autostart=True
```
Restart the examon container to apply the changes:

```bash
docker restart <examon-container-name>
```
Please note that the supervisor configuration will be lost in case the container is recreated.
To make the settings persistent, you need to edit the supervisor configuration file in `docker/examon/supervisor.conf` and rebuild.

## Examon server configuration

The Examon server must be enabled in the supervisor configuration file and configured to use the Examon REST API.

Please refer to the `README.rst` file in the `web/examon-server` folder for more information.

**NOTE:** The Cassandra related settings must be the same as the ones used in the workload scheduler publisher in the Cassandra section.

## Data persistence

During the installation, two Docker volumes are created, which are required for data persistence.

 ```bash
$ docker volume ls
DRIVER    VOLUME NAME
local     examon-seed_cassandra_volume
local     examon-seed_grafana_volume
 ```

*   The `examon-seed_cassandra_volume` is used to store the collected metrics
*   The `examon-seed_grafana_volume` is used to store Grafana:
    *   users account data
    *   dashboards

To set a custom volume path, you can use the following settings in the `docker-compose.yml` file:

```yaml
volumes:
  examon_seed_cassandra_volume:
    driver: local
    driver_opts:
      type: none
      device: /path/to/cassandra/volume
      o: bind
  examon_seed_grafana_volume:
    driver: local
    driver_opts:
      type: none
      device: /path/to/grafana/volume
      o: bind  
```