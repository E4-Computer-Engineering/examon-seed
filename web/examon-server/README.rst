Examon server
=============

Examon RESTful web service


Configuration
-------------

Entries of the ``.conf`` file to be defined for this plugin.

AUTH_URL
  URL of the authentication service. ``http://<grafana_IP>:<grafana_PORT>/api/datasources/id/kairosdb``
CASSANDRA_IP
  Cassandra node IP 
CASSANDRA_KEY_SPACE
  Cassandra keyspace to be created/used for this application
CASSANDRA_USER
  Cassandra username
CASSANDRA_PASSW
  Cassandra password
EXAMON_SERVER_HOST
  IP address of the web service
EXAMON_SERVER_PORT
  Port of the web service


Install
-------
::

  $ mkdir examon-server
  $ cd examon-server
  $ virtualenv flask
  New python executable in flask/bin/python
  Installing setuptools............................done.
  Installing pip...................done.
  $ flask/bin/pip install -r requirements.txt


Systemd
-------
::

  /etc/systemd/system$ cat examon-server.service
  [Unit]
  Description=Examon Server
  After=network.target

  [Service]
  User=ubuntu
  WorkingDirectory=/home/ubuntu/examon-server
  ExecStart=/home/ubuntu/examon-server/flask/bin/python ./server.py
  Restart=always

  [Install]
  WantedBy=multi-user.target
