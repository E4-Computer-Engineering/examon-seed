#!/bin/bash

docker run -d --name grafana --restart=always \
  -p 3000:3000 \
  -v grafana_vol:/var/lib/grafana \
  examon/grafana:7.3.10-ubuntu
