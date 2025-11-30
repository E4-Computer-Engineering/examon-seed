FROM grafana/grafana:7.3.10-ubuntu

COPY --chown=grafana:root ./kairosdb-datasource-3-alerts /var/lib/grafana/plugins/kairosdb-datasource-3-alerts
COPY --chown=grafana:root grafana.ini /etc/grafana/

VOLUME /var/lib/grafana


