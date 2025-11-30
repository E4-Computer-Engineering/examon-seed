FROM examonhpc/examon:0.2.0

ENV EXAMON_HOME /etc/examon_deploy/examon

RUN mv /etc/apt/sources.list /etc/apt/sources.list.backup

RUN touch /etc/apt/sources.list

RUN echo "deb http://archive.debian.org/debian stretch main non-free" > /etc/apt/sources.list

RUN apt-get -y update

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    libffi-dev \
    build-essential \
    libssl-dev \
    python-dev \
	&& rm -rf /var/lib/apt/lists/*

# copy app
ADD ./publishers/pbs_pub ${EXAMON_HOME}/publishers/pbs_pub
ADD ./publishers/enelx_pub ${EXAMON_HOME}/publishers/enelx_pub
ADD ./publishers/bcm_pub ${EXAMON_HOME}/publishers/bcm_pub
ADD ./lib/examon-common $EXAMON_HOME/lib/examon-common
ADD ./docker/examon/supervisor.conf /etc/supervisor/conf.d/supervisor.conf
ADD ./scripts/examon.conf $EXAMON_HOME/scripts/examon.conf
ADD ./web $EXAMON_HOME/web

# install
RUN pip --trusted-host pypi.python.org install --upgrade pip==20.1.1
ENV PIP $EXAMON_HOME/scripts/ve/bin/pip

WORKDIR $EXAMON_HOME/lib/examon-common
RUN $PIP install .
RUN pip install .

WORKDIR $EXAMON_HOME/publishers/pbs_pub
RUN CASS_DRIVER_BUILD_CONCURRENCY=8 pip install -r requirements.txt

WORKDIR $EXAMON_HOME/publishers/enelx_pub
RUN pip install -r requirements.txt

WORKDIR $EXAMON_HOME/publishers/bcm_pub
RUN pip install -r requirements.txt

WORKDIR $EXAMON_HOME/web
RUN virtualenv flask
RUN flask/bin/pip --trusted-host pypi.python.org install --upgrade pip==20.1.1
RUN CASS_DRIVER_BUILD_CONCURRENCY=8 flask/bin/pip --trusted-host pypi.python.org install -r ./examon-server/requirements.txt

WORKDIR $EXAMON_HOME/scripts

EXPOSE 1883 9001

CMD ["./frontend_ctl.sh", "start"]
