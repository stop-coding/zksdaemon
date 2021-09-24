
ARG PYTHON_VERSION=latest

FROM python:${PYTHON_VERSION}

WORKDIR /app
COPY zks_daemon.py /app
RUN pip3 install kazoo
ENTRYPOINT ["/usr/local/bin/python3", "/app/zks_daemon.py"]
