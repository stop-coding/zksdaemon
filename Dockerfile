
ARG PYTHON_VERSION=latest

FROM python:${PYTHON_VERSION}

WORKDIR /app
COPY zksDaemon.py /app
COPY init.sh /app
RUN pip3 install kazoo && \
    chmod +x /app/init.sh

ENTRYPOINT ["/app/init.sh"]
