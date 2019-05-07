ARG BASE_IMAGE=centos:7
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-05-01

LABEL Name="senzing/mock-data-generator" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Install packages via yum.

RUN yum -y update; yum clean all
RUN yum -y install epel-release; yum clean all
RUN yum -y install \
    librdkafka-devel \
    python-devel \
    python-pip; \
    yum clean all

# Perform PIP installs.

RUN pip install \
    confluent-kafka \
    gevent \
    requests \
    pika

# Copy files from repository.

COPY ./rootfs /
COPY ./mock-data-generator.py /app

# Runtime execution.

ENV SENZING_DOCKER_LAUNCHED=true

WORKDIR /app
ENTRYPOINT ["/app/mock-data-generator.py"]
