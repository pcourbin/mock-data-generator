ARG BASE_IMAGE=centos:7
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-07-23

LABEL Name="senzing/mock-data-generator" \
      Maintainer="support@senzing.com" \
      Version="1.1.0"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via yum.

RUN yum -y update; yum clean all
RUN yum -y install epel-release; yum clean all
RUN yum -y install \
    bzip2-devel \
    gcc \
    libffi-devel \
    librdkafka-devel \
    make \
    openssl-devel \
    python-devel \
    python-pip \
    wget; \
    yum clean all

# Install Python 3.7

WORKDIR /usr/src
RUN wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz \
 && tar xzf Python-3.7.3.tgz \
 && cd Python-3.7.3 \
 && ./configure --enable-optimizations \
 && make altinstall \
 && rm /usr/src/Python-3.7.3.tgz \
 && rm -rf /usr/src/Python-3.7.3

# Make soft links for Python 3.7. See https://www.python.org/dev/peps/pep-0394

RUN ln -sf /usr/local/bin/easy_install-3.7  /usr/bin/easy_install3 \
 && ln -sf /usr/local/bin/idle3.7           /usr/bin/idle3 \
 && ln -sf /usr/local/bin/pip3.7            /usr/bin/pip3 \
 && ln -sf /usr/local/bin/pydoc3.7          /usr/bin/pydoc3 \
 && ln -sf /usr/local/bin/python3.7         /usr/bin/python3 \
 && ln -sf /usr/local/bin/python3.7m-config /usr/bin/python3-config  \
 && ln -sf /usr/local/bin/pyvenv-3.7        /usr/bin/pyvenv3

# Install packages via pip.

RUN pip3 install --upgrade pip

RUN pip3 install \
      confluent-kafka \
      gevent \
      requests \
      pika

# Copy files from repository.

COPY ./rootfs /
COPY ./mock-data-generator.py /app

# Make non-root container.

USER 1001

# Runtime execution.

ENV SENZING_DOCKER_LAUNCHED=true

WORKDIR /app
ENTRYPOINT ["/app/mock-data-generator.py"]
