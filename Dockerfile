FROM centos:7
ENV REFRESHED_AT=2018-12-05

# Install prerequisites.

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
    requests

# Copy into the app directory.

RUN mkdir /app
COPY ./mock-data-generator.py /app

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["/app/mock-data-generator.py"]
