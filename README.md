# mock-data-generator

## Overview

The [mock-data-generator.py](mock-data-generator.py) python script produces mock data for Senzing.
The `senzing/mock-data-generator` docker image produces mock data for Senzing for use in
docker formations (e.g. docker-compose, kubernetes).

[mock-data-generator.py](mock-data-generator.py) has a number of subcommands
for performing different types of Senzing mock data creation.

To see all of the subcommands, run

```console
$ ./mock-data-generator.py --help
usage: mock-data-generator.py [-h]
                              {random-to-stdout,random-to-kafka,url-to-stdout,url-to-kafka}
                              ...

Generate mock Senzing data from a template and random data

positional arguments:
  {random-to-stdout,random-to-kafka,url-to-stdout,url-to-kafka}
                        Subcommands (SENZING_SUBCOMMAND):
    random-to-stdout    Send random data to STDOUT
    random-to-kafka     Send random data to Kafka
    url-to-stdout       Send HTTP / file data to STDOUT
    url-to-kafka        Send HTTP / file data to Kafka

optional arguments:
  -h, --help            show this help message and exit
```

To see the options for a subcommand, run commands like:

```console
./mock-data-generator.py random-to-stdout --help
```

### Contents

1. [Using Command Line](#using-command-line)
    1. [Prerequisite software](#prerequisite-software)
    1. [Set environment variables](#set-environment-variables)
    1. [Clone repository](#clone-repository)
    1. [Install](#install)
    1. [Demonstrate](#demonstrate)
1. [Using Docker](#using-docker)
    1. [Build docker image](#build-docker-image)
    1. [Configuration](#configuration)
    1. [Run docker image](#run-docker-image)
        1. [Demonstrate random to STDOUT](#demonstrate-random-to-stdout)
        1. [Demonstrate random to Kafka](#demonstrate-random-to-kafka)
        1. [Demonstrate URL to STDOUT](#demonstrate-url-to-stdout)
        1. [Demonstrate URL to Kafka](#demonstrate-url-to-kafka)
1. [Developing](#developing)
    1. [Build docker image for development](#build-docker-image-for-development)

## Using Command Line

### Prerequisite software

The following software programs need to be installed.

1. YUM-based installs - For Red Hat, CentOS, openSuse and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based).

    ```console
    sudo yum -y install epel-release
    sudo yum -y install git
    ```

1. APT-based installs - For Ubuntu and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based)

    ```console
    sudo apt update
    sudo apt -y install git
    ```

### Set environment variables

1. These variables may be modified, but do not need to be modified.
   The variables are used throughout the installation procedure.

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=mock-data-generator
    export DOCKER_IMAGE_TAG=senzing/mock-data-generator
    ```

1. Synthesize environment variables.

    ```console
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    export GIT_REPOSITORY_URL="https://github.com/${GIT_ACCOUNT}/${GIT_REPOSITORY}.git"
    ```

1. Set environment variables described in "[Configuration](#configuration)".

### Clone repository

1. Get repository.

    ```console
    mkdir --parents ${GIT_ACCOUNT_DIR}
    cd  ${GIT_ACCOUNT_DIR}
    git clone ${GIT_REPOSITORY_URL}
    ```

### Install

1. YUM installs - For Red Hat, CentOS, openSuse and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based).

    ```console
    sudo xargs yum -y install < ${GIT_REPOSITORY_DIR}/src/yum-packages.txt
    ```

1. APT installs - For Ubuntu and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based)

    ```console
    sudo xargs apt -y install < ${GIT_REPOSITORY_DIR}/src/apt-packages.txt
    ```

1. PIP installs

    ```console
    sudo pip install -r ${GIT_REPOSITORY_DIR}/requirements.txt
    ```

### Demonstrate

1. Show help.  Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./mock-data-generator.py --help
    ./mock-data-generator.py random-to-stdout --help
    ```

1. Show random file output.  Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./mock-data-generator.py random-to-stdout
    ```

1. Show random file output with 1 record per second.  Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./mock-data-generator.py random-to-stdout \
      --records-per-second 1
    ```

1. Show repeatable "random" output using random seed.  Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./mock-data-generator.py random-to-stdout \
      --random-seed 1
    ```

1. Show generating 10 (repeatable) random records at the rate of 2 per second. Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./mock-data-generator.py random-to-stdout \
      --random-seed 22 \
      --record-min 1 \
      --record-max 10 \
      --records-per-second 2
    ```

1. Show sending output to a file of JSON-lines. Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./mock-data-generator.py random-to-stdout \
      --random-seed 22 \
      --record-min 1 \
      --record-max 10 \
      --records-per-second 2 \
      > output-file.jsonlines
    ```

1. Show reading 5 records from URL-based file at the rate of 3 per second. Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./mock-data-generator.py url-to-stdout \
      --input-url https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json \
      --record-min 1 \
      --record-max 5 \
      --records-per-second 3
    ```

## Using Docker

### Build docker image

1. Build docker image.

    ```console
    sudo docker build --tag senzing/mock-data-generator https://github.com/senzing/mock-data-generator.git
    ```

### Configuration

- `SENZING_SUBCOMMAND` - Identify the subcommand to be run. See `mock-data-generator.py --help` for complete list.

1. To determine which configuration parameters are use for each `<subcommand>`, run:

    ```console
    ./mock-data-generator.py <subcommand> --help
    ```

    - `SENZING_DEBUG` - Print debug statements to log.
    - `SENZING_INPUT_URL` - URL of source file. Default: [https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json](https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json)
    - `SENZING_KAFKA_BOOTSTRAP_SERVER` - Hostname and port of Kafka server.  Default: "localhost")
    - `SENZING_KAFKA_TOPIC` - Kafka topic. Default: "senzing-kafka-topic"
    - `SENZING_RANDOM_SEED` - Identify seed for random number generator. Value of 0 uses system clock. Values greater than 0 give repeatable results. Default: "0"
    - `SENZING_RECORD_MAX` - Identify highest record number to generate. Value of 0 means no maximum. Default: "0"
    - `SENZING_RECORD_MIN` - Identify lowest record number to generate. Default: "1"
    - `SENZING_RECORD_MONITOR` - Write a log record every N mock records. Default: "10000"
    - `SENZING_RECORDS_PER_SECOND` - Throttle output to a specified records per second. Value of 0 means no throttling. Default: "0"

### Run docker image

#### Demonstrate random to STDOUT

1. Run the docker container. Example:

    ```console
    export SENZING_SUBCOMMAND=random-to-stdout
    export SENZING_RANDOM_SEED=0
    export SENZING_RECORD_MIN=1
    export SENZING_RECORD_MAX=10
    export SENZING_RECORDS_PER_SECOND=0

    sudo docker run -it  \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_RANDOM_SEED="${SENZING_RANDOM_SEED}" \
      --env SENZING_RECORD_MIN="${SENZING_RECORD_MIN}" \
      --env SENZING_RECORD_MAX="${SENZING_RECORD_MAX}" \
      --env SENZING_RECORDS_PER_SECOND="${SENZING_RECORDS_PER_SECOND}" \
      senzing/mock-data-generator
    ```

#### Demonstrate random to Kafka

1. Run [docker-compose-stream-loader-demo](https://github.com/senzing/docker-compose-stream-loader-demo)

1. Identify the Docker network.
   Example:

    ```console
    docker network ls

    # Choose value from NAME column of docker network ls
    export SENZING_NETWORK=nameofthe_network
    ```

1. Run the docker container.  Example:

    ```console
    export SENZING_SUBCOMMAND=random-to-kafka

    export SENZING_KAFKA_BOOTSTRAP_SERVER=senzing-kafka:9092
    export SENZING_KAFKA_TOPIC="senzing-kafka-topic"
    export SENZING_NETWORK=senzingdockercomposestreamloaderdemo_backend
    export SENZING_RANDOM_SEED=1
    export SENZING_RECORD_MAX=220
    export SENZING_RECORD_MIN=210
    export SENZING_RECORDS_PER_SECOND=1

    sudo docker run -it  \
      --net ${SENZING_NETWORK} \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_KAFKA_BOOTSTRAP_SERVER=${SENZING_KAFKA_BOOTSTRAP_SERVER} \
      --env SENZING_KAFKA_TOPIC=${SENZING_KAFKA_TOPIC} \
      --env SENZING_RANDOM_SEED="${SENZING_RANDOM_SEED}" \
      --env SENZING_RECORD_MAX="${SENZING_RECORD_MAX}" \
      --env SENZING_RECORD_MIN="${SENZING_RECORD_MIN}" \
      --env SENZING_RECORDS_PER_SECOND="${SENZING_RECORDS_PER_SECOND}" \
      senzing/mock-data-generator
    ```

#### Demonstrate URL to STDOUT

1. Run the docker container. Example:

    ```console
    export SENZING_SUBCOMMAND=url-to-stdout

    export SENZING_INPUT_URL=https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json
    export SENZING_RECORD_MAX=240
    export SENZING_RECORD_MIN=250
    export SENZING_RECORDS_PER_SECOND=0

    sudo docker run -it  \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_INPUT_URL=${SENZING_INPUT_URL} \
      --env SENZING_RECORD_MAX="${SENZING_RECORD_MAX}" \
      --env SENZING_RECORD_MIN="${SENZING_RECORD_MIN}" \
      --env SENZING_RECORDS_PER_SECOND="${SENZING_RECORDS_PER_SECOND}" \
      senzing/mock-data-generator
    ```

#### Demonstrate URL to Kafka

1. Run [docker-compose-stream-loader-demo](https://github.com/senzing/docker-compose-stream-loader-demo)

1. Identify the Docker network.
   Example:

    ```console
    docker network ls

    # Choose value from NAME column of docker network ls
    export SENZING_NETWORK=nameofthe_network
    ```

1. Run the docker container.  Example:

    ```console
    export SENZING_SUBCOMMAND=url-to-kafka

    export SENZING_INPUT_URL=https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json
    export SENZING_KAFKA_BOOTSTRAP_SERVER=senzing-kafka:9092
    export SENZING_KAFKA_TOPIC="senzing-kafka-topic"
    export SENZING_NETWORK=senzingdockercomposestreamloaderdemo_backend
    export SENZING_RECORD_MAX=260
    export SENZING_RECORD_MIN=300
    export SENZING_RECORD_MONITOR=10
    export SENZING_RECORDS_PER_SECOND=10

    sudo docker run -it  \
      --net ${SENZING_NETWORK} \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_INPUT_URL=${SENZING_INPUT_URL} \
      --env SENZING_KAFKA_BOOTSTRAP_SERVER=${SENZING_KAFKA_BOOTSTRAP_SERVER} \
      --env SENZING_KAFKA_TOPIC=${SENZING_KAFKA_TOPIC} \
      --env SENZING_RECORD_MAX="${SENZING_RECORD_MAX}" \
      --env SENZING_RECORD_MIN="${SENZING_RECORD_MIN}" \
      --env SENZING_RECORD_MONITOR="${SENZING_RECORD_MONITOR}" \
      --env SENZING_RECORDS_PER_SECOND="${SENZING_RECORDS_PER_SECOND}" \
      senzing/mock-data-generator
    ```

## Developing

### Build docker image for development

1. See if docker is already installed.

    ```console
    sudo docker --version
    ```

1. If needed, install Docker.  See [HOWTO - Install Docker](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-docker.md)

1. Option #1 - Using make command

    ```console
    cd ${GIT_REPOSITORY_DIR}
    sudo make docker-build
    ```

1. Option #2 - Using docker command

    ```console
    cd ${GIT_REPOSITORY_DIR}
    sudo docker build --tag ${DOCKER_IMAGE_TAG} .
    ```
