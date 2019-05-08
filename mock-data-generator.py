#! /usr/bin/env python

# -----------------------------------------------------------------------------
# mock-data-generator.py  Create mock data for Senzing.
# -----------------------------------------------------------------------------

# Special case for monkey patching

from gevent import monkey
monkey.patch_all()

# Imports

import argparse
import collections
from confluent_kafka import Producer, KafkaException
from datetime import datetime
import gevent
import json
import logging
import os
import pika
import random
import requests
import signal
import sys
import time

# Python 2 / 3 migration.

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

__all__ = []
__version__ = 1.0
__date__ = '2018-12-03'
__updated__ = '2019-05-08'

SENZING_PRODUCT_ID = "5002"  # Used in log messages for format ppppnnnn, where "p" is product and "n" is error in product.
log_format = '%(asctime)s %(message)s'

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

configuration_locator = {
    "data_source": {
        "default": None,
        "env": "SENZING_DATA_SOURCE",
        "cli": "data-source",
    },
    "data_template": {
        "default": {
            "DATA_SOURCE": "PEOPLE",
            "ENTITY_TYPE": "PEOPLE",
            "NAMES": [{
                "NAME_TYPE": "PRIMARY",
                "NAME_LAST": "last_name",
                "NAME_FIRST": "first_name",
              },
            ],
            "GENDER": "gender",
            "DATE_OF_BIRTH": "date_of_birth",
            "ADDRESSES": [{
                "ADDR_TYPE": "HOME",
                "ADDR_LINE1": "address_street",
                "ADDR_CITY": "address_city",
                "ADDR_STATE": "address_state",
                "ADDR_POSTAL_CODE": "address_zipcode",
             }]
        },
        "env": "SENZING_DATA_TEMPLATE",
        "cli": "data-template",
    },
    "docker_launched": {
        "default": False,
        "env": "SENZING_DOCKER_LAUNCHED",
        "cli": "docker-launched"
    },
    "entity_type": {
        "default": None,
        "env": "SENZING_ENTITY_TYPE",
        "cli": "entity-type"
    },
    "http_request_url": {
        "default": "http://localhost:5001",
        "env": "SENZING_HTTP_REQUEST_URL",
        "cli": "http-request-url",
    },
    "input_url": {
        "default": "https://s3.amazonaws.com/public-read-access/TestDataSets/loadtest-dataset-1M.json",
        "env": "SENZING_INPUT_URL",
        "cli": "input-url",
    },
    "kafka_bootstrap_server": {
        "default": "localhost:9092",
        "env": "SENZING_KAFKA_BOOTSTRAP_SERVER",
        "cli": "kafka-bootstrap-server",
    },
    "kafka_topic": {
        "default": "senzing-kafka-topic",
        "env": "SENZING_KAFKA_TOPIC",
        "cli": "kafka-topic",
    },
    "rabbitmq_host": {
        "default": "localhost:5672",
        "env": "SENZING_RABBITMQ_HOST",
        "cli": "rabbitmq-host",
    },
    "rabbitmq_queue": {
        "default": "senzing-rabbitmq-queue",
        "env": "SENZING_RABBITMQ_QUEUE",
        "cli": "rabbitmq-queue",
    },
    "rabbitmq_username": {
        "default": "user",
        "env": "SENZING_RABBITMQ_USERNAME",
        "cli": "rabbitmq-username",
    },
    "rabbitmq_password": {
        "default": "bitnami",
        "env": "SENZING_RABBITMQ_PASSWORD",
        "cli": "rabbitmq-password",
    },
    "random_seed": {
        "default": "0",
        "env": "SENZING_RANDOM_SEED",
        "cli": "random-seed",
    },
    "record_max": {
        "default": "0",
        "env": "SENZING_RECORD_MAX",
        "cli": "record-max",
    },
    "record_min": {
        "default": "1",
        "env": "SENZING_RECORD_MIN",
        "cli": "record-min",
    },
    "record_monitor": {
        "default": "10000",
        "env": "SENZING_RECORD_MONITOR",
        "cli": "record-monitor",
    },
    "records_per_second": {
        "default": "0",
        "env": "SENZING_RECORDS_PER_SECOND",
        "cli": "records-per-second",
    },
    "simulated_clients": {
        "default": "10",
        "env": "SENZING_SIMULATED_CLIENTS",
        "cli": "simulated-clients",
    },
    "sleep_time": {
        "default": 0,
        "env": "SENZING_SLEEP_TIME",
        "cli": "sleep-time"
    },
    "subcommand": {
        "default": None,
        "env": "SENZING_SUBCOMMAND",
    }
}

# -----------------------------------------------------------------------------
# Mock data
# -----------------------------------------------------------------------------

address_city_suffix = [
    " Beach", "boro", "borough", "burg", " City", " Creek", "dale", " Falls",
    "ford", " Forest", " Hill", " Lakes", "land", "mont", "more", " Springs",
    " Station", "ton", "town", " Valley", "ville", " Vista",
]

address_state_abbreviations = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

address_street_suffix = [
    "Avenue", "Ave.", "Boulevard", "Blvd.", "Court", "Ct.", "Drive", "Dr.",
    "Lane", "Parkway", "Plaza", "Street", "St.", "Road", "Rd.", "Trail", "Way",
]

gender = [
    "M", "F",
]

last_names = [
    "Adams", "Alexander", "Allen", "Anderson", "Bailey", "Baker", "Barnes",
    "Bell", "Bennett", "Brooks", "Brown", "Bryant", "Butler", "Campbell",
    "Carter", "Clark", "Coleman", "Collins", "Cook", "Cooper", "Cox", "Davis",
    "Diaz", "Edwards", "Evans", "Flores", "Foster", "Garcia", "Gonzales",
    "Gonzalez", "Gray", "Green", "Griffin", "Hall", "Harris", "Hayes",
    "Henderson", "Hernandez", "Hill", "Howard", "Hughes", "Jackson", "James",
    "Jenkins", "Johnson", "Jones", "Kelly", "King", "Lee", "Lewis", "Long",
    "Lopez", "Martin", "Martinez", "Miller", "Mitchell", "Moore", "Morgan",
    "Morris", "Murphy", "Nelson", "Parker", "Patterson", "Perez", "Perry",
    "Peterson", "Phillips", "Powell", "Price", "Ramirez", "Reed", "Richardson",
    "Rivera", "Roberts", "Robinson", "Rodriguez", "Rogers", "Ross", "Russell",
    "Sanchez", "Sanders", "Scott", "Simmons", "Smith", "Stewart", "Taylor",
    "Thomas", "Thompson", "Torres", "Turner", "Walker", "Ward", "Washington",
    "Watson", "White", "Williams", "Wilson", "Wood", "Wright", "Young",
]

first_names = [
    "Aaron", "Abigail", "Adam", "Alan", "Albert", "Alexander", "Alexis",
    "Alice", "Amanda", "Amber", "Amy", "Andrea", "Andrew", "Angela", "Ann",
    "Anna", "Anthony", "Arthur", "Ashley", "Austin", "Barbara", "Benjamin",
    "Betty", "Beverly", "Billy", "Bobby", "Brandon", "Brenda", "Brian",
    "Brittany", "Bruce", "Bryan", "Carl", "Carol", "Carolyn", "Catherine",
    "Charles", "Cheryl", "Christian", "Christina", "Christine", "Christopher",
    "Cynthia", "Daniel", "Danielle", "David", "Deborah", "Debra", "Denise",
    "Dennis", "Diana", "Diane", "Donald", "Donna", "Doris", "Dorothy",
    "Douglas", "Dylan", "Edward", "Elizabeth", "Emily", "Emma", "Eric",
    "Ethan", "Eugene", "Evelyn", "Frances", "Frank", "Gabriel", "Gary",
    "George", "Gerald", "Gloria", "Grace", "Gregory", "Hannah", "Harold",
    "Harry", "Heather", "Helen", "Henry", "Jack", "Jacob", "Jacqueline",
    "James", "Jane", "Janet", "Janice", "Jason", "Jean", "Jeffrey", "Jennifer",
    "Jeremy", "Jerry", "Jesse", "Jessica", "Joan", "Joe", "John", "Johnny",
    "Jonathan", "Jordan", "Jose", "Joseph", "Joshua", "Joyce", "Juan", "Judith",
    "Judy", "Julia", "Julie", "Justin", "Karen", "Katherine", "Kathleen",
    "Kathryn", "Kayla", "Keith", "Kelly", "Kenneth", "Kevin", "Kimberly",
    "Kyle", "Larry", "Laura", "Lauren", "Lawrence", "Linda", "Lisa", "Logan",
    "Lori", "Louis", "Madison", "Margaret", "Maria", "Marie", "Marilyn", "Mark",
    "Martha", "Mary", "Matthew", "Megan", "Melissa", "Michael", "Michelle",
    "Nancy", "Natalie", "Nathan", "Nicholas", "Nicole", "Noah", "Olivia",
    "Pamela", "Patricia", "Patrick", "Paul", "Peter", "Philip", "Rachel",
    "Ralph", "Randy", "Raymond", "Rebecca", "Richard", "Robert", "Roger",
    "Ronald", "Rose", "Roy", "Russell", "Ruth", "Ryan", "Samantha", "Samuel",
    "Sandra", "Sara", "Sarah", "Scott", "Sean", "Sharon", "Shirley", "Stephanie",
    "Stephen", "Steven", "Susan", "Teresa", "Terry", "Theresa", "Thomas",
    "Tiffany", "Timothy", "Tyler", "Victoria", "Vincent", "Virginia", "Walter",
    "Wayne", "William", "Willie", "Zachary", ]

# -----------------------------------------------------------------------------
# Define argument parser
# -----------------------------------------------------------------------------


def get_parser():
    '''Parse commandline arguments.'''
    parser = argparse.ArgumentParser(prog="mock-data-generator.py", description="Generate mock data from a URL-addressable file or templated random data. For more information, see https://github.com/Senzing/mock-data-generator")
    subparsers = parser.add_subparsers(dest='subcommand', help='Subcommands (SENZING_SUBCOMMAND):')

    subparser_0 = subparsers.add_parser('version', help='Print version of mock-data-generator.py.')

    subparser_1 = subparsers.add_parser('random-to-stdout', help='Send random data to STDOUT')
    subparser_1.add_argument("--data-source", dest="data_source", metavar="SENZING_DATA_SOURCE", help="Used when JSON line does not have a `DATA_SOURCE` key.")
    subparser_1.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_1.add_argument("--entity-type", dest="entity_type", metavar="SENZING_ENTITY_TYPE", help="Used when JSON line does not have a `ENTITY_TYPE` key.")
    subparser_1.add_argument("--random-seed", dest="random_seed", metavar="SENZING_RANDOM_SEED", help="Change random seed. Default: 0")
    subparser_1.add_argument("--record-min", dest="record_min", metavar="SENZING_RECORD_MIN", help="Lowest record id. Default: 1")
    subparser_1.add_argument("--record-max", dest="record_max", metavar="SENZING_RECORD_MAX", help="Highest record id. Default: 10")
    subparser_1.add_argument("--records-per-second", dest="records_per_second", metavar="SENZING_RECORDS_PER_SECOND", help="Number of record produced per second. Default: 0")

#     subparser_2 = subparsers.add_parser('random-to-http', help='Send random data via HTTP request')
#     subparser_2.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
#     subparser_2.add_argument("--http-request-url", dest="http_request_url", metavar="SENZING_HTTP_REQUEST_URL", help="Senzing REST API service.")
#     subparser_2.add_argument("--random-seed", dest="random_seed", metavar="SENZING_RANDOM_SEED", help="Change random seed. Default: 0")
#     subparser_2.add_argument("--record-min", dest="record_min", metavar="SENZING_RECORD_MIN", help="Lowest record id. Default: 1")
#     subparser_2.add_argument("--record-max", dest="record_max", metavar="SENZING_RECORD_MAX", help="Highest record id. Default: 10")
#     subparser_2.add_argument("--records-per-second", dest="records_per_second", metavar="SENZING_RECORDS_PER_SECOND", help="Number of record produced per second. Default: 0")

    subparser_3 = subparsers.add_parser('random-to-kafka', help='Send random data to Kafka')
    subparser_3.add_argument("--data-source", dest="data_source", metavar="SENZING_DATA_SOURCE", help="Used when JSON line does not have a `DATA_SOURCE` key.")
    subparser_3.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_3.add_argument("--entity-type", dest="entity_type", metavar="SENZING_ENTITY_TYPE", help="Used when JSON line does not have a `ENTITY_TYPE` key.")
    subparser_3.add_argument("--kafka-bootstrap-server", dest="kafka_bootstrap_server", metavar="SENZING_KAFKA_BOOTSTRAP_SERVER", help="Kafka bootstrap server. Default: localhost:9092")
    subparser_3.add_argument("--kafka-topic", dest="kafka_topic", metavar="SENZING_KAFKA_TOPIC", help="Kafka topic. Default: senzing-kafka-topic")
    subparser_3.add_argument("--random-seed", dest="random_seed", metavar="SENZING_RANDOM_SEED", help="Change random seed. Default: 0")
    subparser_3.add_argument("--record-min", dest="record_min", metavar="SENZING_RECORD_MIN", help="Lowest record id. Default: 1")
    subparser_3.add_argument("--record-max", dest="record_max", metavar="SENZING_RECORD_MAX", help="Highest record id. Default: 10")
    subparser_3.add_argument("--records-per-second", dest="records_per_second", metavar="SENZING_RECORDS_PER_SECOND", help="Number of record produced per second. Default: 0")

    subparser_4 = subparsers.add_parser('url-to-stdout', help='Send HTTP or file data to STDOUT')
    subparser_4.add_argument("--data-source", dest="data_source", metavar="SENZING_DATA_SOURCE", help="Used when JSON line does not have a `DATA_SOURCE` key.")
    subparser_4.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. Default: False (SENZING_DEBUG)")
    subparser_4.add_argument("--entity-type", dest="entity_type", metavar="SENZING_ENTITY_TYPE", help="Used when JSON line does not have a `ENTITY_TYPE` key.")
    subparser_4.add_argument("--input-url", dest="input_url", metavar="SENZING_INPUT_URL", help="File/URL to read.")
    subparser_4.add_argument("--record-min", dest="record_min", metavar="SENZING_RECORD_MIN", help="Lowest record id. Default: 1 ")
    subparser_4.add_argument("--record-max", dest="record_max", metavar="SENZING_RECORD_MAX", help="Highest record id. Default: 10 ")
    subparser_4.add_argument("--records-per-second", dest="records_per_second", metavar="SENZING_RECORDS_PER_SECOND", help="Number of record produced per second. Default: 0 ")

#     subparser_5 = subparsers.add_parser('url-to-http', help='Send HTTP / file data via HTTP request')
#     subparser_5.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
#     subparser_5.add_argument("--http-request-url", dest="http_request_url", metavar="SENZING_HTTP_REQUEST_URL", help="Senzing REST API service.")
#     subparser_5.add_argument("--input-url", dest="input_url", metavar="SENZING_INPUT_URL", help="File/URL to read.")
#     subparser_5.add_argument("--records-per-second", dest="records_per_second", metavar="SENZING_RECORDS_PER_SECOND", help="Number of record produced per second. Default: 0")

    subparser_6 = subparsers.add_parser('url-to-kafka', help='Send HTTP or file data to Kafka')
    subparser_6.add_argument("--data-source", dest="data_source", metavar="SENZING_DATA_SOURCE", help="Used when JSON line does not have a `DATA_SOURCE` key.")
    subparser_6.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_6.add_argument("--entity-type", dest="entity_type", metavar="SENZING_ENTITY_TYPE", help="Used when JSON line does not have a `ENTITY_TYPE` key.")
    subparser_6.add_argument("--input-url", dest="input_url", metavar="SENZING_INPUT_URL", help="File/URL to read.")
    subparser_6.add_argument("--kafka-bootstrap-server", dest="kafka_bootstrap_server", metavar="SENZING_KAFKA_BOOTSTRAP_SERVER", help="Kafka bootstrap server. Default: localhost:9092")
    subparser_6.add_argument("--kafka-topic", dest="kafka_topic", metavar="SENZING_KAFKA_TOPIC", help="Kafka topic. Default: senzing-kafka-topic")
    subparser_6.add_argument("--record-min", dest="record_min", metavar="SENZING_RECORD_MIN", help="Lowest record id. Default: 1")
    subparser_6.add_argument("--record-max", dest="record_max", metavar="SENZING_RECORD_MAX", help="Highest record id. Default: 10")
    subparser_6.add_argument("--records-per-second", dest="records_per_second", metavar="SENZING_RECORDS_PER_SECOND", help="Number of record produced per second. Default: 0")

    subparser_7 = subparsers.add_parser('random-to-rabbitmq', help='Send random data to RabbitMQ')
    subparser_7.add_argument("--data-source", dest="data_source", metavar="SENZING_DATA_SOURCE", help="Used when JSON line does not have a `DATA_SOURCE` key.")
    subparser_7.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_7.add_argument("--entity-type", dest="entity_type", metavar="SENZING_ENTITY_TYPE", help="Used when JSON line does not have a `ENTITY_TYPE` key.")
    subparser_7.add_argument("--rabbitmq-host", dest="rabbitmq_host", metavar="SENZING_RABBITMQ_HOST", help="RabbitMQ host. Default: localhost:5672")
    subparser_7.add_argument("--rabbitmq-queue", dest="rabbitmq_queue", metavar="SENZING_RABBITMQ_QUEUE", help="RabbitMQ queue. Default: senzing-rabbitmq-queue")
    subparser_7.add_argument("--rabbitmq-username", dest="rabbitmq_username", metavar="SENZING_RABBITMQ_USERNAME", help="RabbitMQ username. Default: user")
    subparser_7.add_argument("--rabbitmq-password", dest="rabbitmq_password", metavar="SENZING_RABBITMQ_PASSWORD", help="RabbitMQ password. Default: bitnami")
    subparser_7.add_argument("--random-seed", dest="random_seed", metavar="SENZING_RANDOM_SEED", help="Change random seed. Default: 0")
    subparser_7.add_argument("--record-min", dest="record_min", metavar="SENZING_RECORD_MIN", help="Lowest record id. Default: 1")
    subparser_7.add_argument("--record-max", dest="record_max", metavar="SENZING_RECORD_MAX", help="Highest record id. Default: 10")
    subparser_7.add_argument("--records-per-second", dest="records_per_second", metavar="SENZING_RECORDS_PER_SECOND", help="Number of record produced per second. Default: 0")

    subparser_8 = subparsers.add_parser('url-to-rabbitmq', help='Send HTTP or file data to RabbitMQ')
    subparser_8.add_argument("--data-source", dest="data_source", metavar="SENZING_DATA_SOURCE", help="Used when JSON line does not have a `DATA_SOURCE` key.")
    subparser_8.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_8.add_argument("--entity-type", dest="entity_type", metavar="SENZING_ENTITY_TYPE", help="Used when JSON line does not have a `ENTITY_TYPE` key.")
    subparser_8.add_argument("--input-url", dest="input_url", metavar="SENZING_INPUT_URL", help="File/URL to read.")
    subparser_8.add_argument("--rabbitmq-host", dest="rabbitmq_host", metavar="SENZING_RABBITMQ_HOST", help="RabbitMQ host. Default: localhost:5672")
    subparser_8.add_argument("--rabbitmq-queue", dest="rabbitmq_queue", metavar="SENZING_RABBITMQ_QUEUE", help="RabbitMQ queue. Default: senzing-rabbitmq-queue")
    subparser_8.add_argument("--rabbitmq-username", dest="rabbitmq_username", metavar="SENZING_RABBITMQ_USERNAME", help="RabbitMQ username. Default: user")
    subparser_8.add_argument("--rabbitmq-password", dest="rabbitmq_password", metavar="SENZING_RABBITMQ_PASSWORD", help="RabbitMQ password. Default: bitnami")
    subparser_8.add_argument("--record-min", dest="record_min", metavar="SENZING_RECORD_MIN", help="Lowest record id. Default: 1")
    subparser_8.add_argument("--record-max", dest="record_max", metavar="SENZING_RECORD_MAX", help="Highest record id. Default: 10")
    subparser_8.add_argument("--records-per-second", dest="records_per_second", metavar="SENZING_RECORDS_PER_SECOND", help="Number of record produced per second. Default: 0")

    subparser_9 = subparsers.add_parser('sleep', help='Do nothing but sleep. For Docker testing.')
    subparser_9.add_argument("--sleep-time", dest="sleep_time", metavar="SENZING_SLEEP_TIME", help="Sleep time in seconds. DEFAULT: 0 (infinite)")

    subparser_10 = subparsers.add_parser('docker-acceptance-test', help='For Docker acceptance testing.')

    return parser

# -----------------------------------------------------------------------------
# Message handling
# -----------------------------------------------------------------------------

# 1xx Informational (i.e. logging.info())
# 2xx Warning (i.e. logging.warn())
# 4xx User configuration issues (i.e. logging.err() for Client errors)
# 5xx Internal error (i.e. logging.error for Server errors)
# 9xx Debugging (i.e. logging.debug())


message_dictionary = {
    "100": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}I",
    "101": "Enter {0}",
    "102": "Exit {0}",
    "103": "Kafka topic: {0}; message: {1}; error: {2}; error: {3}",
    "104": "Records sent to Kafka: {0}",
    "105": "Records sent via HTTP POST: {0}",
    "106": "Records sent to RabbitMQ: {0}",
    "128": "Sleeping {0} seconds.",
    "131": "Sleeping infinitely.",
    "197": "Version: {0}  Updated: {1}",
    "198": "For information on warnings and errors, see https://github.com/Senzing/mock-data-generator#errors",
    "199": "{0}",
    "200": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "400": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "401": "Bad JSON template: {0}.",
    "403": "Bad file protocol in --input-file-name: {0}.",
    "404": "Buffer error: {0} for line #{1} '{2}'.",
    "405": "Kafka error: {0} for line #{1} '{2}'.",
    "406": "Not implemented error: {0} for line #{1} '{2}'.",
    "407": "Unknown kafka error: {0} for line #{1} '{2}'.",
    "408": "Kafka topic: {0}; message: {1}; error: {2}; error: {3}",
    "409": "SENZING_SIMULATED_CLIENTS cannot be 0.",
    "410": "Unknown RabbitMQ error when connecting: {0}.",
    "411": "Unknown RabbitMQ error when adding record to queue: {0} for line {1}.",
    "412": "Could not connect to RabbitMQ host at {1}. The host name maybe wrong, it may not be ready, or your credentials are incorrect. See the RabbitMQ log for more details.",
    "498": "Bad SENZING_SUBCOMMAND: {0}.",
    "499": "No processing done.",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "599": "Program terminated with error.",
    "900": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}D",
    "999": "{0}",
}


def message(index, *args):
    index_string = str(index)
    template = message_dictionary.get(index_string, "No message for index {0}.".format(index_string))
    return template.format(*args)


def message_generic(generic_index, index, *args):
    index_string = str(index)
    return "{0} {1}".format(message(generic_index, index), message(index, *args))


def message_info(index, *args):
    return message_generic(100, index, *args)


def message_warn(index, *args):
    return message_generic(200, index, *args)


def message_error(index, *args):
    return message_generic(500, index, *args)


def message_debug(index, *args):
    return message_generic(900, index, *args)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------


def get_configuration(args):
    ''' Order of precedence: CLI, OS environment variables, INI file, default.'''
    result = {}

    # Copy default values into configuration dictionary.

    for key, value in configuration_locator.items():
        result[key] = value.get('default', None)

    # Special case: Change default data_template to string.

    result["data_template"] = json.dumps(configuration_locator.get("data_template", {}).get("default", {}), sort_keys=True)

    # "Prime the pump" with command line args. This will be done again as the last step.

    for key, value in args.__dict__.items():
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Copy OS environment variables into configuration dictionary.

    for key, value in configuration_locator.items():
        os_env_var = value.get('env', None)
        if os_env_var:
            os_env_value = os.getenv(os_env_var, None)
            if os_env_value:
                result[key] = os_env_value

    # Copy 'args' into configuration dictionary.

    for key, value in args.__dict__.items():
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Special case: subcommand from command-line

    if args.subcommand:
        result['subcommand'] = args.subcommand

    # Special case: Change boolean strings to booleans.

    booleans = ['debug',
                'docker_launched']
    for boolean in booleans:
        boolean_value = result.get(boolean)
        if isinstance(boolean_value, str):
            boolean_value_lower_case = boolean_value.lower()
            if boolean_value_lower_case in ['true', '1', 't', 'y', 'yes']:
                result[boolean] = True
            else:
                result[boolean] = False

    # Special case: Change integer strings to integers.

    integers = ['random_seed',
                'record_max',
                'record_min',
                'record_monitor',
                'records_per_second',
                'simulated_clients',
                'sleep_time']
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    # Special case: DATA_SOURCE and ENTITY_TYPE

    if result["data_source"] is not None or result["entity_type"] is not None:
        data_template_dictionary = json.loads(result["data_template"])
        if result["data_source"]:
            data_template_dictionary["DATA_SOURCE"] = result["data_source"]
        if result["entity_type"]:
            data_template_dictionary["ENTITY_TYPE"] = result["entity_type"]
        result["data_template"] = json.dumps(data_template_dictionary, sort_keys=True)

    return result


def validate_configuration(config):
    '''Check aggregate configuration from commandline options, environment variables, config files, and defaults.'''

    user_warning_messages = []
    user_error_messages = []

    try:
        template_dictionary = json.loads(config.get('data_template'))
    except:
        user_error_messages.append(message_warn(401, config.get('data_template')))

    if config.get('simulated_clients', 0) == 0:
        user_error_messages.append(message_error(409))

    # Log warning messages.

    for user_warning_message in user_warning_messages:
        logging.warn(user_warning_message)

    # Log error messages.

    for user_error_message in user_error_messages:
        logging.error(user_error_message)

    # Log where to go for help.

    if len(user_warning_messages) > 0 or len(user_error_messages) > 0:
        logging.info(message_info(198))

    # If there are error messages, exit.

    if len(user_error_messages) > 0:
        exit_error(499)

# -----------------------------------------------------------------------------
# Create functions
# -----------------------------------------------------------------------------


def create_http_request_function(url, headers):
    '''Tricky code.  Uses currying technique. Create a function for signal handling.
       that knows about "args".
    '''

    def result_function(session, line):
        result = session.post(url, headers=headers, data=line)
        logging.info("{0}".format(result.text))
        return result

    return result_function


def create_line_reader_file_function(input_url, data_source, entity_type):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from a file.
    '''

    def result_function():
        counter = 0
        with open(input_url) as input_file:
            for line in input_file:
                counter += 1
                yield transform_line(line, data_source, entity_type, counter)

    return result_function


def create_line_reader_file_function_max(input_url, data_source, entity_type, max):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from a file.
    '''

    def result_function():
        counter = 0
        with open(input_url) as input_file:
            for line in input_file:
                counter += 1
                if counter > max:
                    break
                yield transform_line(line, data_source, entity_type, counter)

    return result_function


def create_line_reader_file_function_min(input_url, data_source, entity_type, min):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from a file.
    '''

    def result_function():
        counter = min - 1
        if counter < 0:
            counter = 0
        with open(input_url) as input_file:
            for line in input_file:
                counter += 1
                if counter >= min:
                    yield transform_line(line, data_source, entity_type, counter)

    return result_function


def create_line_reader_file_function_min_max(input_url, data_source, entity_type, min, max):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from a file.
    '''

    def result_function():
        counter = min - 1
        if counter < 0:
            counter = 0
        with open(input_url) as input_file:
            for line in input_file:
                counter += 1
                if counter > max:
                    break
                if counter >= min:
                    yield transform_line(line, data_source, entity_type, counter)

    return result_function


def create_line_reader_http_function(input_url, data_source, entity_type):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from HTTP response.
    '''

    def result_function():
        counter = 0
        data = urlopen(input_url)
        for line in data:
            counter += 1
            yield transform_line(line, data_source, entity_type, counter)

    return result_function


def create_line_reader_http_function_max(input_url, data_source, entity_type, max):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from HTTP response.
    '''

    def result_function():
        counter = 0
        data = urlopen(input_url)
        for line in data:
            counter += 1
            if counter > max:
                break
            yield transform_line(line, data_source, entity_type, counter)

    return result_function


def create_line_reader_http_function_min(input_url, data_source, entity_type, min):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from HTTP response.
    '''

    def result_function():
        counter = min - 1
        if counter < 0:
            counter = 0
        data = urlopen(input_url)
        for line in data:
            counter += 1
            if counter >= min:
                yield transform_line(line, data_source, entity_type, counter)

    return result_function


def create_line_reader_http_function_min_max(input_url, data_source, entity_type, min, max):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from HTTP response.
    '''

    def result_function():
        counter = min - 1
        if counter < 0:
            counter = 0
        data = urlopen(input_url)
        for line in data:
            counter += 1
            if counter > max:
                break
            if counter >= min:
                yield transform_line(line, data_source, entity_type, counter)

    return result_function


def create_signal_handler_function(args):
    '''Tricky code.  Uses currying technique. Create a function for signal handling.
       that knows about "args".
    '''

    def result_function(signal_number, frame):
        logging.info(message_info(102, args))
        sys.exit(0)

    return result_function


def bootstrap_signal_handler(signal, frame):
    sys.exit(0)

# -----------------------------------------------------------------------------
# Entry / Exit utility functions
# -----------------------------------------------------------------------------


def entry_template(config):
    '''Format of entry message.'''
    config['start_time'] = time.time()
    config_json = json.dumps(config, sort_keys=True)
    return message_info(101, config_json)


def exit_template(config):
    '''Format of exit message.'''
    stop_time = time.time()
    config['stop_time'] = stop_time
    config['elapsed_time'] = stop_time - config.get('start_time', stop_time)
    config_json = json.dumps(config, sort_keys=True)
    return message_info(102, config_json)


def exit_error(index, *args):
    '''Log error message and exit program.'''
    logging.error(message_error(index, *args))
    logging.error(message_error(599))
    sys.exit(1)


def exit_silently():
    '''Exit program.'''
    sys.exit(1)

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------


def sleep(counter, records_per_second, last_time):
    result_counter = counter + 1
    if (records_per_second > 0) and (result_counter % records_per_second == 0):
        sleep_time = 1.0 - (time.time() - last_time)
        if sleep_time > 0:
            time.sleep(sleep_time)
    return result_counter, time.time()


def create_url_reader_factory(input_url, data_source, entity_type, min, max):
    result = None
    parsed_file_name = urlparse(input_url)
    if parsed_file_name.scheme in ['http', 'https']:
        if min > 0 and max > 0:
            result = create_line_reader_http_function_min_max(input_url, data_source, entity_type, min, max)
        elif min > 0:
            result = create_line_reader_http_function_min(input_url, data_source, entity_type, min)
        elif max > 0:
            result = create_line_reader_http_function_max(input_url, data_source, entity_type, max)
        else:
            result = create_line_reader_http_function(input_url, data_source, entity_type)
    elif parsed_file_name.scheme in ['file', '']:
        if min > 0 and max > 0:
            result = create_line_reader_file_function_min_max(parsed_file_name.path, data_source, entity_type, min, max)
        elif min > 0:
            result = create_line_reader_file_function_min(parsed_file_name.path, data_source, entity_type, min)
        elif max > 0:
            result = create_line_reader_file_function_max(parsed_file_name.path, data_source, entity_type, max)
        else:
            result = create_line_reader_file_function(parsed_file_name.path, data_source, entity_type)
    return result


def transform_line(line, data_source, entity_type, counter):
    line_dictionary = json.loads(line)
    if data_source is not None and 'DATA_SOURCE' not in line_dictionary:
        line_dictionary['DATA_SOURCE'] = str(data_source)
    if entity_type is not None and 'ENTITY_TYPE' not in line_dictionary:
        line_dictionary['ENTITY_TYPE'] = str(entity_type)
    if counter is not None and 'RECORD_ID' not in line_dictionary:
        line_dictionary['RECORD_ID'] = str(counter)
    return json.dumps(line_dictionary, sort_keys=True)


def range_with_infinity(min_value, max_value):
    '''Create an index generator where max_value of 0 gives infinity.'''
    if max_value <= 0:
        index = min_value
        while True:
            index += 1
            yield index
    else:
        max_value_final = max_value + 1
        for index in range(min_value, max_value):
            yield index

# -----------------------------------------------------------------------------
# Kafka support
# -----------------------------------------------------------------------------


def on_kafka_delivery(error, message):
    message_topic = message.topic()
    message_value = message.value()
    message_error = message.error()
    logging.debug(message_debug(103, message_topic, message_value, message_error, error))
    if error is not None:
        logging.warn(message_warn(408, message_topic, message_value, message_error, error))

# -----------------------------------------------------------------------------
# generate functions
#   Common function signature: generate_XXX()
#   These functions return a random string value.
# -----------------------------------------------------------------------------


def generate_address_city():
    """Create city name (with suffix)."""
    suffix = address_city_suffix[random.randint(0, len(address_city_suffix) - 1)]
    return "{0}{1}".format(last_names[random.randint(0, len(last_names) - 1)], suffix)


def generate_address_state():
    """Pull random state abbreviations out of list."""
    return address_state_abbreviations[random.randint(0, len(address_state_abbreviations) - 1)]


def generate_address_street():
    """Concatenate number, street, and street sufix."""
    number = random.randint(1, 9999)
    street = last_names[random.randint(0, len(last_names) - 1)]
    suffix = address_street_suffix[random.randint(0, len(address_street_suffix) - 1)]
    return "{0} {1} {2}".format(number, street, suffix)


def generate_address_zipcode():
    """Create random 5-digit number."""
    return "{0:05d}".format(random.randint(0, 99999))


def generate_date_of_birth():
    """Create random Year, Month, day."""
    year = random.randint(1950, 2018)
    month = random.randint(1, 12)
    max_day = 28
    if month in [1, 3, 5, 7, 8, 10, 12]:
        max_day = 31
    elif month in [4, 6, 9, 11]:
        max_day = 30
    day = random.randint(1, max_day)
    result = datetime(year, month, day)
    return "{0}".format(result.strftime('%m/%d/%Y'))


def generate_first_name():
    """Pull random first name out of list."""
    return first_names[random.randint(0, len(first_names) - 1)]


def generate_gender():
    """Pull random gender out of list."""
    return gender[random.randint(0, len(gender) - 1)]


def generate_last_name():
    """Pull random last name out of list."""
    return last_names[random.randint(0, len(last_names) - 1)]

# -----------------------------------------------------------------------------
# generate functions
# -----------------------------------------------------------------------------


def generate_values(template_dictionary):
    """A function used to recursively descend into the template_dictionary.
       At each "leaf", try to call a function to generate a "random" variable.
    """

    result = {}
    for key, value in template_dictionary.items():

        # Handle dictionaries recursively.

        if isinstance(value, collections.Mapping):
            result[key] = generate_values(value)

        # Handle lists serially.

        elif isinstance(value, list):
            result[key] = []
            for list_element in value:
                result[key].append(generate_values(list_element))

        # Handle leafs.  Try calling "generate_XXXXX" functions.

        else:
            try:

                # Tricky code for calling function based on string.

                function_name = "generate_{0}".format(value)
                result[key] = globals()[function_name]()

            # If no function exists, simply copy the value.

            except:
                result[key] = value
    return result


def generate_json_strings(data_template, min, max, seed=0):
    """A generator that yields JSON strings."""

    # Pseudo-random results can be recreated with the same seed.

    if seed > 0:
        random.seed(seed)

    #  Loop to yield JSON strings.

    for record_id in range_with_infinity(min, max):
        result = generate_values(json.loads(data_template))
        result["RECORD_ID"] = str(record_id)
        yield json.dumps(result, sort_keys=True)

# -----------------------------------------------------------------------------
# do_* functions
#   Common function signature: do_XXX(args)
# -----------------------------------------------------------------------------


def do_docker_acceptance_test(args):
    '''Sleep.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Epilog.

    logging.info(exit_template(config))


def do_random_to_http(args):
    '''Write random data via HTTP POST request.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    data_template = config.get("data_template")
    http_request_url = config.get("http_request_url")
    min = config.get("record_min")
    max = config.get("record_max")
    record_monitor = config.get("record_monitor")
    records_per_second = config.get("records_per_second")
    seed = config.get("random_seed")
    simulated_clients = config.get("simulated_clients")

    # Synthesize variables

    monitor_period = record_monitor
    if monitor_period <= 0:
        monitor_period = configuration_locator.get('record_monitor', {}).get('default', 10000)

    if simulated_clients <= 0:
        simulated_clients = configuration_locator.get('simulated_clients', {}).get('default', 10)

    # Parameters for HTTP request.

    url = "{0}/records".format(http_request_url)
    headers = {
        "Content-Type": "application/json"
    }

    http_request_function = create_http_request_function(url, headers)

    # Make HTTP persistent connections which simulate separate clients.

    sessions = []
    for simulated_client in range(0, simulated_clients):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3, pool_block=True)
        session.mount('http://', adapter)
        sessions.append(session)

    # Make HTTP requests distributed among sessions/simulated-clients.

    http_workers = []
    last_time = time.time()
    counter = 1
    for line in generate_json_strings(data_template, min, max, seed):
        session = sessions[counter % simulated_clients]
        http_workers.append(gevent.spawn(http_request_function, session, line))

        # Periodic activities.

        if counter % monitor_period == 0:
            logging.info(message_debug(105, counter))

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Wait for all processing to be done.

    gevent.joinall(http_workers)

    # Get statistics.

    stats_url = "{0}/stats".format(http_request_url)
    result = requests.get(stats_url, headers=headers)
    logging.info(result.text)

    # Close all HTTP sessions.

    for session in sessions:
        session.close()

    # Epilog.

    logging.info(exit_template(config))


def do_random_to_kafka(args):
    '''Write random data via HTTP POST request.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    data_template = config.get("data_template")
    kafka_bootstrap_server = config.get("kafka_bootstrap_server")
    kafka_topic = config.get("kafka_topic")
    min = config.get("record_min")
    max = config.get("record_max")
    seed = config.get("random_seed")
    record_monitor = config.get("record_monitor")
    records_per_second = config.get("records_per_second")

    # Synthesize variables

    monitor_period = record_monitor
    if monitor_period <= 0:
        monitor_period = configuration_locator.get('record_monitor', {}).get('default', 10000)

    # Kafka configuration.

    kafka_producer_configuration = {
        'bootstrap.servers': kafka_bootstrap_server
    }
    kafka_producer = Producer(kafka_producer_configuration)

    # Make Kafka requests.

    last_time = time.time()
    counter = 1
    for line in generate_json_strings(data_template, min, max, seed):
        try:
            kafka_producer.produce(kafka_topic, line, on_delivery=on_kafka_delivery)
        except BufferError as err:
            logging.warn(message_warn(404, err, counter, line))
        except KafkaException as err:
            logging.warn(message_warn(405, err, counter, line))
        except NotImplemented as err:
            logging.warn(message_warn(406, err, counter, line))
        except:
            logging.warn(message_warn(407, err, counter, line))

        # Periodic activities.

        if counter % monitor_period == 0:
            logging.info(message_debug(104, counter))
            kafka_producer.poll(0)

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Wait until all Kafka messages are sent.

    kafka_producer.flush()

    # Epilog.

    logging.info(exit_template(config))


def do_random_to_rabbitmq(args):
    '''Write random data via rabbitmq.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    data_template = config.get("data_template")
    min = config.get("record_min")
    max = config.get("record_max")
    seed = config.get("random_seed")
    record_monitor = config.get("record_monitor")
    records_per_second = config.get("records_per_second")
    rabbitmq_host = config.get("rabbitmq_host")
    rabbitmq_queue = config.get("rabbitmq_queue")
    rabbitmq_username = config.get("rabbitmq_username")
    rabbitmq_password = config.get("rabbitmq_password")

    # Synthesize variables

    monitor_period = record_monitor
    if monitor_period <= 0:
        monitor_period = configuration_locator.get('record_monitor', {}).get('default', 10000)

    # Connect to the RabbitMQ host

    try:
        credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=rabbitmq_queue)
    except (pika.exceptions.AMQPConnectionError) as err:
        exit_error(412, err, rabbitmq_host)
    except BaseException as err:
        exit_error(410, err)

    # Load RabbitMQ

    last_time = time.time()
    counter = 1
    for line in generate_json_strings(data_template, min, max, seed):
        try:
            channel.basic_publish(exchange='',
                                  routing_key=rabbitmq_queue,
                                  body=line,
                                  properties=pika.BasicProperties(
                                    delivery_mode=1))  # make message non-persistent
        except BaseException as err:
            logging.warn(message_warn(411, err, line))

        # Periodic activities.

        if counter % monitor_period == 0:
            logging.info(message_debug(104, counter))

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Close our end of the connection

    connection.close()

    # Epilog.

    logging.info(exit_template(config))


def do_random_to_stdout(args):
    '''Write random data to STDOUT.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    data_template = config.get("data_template", "{}")
    max = config.get("record_max", 10)
    min = config.get("record_min", 1)
    records_per_second = config.get("records_per_second", 0)
    seed = config.get("random_seed", 0)

    # Print line to STDOUT.

    last_time = time.time()
    counter = 1
    for line in generate_json_strings(data_template, min, max, seed):
        print(line)

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Epilog.

    logging.info(exit_template(config))


def do_sleep(args):
    '''Sleep.  Used for debugging.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    sleep_time = config.get('sleep_time')

    # Sleep

    if sleep_time > 0:
        logging.info(message_info(128, sleep_time))
        time.sleep(sleep_time)

    else:
        sleep_time = 3600
        while True:
            logging.info(message_info(131))
            time.sleep(sleep_time)

    # Epilog.

    logging.info(exit_template(config))


def do_url_to_http(args):
    '''Write random data via HTTP POST request.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    data_source = config.get("data_source")
    data_template = config.get("data_template")
    entity_type = config.get("entity_type")
    http_request_url = config.get("http_request_url")
    min = config.get("record_min")
    max = config.get("record_max")
    record_monitor = config.get("record_monitor")
    records_per_second = config.get("records_per_second")
    simulated_clients = config.get("simulated_clients")

    # Synthesize variables

    monitor_period = record_monitor
    if monitor_period <= 0:
        monitor_period = configuration_locator.get('record_monitor', {}).get('default', 10000)

    if simulated_clients <= 0:
        simulated_clients = configuration_locator.get('simulated_clients', {}).get('default', 10)

    # Parameters for HTTP request.

    url = "{0}/records".format(http_request_url)
    headers = {
        "Content-Type": "application/json"
    }

    http_request_function = create_http_request_function(url, headers)

    # Make HTTP persistent connections which simulate separate clients.

    sessions = []
    for simulated_client in range(0, simulated_clients):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3, pool_block=True)
        session.mount('http://', adapter)
        sessions.append(session)

    # Construct line reader.

    line_reader = create_url_reader_factory(input_url, data_source, entity_type, min, max)
    if not line_reader:
        exit_error(403, input_url)

    # Make HTTP requests distributed among sessions/simulated-clients.

    http_workers = []
    last_time = time.time()
    counter = 1
    for line in line_reader():
        session = sessions[counter % simulated_clients]
        http_workers.append(gevent.spawn(http_request_function, session, line))

        # Periodic activities.

        if counter % monitor_period == 0:
            logging.info(message_debug(105, counter))

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Wait for all processing to be done.

    gevent.joinall(http_workers)

    # Get statistics.

    stats_url = "{0}/stats".format(http_request_url)
    result = requests.get(stats_url, headers=headers)
    logging.info(result.text)

    # Close all HTTP sessions.

    for session in sessions:
        session.close()

    # Epilog.

    logging.info(exit_template(config))


def do_url_to_kafka(args):
    '''Write random data via HTTP POST request.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    data_source = config.get("data_source")
    entity_type = config.get("entity_type")
    input_url = config.get("input_url")
    kafka_bootstrap_server = config.get("kafka_bootstrap_server")
    kafka_topic = config.get("kafka_topic")
    min = config.get("record_min")
    max = config.get("record_max")
    record_monitor = config.get("record_monitor")
    records_per_second = config.get("records_per_second")

    # Synthesize variables

    monitor_period = record_monitor
    if monitor_period <= 0:
        monitor_period = configuration_locator.get('record_monitor', {}).get('default', 10000)

    # Kafka producer configuration.

    kafka_producer_configuration = {
        'bootstrap.servers': kafka_bootstrap_server
    }
    kafka_producer = Producer(kafka_producer_configuration)

    # Construct line reader.

    line_reader = create_url_reader_factory(input_url, data_source, entity_type, min, max)
    if not line_reader:
        exit_error(403, input_url)

    # Make Kafka requests.

    last_time = time.time()
    counter = 1
    for line in line_reader():
        try:
            kafka_producer.produce(kafka_topic, line, on_delivery=on_kafka_delivery)
        except BufferError as err:
            logging.warn(message_warn(404, err, counter, line))
        except KafkaException as err:
            logging.warn(message_warn(405, err, counter, line))
        except NotImplemented as err:
            logging.warn(message_warn(406, err, counter, line))
        except:
            logging.warn(message_warn(407, err, counter, line))

        # Periodic activities.

        if counter % monitor_period == 0:
            logging.info(message_debug(104, counter))
            kafka_producer.poll(0)

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Wait until all Kafka messages are delivered.

    kafka_producer.flush()

    # Epilog.

    logging.info(exit_template(config))


def do_url_to_rabbitmq(args):
    '''Write URL data to RabbitMQ.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    data_source = config.get("data_source")
    entity_type = config.get("entity_type")
    input_url = config.get("input_url")
    rabbitmq_host = config.get("rabbitmq_host")
    rabbitmq_queue = config.get("rabbitmq_queue")
    rabbitmq_username = config.get("rabbitmq_username")
    rabbitmq_password = config.get("rabbitmq_password")
    min = config.get("record_min")
    max = config.get("record_max")
    record_monitor = config.get("record_monitor")
    records_per_second = config.get("records_per_second")

    # Synthesize variables

    monitor_period = record_monitor
    if monitor_period <= 0:
        monitor_period = configuration_locator.get('record_monitor', {}).get('default', 10000)

    # Connect to the RabbitMQ host

    try:
        credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=rabbitmq_queue)
    except (pika.exceptions.AMQPConnectionError) as err:
        exit_error(412, err, rabbitmq_host)
    except BaseException as err:
        exit_error(410, err)

    # Construct line reader.

    line_reader = create_url_reader_factory(input_url, data_source, entity_type, min, max)
    if not line_reader:
        exit_error(403, input_url)

    # Load RabbitMQ

    last_time = time.time()
    counter = 1
    for line in line_reader():
        try:
            channel.basic_publish(exchange='',
                                  routing_key=rabbitmq_queue,
                                  body=line,
                                  properties=pika.BasicProperties(
                                    delivery_mode=2))  # make message persistent
        except BaseException as err:
            logging.warn(message_warn(411, err, line))

        # Periodic activities.

        if counter % monitor_period == 0:
            logging.info(message_debug(106, counter))

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Close our end of the connection

    connection.close()

    # Epilog.

    logging.info(exit_template(config))


def do_url_to_stdout(args):
    '''Write random data to STDOUT.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    data_source = config.get("data_source")
    entity_type = config.get("entity_type")
    input_url = config.get("input_url")
    min = config.get("record_min")
    max = config.get("record_max")
    records_per_second = config.get("records_per_second")

    # Construct line reader.

    line_reader = create_url_reader_factory(input_url, data_source, entity_type, min, max)
    if not line_reader:
        exit_error(403, input_url)

    # Print line to STDOUT.

    last_time = time.time()
    counter = 1
    for line in line_reader():
        print(line)

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Epilog.

    logging.info(exit_template(config))


def do_version(args):
    '''Log version information.'''

    logging.info(message_info(197, __version__, __updated__))

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


if __name__ == "__main__":

    # Configure logging. See https://docs.python.org/2/library/logging.html#levels

    log_level_map = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "fatal": logging.FATAL,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
    log_level = log_level_map.get(log_level_parameter, logging.INFO)
    logging.basicConfig(format=log_format, level=log_level)

    # Trap signals temporarily until args are parsed.

    signal.signal(signal.SIGTERM, bootstrap_signal_handler)
    signal.signal(signal.SIGINT, bootstrap_signal_handler)

    # Parse the command line arguments.

    subcommand = os.getenv("SENZING_SUBCOMMAND", None)
    parser = get_parser()
    if len(sys.argv) > 1:
        args = parser.parse_args()
        subcommand = args.subcommand
    elif subcommand:
        args = argparse.Namespace(subcommand=subcommand)
    else:
        parser.print_help()
        if len(os.getenv("SENZING_DOCKER_LAUNCHED", "")):
            subcommand = "sleep"
            args = argparse.Namespace(subcommand=subcommand)
            do_sleep(args)
        exit_silently()

    # Catch interrupts. Tricky code: Uses currying.

    signal_handler = create_signal_handler_function(args)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Transform subcommand from CLI parameter to function name string.

    subcommand_function_name = "do_{0}".format(subcommand.replace('-', '_'))

    # Test to see if function exists in the code.

    if subcommand_function_name not in globals():
        logging.warn(message_warn(498, subcommand))
        parser.print_help()
        exit_silently()

    # Tricky code for calling function based on string.

    globals()[subcommand_function_name](args)

