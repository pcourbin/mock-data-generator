#! /usr/bin/env python

# -----------------------------------------------------------------------------
# mock-data-generator.py  Create mock data for Senzing.
# -----------------------------------------------------------------------------

from datetime import datetime
from gevent import monkey
from confluent_kafka import Producer, KafkaException
from urlparse import urlparse
import argparse
import collections
import gevent
import json
import logging
import os
import random
import requests
import signal
import sys
import time
import urllib2

monkey.patch_all()

__all__ = []
__version__ = 1.0
__date__ = '2018-12-03'
__updated__ = '2018-12-11'

SENZING_PRODUCT_ID = "5105"  # Used in log messages for format ppppnnnn, where "p" is product and "n" is error in product.
log_format = '%(asctime)s %(message)s'

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

configuration_locator = {
    "data_source": {
        "default": "PEOPLE",
        "env": "SENZING_DATA_SOURCE",
        "cli": "data-source",
    },
    "data_template": {
        "default": {
            "DATA_SOURCE": "",
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
    parser = argparse.ArgumentParser(prog="mock-data-generator.py", description="Generate mock data from a URL-addressable file or templated random data. For more information see https://github.com/Senzing/mock-data-generator")
    subparsers = parser.add_subparsers(dest='subcommand', help='Subcommands (SENZING_SUBCOMMAND):')

    subparser_1 = subparsers.add_parser('random-to-stdout', help='Send random data to STDOUT')
    subparser_1.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. Default: False (SENZING_DEBUG)")
    subparser_1.add_argument("--random-seed", dest="random_seed", help="Change random seed. Default: 0 (SENZING_RANDOM_SEED)")
    subparser_1.add_argument("--record-min", dest="record_min", help="Lowest record id. Default: 1 (SENZING_RECORD_MIN)")
    subparser_1.add_argument("--record-max", dest="record_max", help="Highest record id. Default: 10 (SENZING_RECORD_MAX)")
    subparser_1.add_argument("--records-per-second", dest="records_per_second", help="Number of record produced per second. Default: 0 (SENZING_RECORDS_PER_SECOND)")

#     subparser_2 = subparsers.add_parser('random-to-http', help='Send random data via HTTP request')
#     subparser_2.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. Default: False (SENZING_DEBUG)")
#     subparser_2.add_argument("--http-request-url", dest="http_request_url", help="Senzing REST API service. (SENZING_HTTP_REQUEST_URL)")
#     subparser_2.add_argument("--random-seed", dest="random_seed", help="Change random seed. Default: 0 (SENZING_RANDOM_SEED)")
#     subparser_2.add_argument("--record-min", dest="record_min", help="Lowest record id. Default: 1 (SENZING_RECORD_MIN)")
#     subparser_2.add_argument("--record-max", dest="record_max", help="Highest record id. Default: 10 (SENZING_RECORD_MAX)")
#     subparser_2.add_argument("--records-per-second", dest="records_per_second", help="Number of record produced per second. Default: 0 (SENZING_RECORDS_PER_SECOND)")

    subparser_3 = subparsers.add_parser('random-to-kafka', help='Send random data to Kafka')
    subparser_3.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. Default: False (SENZING_DEBUG)")
    subparser_3.add_argument("--kafka-bootstrap-server", dest="kafka_bootstrap_server", help="Kafka bootstrap server. Default: localhost:9092 (SENZING_KAFKA_BOOTSTRAP_SERVER)")
    subparser_3.add_argument("--kafka-topic", dest="kafka_topic", help="Kafka topic. Default: senzing-kafka-topic (SENZING_KAFKA_TOPIC)")
    subparser_3.add_argument("--random-seed", dest="random_seed", help="Change random seed. Default: 0 (SENZING_RANDOM_SEED)")
    subparser_3.add_argument("--record-min", dest="record_min", help="Lowest record id. Default: 1 (SENZING_RECORD_MIN)")
    subparser_3.add_argument("--record-max", dest="record_max", help="Highest record id. Default: 10 (SENZING_RECORD_MAX)")
    subparser_3.add_argument("--records-per-second", dest="records_per_second", help="Number of record produced per second. Default: 0 (SENZING_RECORDS_PER_SECOND)")

    subparser_4 = subparsers.add_parser('url-to-stdout', help='Send HTTP or file data to STDOUT')
    subparser_4.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. Default: False (SENZING_DEBUG)")
    subparser_4.add_argument("--input-url", dest="input_url", help="File/URL to read (SENZING_INPUT_URL)")
    subparser_4.add_argument("--record-min", dest="record_min", help="Lowest record id. Default: 1 (SENZING_RECORD_MIN)")
    subparser_4.add_argument("--record-max", dest="record_max", help="Highest record id. Default: 10 (SENZING_RECORD_MAX)")
    subparser_4.add_argument("--records-per-second", dest="records_per_second", help="Number of record produced per second. Default: 0 (SENZING_RECORDS_PER_SECOND)")

#     subparser_5 = subparsers.add_parser('url-to-http', help='Send HTTP / file data via HTTP request')
#     subparser_5.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. Default: False (SENZING_DEBUG)")
#     subparser_5.add_argument("--http-request-url", dest="http_request_url", help="Senzing REST API service. (SENZING_HTTP_REQUEST_URL)")
#     subparser_5.add_argument("--input-url", dest="input_url", help="File/URL to read (SENZING_INPUT_URL)")
#     subparser_5.add_argument("--records-per-second", dest="records_per_second", help="Number of record produced per second. Default: 0 (SENZING_RECORDS_PER_SECOND)")

    subparser_6 = subparsers.add_parser('url-to-kafka', help='Send HTTP or file data to Kafka')
    subparser_6.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. Default: False (SENZING_DEBUG)")
    subparser_6.add_argument("--input-url", dest="input_url", help="File/URL to read (SENZING_INPUT_URL)")
    subparser_6.add_argument("--kafka-bootstrap-server", dest="kafka_bootstrap_server", help="Kafka bootstrap server. Default: localhost:9092 (SENZING_KAFKA_BOOTSTRAP_SERVER)")
    subparser_6.add_argument("--kafka-topic", dest="kafka_topic", help="Kafka topic. Default: senzing-kafka-topic (SENZING_KAFKA_TOPIC)")
    subparser_6.add_argument("--record-min", dest="record_min", help="Lowest record id. Default: 1 (SENZING_RECORD_MIN)")
    subparser_6.add_argument("--record-max", dest="record_max", help="Highest record id. Default: 10 (SENZING_RECORD_MAX)")
    subparser_6.add_argument("--records-per-second", dest="records_per_second", help="Number of record produced per second. Default: 0 (SENZING_RECORDS_PER_SECOND)")

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
    "199": "{0}",
    "200": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "400": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "401": "Bad JSON template: {0}.",
    "402": "Bad SENZING_SUBCOMMAND: {0}.",
    "403": "Bad file protocol in --input-file-name: {0}.",
    "404": "Buffer error: {0} for line '{1}'.",
    "405": "Kafka error: {0} for line '{1}'.",
    "406": "Not implemented error: {0} for line '{1}'.",
    "407": "Unknown kafka error: {0} for line '{1}'.",
    "408": "Kafka topic: {0}; message: {1}; error: {2}; error: {3}",
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

    booleans = ['debug']
    for boolean in booleans:
        boolean_value = result.get(boolean)
        if isinstance(boolean_value, str):
            boolean_value_lower_case = boolean_value.lower()
            if boolean_value_lower_case in ['true', '1', 't', 'y', 'yes']:
                result[boolean] = True
            else:
                result[boolean] = False

    # Special case: Change integer strings to integers.

    integers = ['random_seed', 'record_max', 'record_min', 'record_monitor', 'records_per_second', 'simulated_clients']
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    # Special case: DATA_SOURCE

    data_template_dictionary = json.loads(result["data_template"])
    data_template_dictionary["DATA_SOURCE"] = result["data_source"]
    result["data_template"] = json.dumps(data_template_dictionary, sort_keys=True)

    return result


def validate_configuration(config):
    '''Check aggregate configuration from commandline options, environment variables, config files, and defaults.'''

    user_error_messages = []

    try:
        template_dictionary = json.loads(config.get('data_template'))
    except:
        user_error_messages.append(message_warn(401, config.get('data_template')))

    if len(user_error_messages) > 0:
        for user_error_message in user_error_messages:
            logging.error(user_error_message)
        exit_error(499)

# -----------------------------------------------------------------------------
# Create functions
# -----------------------------------------------------------------------------


def create_line_reader_file_function(input_url, data_source):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from a file.
    '''

    def result_function():
        counter = 0
        with open(input_url) as input_file:
            for line in input_file:
                counter += 1
                yield transform_line(line, data_source, counter)

    return result_function


def create_line_reader_file_function_min(input_url, data_source, min):
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
                    yield transform_line(line, data_source, counter)

    return result_function


def create_line_reader_file_function_max(input_url, data_source, max):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from a file.
    '''

    def result_function():
        counter = 0
        with open(input_url) as input_file:
            for line in input_file:
                counter += 1
                if counter > max:
                    break
                yield transform_line(line, data_source, counter)

    return result_function


def create_line_reader_file_function_min_max(input_url, data_source, min, max):
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
                    yield transform_line(line, data_source, counter)

    return result_function


def create_line_reader_http_function(input_url, data_source):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from HTTP response.
    '''

    def result_function():
        counter = 0
        data = urllib2.urlopen(input_url)
        for line in data:
            counter += 1
            yield transform_line(line, data_source, counter)

    return result_function


def create_line_reader_http_function_min(input_url, data_source, min):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from HTTP response.
    '''

    def result_function():
        counter = min - 1
        if counter < 0:
            counter = 0
        data = urllib2.urlopen(input_url)
        for line in data:
            counter += 1
            if counter >= min:
                yield transform_line(line, data_source, counter)

    return result_function


def create_line_reader_http_function_max(input_url, data_source, max):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from HTTP response.
    '''

    def result_function():
        counter = 0
        data = urllib2.urlopen(input_url)
        for line in data:
            counter += 1
            if counter > max:
                break
            yield transform_line(line, data_source, counter)

    return result_function


def create_line_reader_http_function_min_max(input_url, data_source, min, max):
    '''Tricky code.  Uses currying technique. Create a function for reading lines from HTTP response.
    '''

    def result_function():
        counter = min - 1
        if counter < 0:
            counter = 0
        data = urllib2.urlopen(input_url)
        for line in data:
            counter += 1
            if counter > max:
                break
            if counter >= min:
                yield transform_line(line, data_source, counter)

    return result_function


def create_http_request_function(url, headers):
    '''Tricky code.  Uses currying technique. Create a function for signal handling.
       that knows about "args".
    '''

    def result_function(session, line):
        result = session.post(url, headers=headers, data=line)
        logging.info("{0}".format(result.text))
        return result

    return result_function


def create_signal_handler_function(args):
    '''Tricky code.  Uses currying technique. Create a function for signal handling.
       that knows about "args".
    '''

    def result_function(signal_number, frame):
        logging.info(message_info(102, args))
        sys.exit(0)

    return result_function

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


def create_line_reader_factory(input_url, data_source, min, max):
    result = None
    parsed_file_name = urlparse(input_url)
    if parsed_file_name.scheme in ['http', 'https']:
        if min > 0 and max > 0:
            result = create_line_reader_http_function_min_max(input_url, data_source, min, max)
        elif min > 0:
            result = create_line_reader_http_function_min(input_url, data_source, min)
        elif max > 0:
            result = create_line_reader_http_function_max(input_url, data_source, max)
        else:
            result = create_line_reader_http_function(input_url, data_source)
    elif parsed_file_name.scheme in ['file', '']:
        if min > 0 and max > 0:
            result = create_line_reader_file_function_min_max(parsed_file_name.path, data_source, min, max)
        elif min > 0:
            result = create_line_reader_file_function_min(parsed_file_name.path, data_source, min)
        elif max > 0:
            result = create_line_reader_file_function_max(parsed_file_name.path, data_source, max)
        else:
            result = create_line_reader_file_function(parsed_file_name.path, data_source)
    return result


def transform_line(line, data_source, counter):
    line_dictionary = json.loads(line)
    if 'DATA_SOURCE' not in line_dictionary:
        line_dictionary['DATA_SOURCE'] = data_source
    if 'RECORD_ID' not in line_dictionary:
        line_dictionary['RECORD_ID'] = str(counter)
    return json.dumps(line_dictionary, sort_keys=True)

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

    record_max = max + 1
    for record_id in range(min, record_max):
        result = generate_values(json.loads(data_template))
        result["RECORD_ID"] = str(record_id)
        yield json.dumps(result, sort_keys=True)

# -----------------------------------------------------------------------------
# do_* functions
#   Common function signature: do_XXX(args)
# -----------------------------------------------------------------------------


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

        if counter % record_monitor == 0:
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
            logging.warn(message_warn(404, err, line))
        except KafkaException as err:
            logging.warn(message_warn(405, err, line))
        except NotImplemented as err:
            logging.warn(message_warn(406, err, line))
        except:
            logging.warn(message_warn(407, err, line))

        # Periodic activities.

        if counter % records_per_second == 0:
            kafka_producer.flush()
        if counter % record_monitor == 0:
            logging.info(message_debug(104, counter))

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Wait until all Kafka messages are sent.

    kafka_producer.flush()

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


def do_url_to_http(args):
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
    simulated_clients = config.get("simulated_clients")

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

    line_reader = create_line_reader_factory(input_url, data_source, min, max)
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

        if counter % record_monitor == 0:
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
    input_url = config.get("input_url")
    kafka_bootstrap_server = config.get("kafka_bootstrap_server")
    kafka_topic = config.get("kafka_topic")
    min = config.get("record_min")
    max = config.get("record_max")
    record_monitor = config.get("record_monitor")
    records_per_second = config.get("records_per_second")

    # Kafka producer configuration.

    kafka_producer_configuration = {
        'bootstrap.servers': kafka_bootstrap_server
    }
    kafka_producer = Producer(kafka_producer_configuration)

    # Construct line reader.

    line_reader = create_line_reader_factory(input_url, data_source, min, max)
    if not line_reader:
        exit_error(403, input_url)

    # Make Kafka requests.

    last_time = time.time()
    counter = 1
    for line in line_reader():
        try:
            kafka_producer.produce(kafka_topic, line, on_delivery=on_kafka_delivery)
        except BufferError as err:
            logging.warn(message_warn(404, err, line))
        except KafkaException as err:
            logging.warn(message_warn(405, err, line))
        except NotImplemented as err:
            logging.warn(message_warn(406, err, line))
        except:
            logging.warn(message_warn(407, err, line))

        # Periodic activities.

        if counter % records_per_second == 0:
            kafka_producer.flush()
        if counter % record_monitor == 0:
            logging.info(message_debug(104, counter))

        # Determine sleep time to create records_per_second.

        counter, last_time = sleep(counter, records_per_second, last_time)

    # Wait until all Kafka messages are sent.

    kafka_producer.flush()

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
    input_url = config.get("input_url")
    min = config.get("record_min")
    max = config.get("record_max")
    records_per_second = config.get("records_per_second")

    # Construct line reader.

    line_reader = create_line_reader_factory(input_url, data_source, min, max)
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

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


if __name__ == "__main__":

    # Configure logging.

    logging.basicConfig(format=log_format, level=logging.INFO)

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
        exit_silently()

    # Catch interrupts. Tricky code: Uses currying.

    signal_handler = create_signal_handler_function(args)
    signal.signal(signal.SIGINT, signal_handler)

    # Transform subcommand from CLI parameter to function name string.

    subcommand_function_name = "do_{0}".format(subcommand.replace('-', '_'))

    # Test to see if function exists in the code.

    if subcommand_function_name not in globals():
        logging.warn(message_warn(402, subcommand))
        parser.print_help()
        exit_silently()

    # Tricky code for calling function based on string.

    globals()[subcommand_function_name](args)
