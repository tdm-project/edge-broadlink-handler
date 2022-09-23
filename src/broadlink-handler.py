#!/usr/bin/env python
#
#  Copyright 2022, CRS4 - Center for Advanced Studies, Research and
#  Development in Sardinia
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
Edge Gateway Broadlink environment sensor's handler.
"""

import broadlink as bl
from broadlink.exceptions import NetworkTimeoutError, AuthenticationError
import click
from click_config_file import configuration_option, configobj_provider
from collections import namedtuple
from contextlib import contextmanager
import datetime
import json
import influxdb
from influxdb.exceptions import InfluxDBClientError
import logging
import paho.mqtt.publish as publish
from requests.exceptions import ConnectionError
import sys

import continuous_scheduler

APPLICATION_NAME = 'broadlink_handler'
logger = logging.getLogger(APPLICATION_NAME)


# Supresses 'requests' library default logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


ATTRIBUTES_MAP = {
    "temperature": ("temperature", None),
    "humidity": ("relativeHumidity", None),
}


TopicMap = namedtuple('TopicMap', ['measurement', 'sensor'])


TOPIC_LIST = {
    'RM4MINI': TopicMap('NotToSend', 'HTS2'),
    # 'RM4MINI': TopicMap('WeatherObserved', 'HTS2'),
}


@contextmanager
def influxdb_connection(host, port, username, password, database):
    _client = influxdb.InfluxDBClient(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database
    )

    try:
        _dbs = _client.get_list_database()
        if database not in [_d['name'] for _d in _dbs]:
            logger.info(
                "InfluxDB database '{:s}' not found. Creating a new one.".
                format(database))
            _client.create_database(database)

        yield _client
    finally:
        _client.close()


def cast_to_numeric(value):
    try:
        return float(value)
    except ValueError:
        if isinstance(value, str):
            if value.lower() in ['on', 'true']:
                return int(1)
            elif value.lower() in ['off', 'false']:
                return int(0)
        raise ValueError


def broadlink_task(userdata):
    _data_to_save = []
    _data_to_send = []

    for _device_ip in userdata['BROADLINK_DEVICES']:
        try:
            _device = bl.hello(_device_ip)
            _type = _device.type.upper()

            logger.info("Polling '%s' found - '%s' model ''%s'",
                        _device_ip, _device.manufacturer,
                        _device.model)

            if _type != "RM4MINI":
                logger.warning(
                    "Device '%s' '%s' '%s' has no sensors: skipping",
                    _device_ip, _device.manufacturer, _device.model)
                continue

            if _device.is_locked:
                logger.warning(
                    "Device '%s' '%s' '%s' is locked: cannot acquire data",
                    _device.manufacturer, _device.model, _device_ip)
                continue

            _device.auth()
            _mac = "".join(format(x, "02x") for x in _device.mac)
            _data = _device.check_sensors()
            _timestamp = int(datetime.datetime.now().timestamp())

            _json_data = {
                "measurement": TOPIC_LIST[_type].measurement,
                "tags": {
                    "node": _mac,
                    "model": _device.type,
                    "sensor": TOPIC_LIST[_type].sensor,
                },
                "time": _timestamp,
                "fields": {
                    ATTRIBUTES_MAP[_k][0]: cast_to_numeric(_v)
                    for _k, _v in _data.items()
                }
            }

            _mqtt_data = {
                "topic": f"{TOPIC_LIST[_type].measurement}/{_mac}."
                         f"{TOPIC_LIST[_type].sensor}",
                "qos": 0,
                "retain": False,
                "payload": {
                    "timestamp": _timestamp,
                    "dateObserved": datetime.datetime.fromtimestamp(
                        int(_timestamp),
                        tz=datetime.timezone.utc).isoformat(),
                }
            }
            _mqtt_data["payload"].update(_json_data["fields"])
            _mqtt_data["payload"] = json.dumps(_mqtt_data["payload"])

            _data_to_save.append(_json_data)
            _data_to_send.append(_mqtt_data)

        except NetworkTimeoutError:
            logger.warning("Sensor '%s' not responding", _device_ip)
            continue

        except AuthenticationError:
            logger.warning(
                "Device '%s' '%s' '%s' cannot authenticate: skipping",
                _device.manufacturer, _device.model,
                _device.host[0])
            continue

        _internal_broker_host = userdata['MQTT_LOCAL_HOST']
        _internal_broker_port = userdata['MQTT_LOCAL_PORT']
        _influxdb_host = userdata['INFLUXDB_HOST']
        _influxdb_port = userdata['INFLUXDB_PORT']
        _influxdb_user = userdata['INFLUXDB_USER']
        _influxdb_pass = userdata['INFLUXDB_PASS']
        _influxdb_dtbs = userdata['INFLUXDB_DB']

        try:
            with influxdb_connection(
                    _influxdb_host, _influxdb_port, _influxdb_user,
                    _influxdb_pass, _influxdb_dtbs) as _client:
                _client.write_points(_data_to_save, time_precision='s')
                logger.debug(
                    "Insert data into InfluxDB: %s", str(_data_to_save))
        except (InfluxDBClientError, ConnectionError) as iex:
            logger.error("Error in InfluxDB connection: %s", iex)

        try:
            logger.debug("Sending MQTT messages to %s:%d: '%s",
                         _internal_broker_host, _internal_broker_port,
                         _data_to_send)

            publish.multiple(_data_to_send, _internal_broker_host,
                             _internal_broker_port)
        except Exception as ex:
            logger.warning(ex)


@click.command()
@click.option("--logging-level", envvar="LOGGING_LEVEL",
              type=click.Choice(['DEBUG', 'INFO', 'WARNING',
                                 'ERROR', 'CRITICAL']), default='INFO')
@click.option('--mqtt-local-host', envvar='MQTT_LOCAL_HOST',
              type=str, default='localhost', show_default=True,
              show_envvar=True,
              help=('hostname or address of the local broker'))
@click.option('--mqtt-local-port', envvar='MQTT_LOCAL_PORT',
              type=int, default=1883, show_default=True, show_envvar=True,
              help=('port of the local broker'))
@click.option('--broadlink-devices', envvar='BROADLINK_DEVICES',
              type=str, default='localhost', show_default=True,
              show_envvar=True,
              help=('hostname or address of the Broadlink device'))
@click.option('--influxdb-host', envvar='INFLUXDB_HOST',
              type=str, default='localhost', show_default=True,
              show_envvar=True,
              help=('hostname or address of the influx database'))
@click.option('--influxdb-port', envvar='INFLUXDB_PORT',
              type=int, default=8086, show_default=True, show_envvar=True,
              help=('port of the influx database'))
@click.option('--influxdb-username', envvar='INFLUXDB_USER',
              type=str, default='', show_default=True, show_envvar=True,
              help=('user of the influx database'))
@click.option('--influxdb-password', envvar='INFLUXDB_PASS',
              type=str, default='', show_default=True, show_envvar=True,
              help=('password of the influx database'))
@click.option('--influxdb-database', envvar='INFLUXDB_DB',
              type=str, default='broadlink', show_default=True,
              show_envvar=True,
              help=('database inside the influx database'))
@click.option('--polling-interval', envvar='BROADLINK_POLLING_INTERVAL',
              type=int, default=60, show_default=True, show_envvar=True,
              help=('polling interval in seconds'))
@configuration_option("--config", "-c", provider=configobj_provider(
                        unrepr=False, section="BROADLINK"))
@click.pass_context
def broadlink_handler(ctx, mqtt_local_host: str, mqtt_local_port: int,
                      broadlink_devices: str,
                      influxdb_host: str, influxdb_port: int,
                      influxdb_username: str, influxdb_password: str,
                      influxdb_database: str, logging_level,
                      polling_interval: int) -> None:
    _level = getattr(logging, logging_level)
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        level=_level)

    # Checks the Python Interpeter version
    if sys.version_info < (3, 7):
        logger.fatal("This software requires Python version >= 3.7: exiting.")
        sys.exit(-1)

    logger.info("Starting %s", APPLICATION_NAME)
    logger.debug("loggin level set to %s", logging_level)
    logger.debug(ctx.params)

    _userdata = {
       'MQTT_LOCAL_HOST': mqtt_local_host,
       'MQTT_LOCAL_PORT': mqtt_local_port,
       'INFLUXDB_HOST': influxdb_host,
       'INFLUXDB_PORT': influxdb_port,
       'INFLUXDB_USER': influxdb_username,
       'INFLUXDB_PASS': influxdb_password,
       'INFLUXDB_DB': influxdb_database,
       'BROADLINK_DEVICES': broadlink_devices.split(',')
    }

    _main_scheduler = continuous_scheduler.MainScheduler()
    _main_scheduler.add_task(
        broadlink_task, 0, polling_interval, 0, _userdata)
    _main_scheduler.start()


if __name__ == "__main__":
    broadlink_handler()

# vim:ts=4:expandtab
