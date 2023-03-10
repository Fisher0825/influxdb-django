"""Utilities for working with influxdb."""
import logging
from django.conf import settings
from threading import Thread

from influxdb import InfluxDBClient


logger = logging.getLogger(__name__)


def get_client():
    """Returns an ``InfluxDBClient`` instance."""
    return InfluxDBClient(
        settings.INFLUXDB_HOST,
        settings.INFLUXDB_PORT,
        settings.INFLUXDB_USER,
        settings.INFLUXDB_PASSWORD,
        settings.INFLUXDB_DATABASE,
        timeout=settings.INFLUXDB_TIMEOUT,
        ssl=getattr(settings, 'INFLUXDB_SSL', False),
        verify_ssl=getattr(settings, 'INFLUXDB_VERIFY_SSL', False),
    )


def query(query, **kwargs):
    """Wrapper around ``InfluxDBClient.query()``."""
    client = get_client()
    return client.query(query, kwargs)


def write_points(data, force_disable_threading=False, **kwargs):
    """
    Writes a series to influxdb.

    :param data: Array of dicts, as required by
      https://github.com/influxdb/influxdb-python
    :param force_disable_threading: When being called from the Celery task, we
      set this to `True` so that the user doesn't accidentally use Celery and
      threading at the same time.

    """
    if getattr(settings, 'INFLUXDB_DISABLED', False):
        return

    client = get_client()
    use_threading = getattr(settings, 'INFLUXDB_USE_THREADING', False)
    if force_disable_threading:
        use_threading = False
    if use_threading is True:
        thread = Thread(target=process_points, args=(client, data, kwargs))
        thread.start()
    else:
        process_points(client, data, kwargs)


def process_points(client, data, kwargs):  # pragma: no cover
    """Method to be called via threading module."""
    try:
        client.write_points(data, **kwargs)
    except Exception:
        if getattr(settings, 'INFLUXDB_FAIL_SILENTLY', True):
            logger.exception('Error while writing data points')
        else:
            raise


def drop_measurement(measurement):
    client = get_client()
    client.drop_measurement(measurement)


def drop_database(database):
    client = get_client()
    client.drop_measurement(database)


def async_exec(f):
    def wrapper(*args, **kwargs):
        thr = Thread(target = f, args = args, kwargs = kwargs)
        thr.start()
    return wrapper
