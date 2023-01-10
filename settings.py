from django.conf import settings

INFLUXDB_URL = getattr(settings, 'INFLUXDB_URL')
INFLUXDB_USER = getattr(settings, 'INFLUXDB_USER')
INFLUXDB_PASSWORD = getattr(settings, 'INFLUXDB_PASSWORD')
INFLUXDB_DATABASE = getattr(settings, 'INFLUXDB_DATABASE')
