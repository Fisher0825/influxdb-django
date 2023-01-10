import json
import arrow
from datetime import datetime
from decimal import Decimal as D, InvalidOperation
from .helpers.utils import inv
from .exceptions import InfluxDBFieldValueError


class TimestampPrecision:
    HOURS = 'h'
    MICROSECONDS = 'u'
    MILLISECONDS = 'ms'
    MINUTES = 'm'
    NANOSECONDS = 'ns'
    SECONDS = 's'


TIMESTAMP_CONVERT_RATIO = {
    TimestampPrecision.HOURS: inv(60 * 60),
    TimestampPrecision.MICROSECONDS: 1 * 1000 * 1000,
    TimestampPrecision.MILLISECONDS: 1000,
    TimestampPrecision.MINUTES: inv(60),
    TimestampPrecision.NANOSECONDS: 1 * 1000 * 1000 * 1000,
    TimestampPrecision.SECONDS: 1,
}


class BaseField:
    def __init__(self, **kwargs):
        self._value = None
        self.raw_value = None
        self.field_name = kwargs.get('name', None)
        self.default = kwargs.get('default', None)
        self.enforce_cast = kwargs.get('enforce_cast', True)
        self.is_nullable = kwargs.get('is_nullable', True)
        self.validate_options()

    def clean(self, value):
        if value is None and self.default is not None:
            self._value = self.default
        elif value is None:
            self._value = None

    def clone(self):
        cls = self.__class__
        instance = cls(**self.__dict__)
        if self._value is not None:
            instance.set_internal_value(self._value)
        return instance

    def get_internal_value(self):
        return self._value

    def get_prep_value(self):
        prep_value = self.to_influx(self._value)
        return prep_value

    @property
    def name(self):
        return self.field_name

    def to_influx(self, value):
        return str(value)

    def to_python(self, value):
        return value

    def reset(self):
        self._value = None

    def set_internal_value(self, value):
        self.validate(value)
        self.raw_value = value
        if value is not None:
            try:
                self._value = self.to_python(value)
            except (InvalidOperation, ValueError) as exception:
                if self.enforce_cast:
                    raise exception
                self._value = value
        self.clean(value)

    def validate(self, value):
        if value is None and self.default is None and not self.is_nullable:
            raise InfluxDBFieldValueError('The field cannot be nullable')

    def validate_options(self):
        pass

    @property
    def value(self):
        return self.get_internal_value()


class GenericField(BaseField):
    pass


class IntegerField(GenericField):
    def __init__(self, **kwargs):
        self.min_value = kwargs.get('min_value', None)
        self.max_value = kwargs.get('max_value', None)
        super(IntegerField, self).__init__(**kwargs)

    def to_influx(self, value):
        # str_value = str(value)
        # return "{}i".format(str_value)
        return int(value)

    def to_python(self, value):
        return int(value)

    def validate(self, value):
        super(IntegerField, self).validate(value)
        if value is not None and self.min_value is not None \
           and self.to_python(value) < self.min_value:
            raise InfluxDBFieldValueError(
                'The value must be greater than the min_value'
            )
        if value is not None and self.max_value is not None \
           and self.to_python(value) > self.max_value:
            raise InfluxDBFieldValueError(
                'The value must be lower than the max_value'
            )

    def validate_options(self):
        super(IntegerField, self).validate_options()
        if self.min_value is not None \
           and not isinstance(self.min_value, int):
            raise InfluxDBFieldValueError(
                'min_value must be integer'
            )
        if self.max_value is not None \
           and not isinstance(self.max_value, int):
            raise InfluxDBFieldValueError(
                'max_value must be integer'
            )


class FloatField(IntegerField):
    def __init__(self, **kwargs):
        self.max_nb_decimals = kwargs.get('max_nb_decimals', None)
        super(FloatField, self).__init__(**kwargs)

    def clean(self, value):
        super(FloatField, self).clean(value)
        if self.max_nb_decimals is not None:
            precision = '.' + '0' * (self.max_nb_decimals - 1) + '1'
            self._value = self.to_python(value).quantize(D(precision))

    def to_influx(self, value):
        # str_value = str(value)
        # return str_value
        return float(value)

    def to_python(self, value):
        return D(value)

    def validate_options(self):
        super(FloatField, self).validate_options()
        if self.max_nb_decimals is not None \
           and not isinstance(self.max_nb_decimals, int):
            raise InfluxDBFieldValueError(
                'max_nb_decimals must be integer'
            )
        if self.max_nb_decimals is not None and self.max_nb_decimals <= 0:
            raise InfluxDBFieldValueError(
                'max_nb_decimals must be positive'
            )


class StringField(GenericField):
    def __init__(self, **kwargs):
        self.choices = kwargs.get('choices', None)
        self.max_length = kwargs.get('max_length', None)
        super(StringField, self).__init__(**kwargs)

    # def to_influx(self, value):
    #     str_value = str(value)
    #     return "\"{}\"".format(str_value)

    def to_python(self, value):
        return str(value)

    def validate(self, value):
        super(StringField, self).validate(value)
        if self.choices is not None and value not in self.choices:
            raise InfluxDBFieldValueError(
                'The value is not refered in choices'
            )
        if self.max_length is not None and len(str(value)) > self.max_length:
            raise InfluxDBFieldValueError(
                'the string length must be lower than the max_length'
            )

    def validate_options(self):
        super(StringField, self).validate_options()
        if self.choices is not None and not isinstance(self.choices, list):
            raise InfluxDBFieldValueError('choices must be a list')

        if self.choices is not None and not any(
            [isinstance(c, str) for c in self.choices]
        ):
            raise InfluxDBFieldValueError(
                'choices items must be a string'
            )

        if self.max_length is not None and\
           not isinstance(self.max_length, int):
            raise InfluxDBFieldValueError(
                'max_length must be integer'
            )

        if self.max_length is not None and self.max_length <= 0:
            raise InfluxDBFieldValueError(
                'max_length must be positive'
            )


class BooleanField(GenericField):
    def to_influx(self, value):
        # str_value = str(value).lower()
        # return str_value
        return bool(value)

    def to_python(self, value):
        return bool(value)


class TagField(BaseField):
    pass


class TimestampField(BaseField):
    def __init__(self, **kwargs):
        self.auto_now = kwargs.get('auto_now', True)
        self.precision = kwargs.get('precision', 'ns')
        super(TimestampField, self).__init__(**kwargs)

    def clean(self, value):
        super(TimestampField, self).clean(value)
        if value is None and self.auto_now:
            timestamp = arrow.now().timestamp
            self._value = self.to_python(timestamp)
            self.formatted_timestamp = self.convert_to_nanoseconds(timestamp)
        elif value:
            self.formatted_timestamp = self.convert_to_nanoseconds(value)
        else:
            self.formatted_timestamp = None

    def convert_to_nanoseconds(self, timestamp):
        precision = TimestampPrecision.NANOSECONDS
        return self.convert_to_precision(timestamp, precision)

    def convert_to_precision(self, timestamp, precision):
        convert_ratio = TIMESTAMP_CONVERT_RATIO[precision]
        return D(timestamp) * convert_ratio

    def to_influx(self, value):
        timestamp = D(self.formatted_timestamp)
        str_value = str(timestamp)
        # return "{}".format(str_value)
        return str_value

    def to_python(self, value):
        timestamp = D(value)
        return self.convert_to_precision(timestamp, self.precision)

    def validate_options(self):
        super(TimestampField, self).validate_options()
        if self.precision not in TIMESTAMP_CONVERT_RATIO:
            raise InfluxDBFieldValueError(
                'precision must be one of [ns,u,ms,s,m,h]'
            )


class DateTimeField(TimestampField):
    def __init__(self, **kwargs):
        self.str_format = kwargs.get('str_format', 'YYYY-MM-DD HH:mm:ss')
        super(DateTimeField, self).__init__(**kwargs)

    def clean(self, value):
        if value is None and self.auto_now:
            timestamp = arrow.now().datetime
            self._value = self.to_python(timestamp)
        elif value is None and self.default is not None:
            self._value = self.default
        elif value is None:
            self._value = None

    def get_internal_value(self):
        if self._value is None:
            return None
        return arrow.get(self._value).format(self.str_format)

    def to_influx(self, value):
        timestamp = arrow.get(value).timestamp
        nanoseconds = self.convert_to_nanoseconds(timestamp)
        str_value = str(nanoseconds)
        # return "{}".format(str_value)
        return str_value

    def to_python(self, value):
        if isinstance(value, datetime):
            return value
        dt = arrow.get(value, self.str_format).datetime
        return dt


class JsonStringField(BaseField):
    def __init__(self, **kwargs):
        self.json_string = kwargs.get('json_string')
        super(JsonStringField, self).__init__(**kwargs)

    def to_influx(self, value):
        return str(value)

    def to_python(self, value):
        try:
            return json.loads(value)
        except:
            return ''


class SerializerMethodField(BaseField):
    pass
