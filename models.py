import inspect

from django_cloudapp_common.influx.manager import Manager

from .fields import *
from .exceptions import InfluxDBFieldValueError

EXTENDED_FIELDS_PREFIX_NAME = '__fields__'


def _has_contribute_to_class(value):
    # Only call contribute_to_class() if it's bound.
    return not inspect.isclass(value) and hasattr(value, 'contribute_to_class')


class MeasurementMeta(type):
    def __init__(cls, name, *args, **kwargs):
        super(MeasurementMeta, cls).__init__(name, *args, **kwargs)
        field_names = cls._get_field_names()
        cls._extend_fields(field_names)

    def __new__(cls, name, bases, attrs, **kwargs):
        new_cls = super().__new__(cls, name, bases, attrs, **kwargs)
        objects = Manager()
        new_cls.add_to_class('objects', objects)
        return new_cls

    def add_to_class(cls, name, value):
        if _has_contribute_to_class(value):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)

    def __call__(cls, *args, **kwargs):
        setattr(cls, '_get_fields', cls._get_fields)
        instance = type.__call__(cls, *args, **kwargs)
        return instance

    def _get_field_names(cls):
        def filter_func(x):
            return isinstance(variables[x], BaseField)
        variables = cls.__dict__
        attribute_names = list(filter(filter_func, variables))
        return attribute_names

    def _get_fields(cls):
        def filter_func(x):
            return isinstance(x, BaseField)
        variables = cls.__dict__.values()
        attributes = list(filter(filter_func, variables))
        return attributes

    def _get_timestamp_attributes(cls):
        def filter_func(x):
            return isinstance(x, TimestampField)
        attributes = cls._get_attributes()
        timestamp_attributes = list(filter(filter_func, attributes))
        return timestamp_attributes

    def _extend_fields(cls, field_names):
        def generate_getter_and_setter(attr_name):
            def getx(self):
                _field = getattr(self, attr_name)
                return _field.get_internal_value()

            def setx(self, value):
                _field = getattr(self, attr_name)
                _field.set_internal_value(value)
            return getx, setx

        for field_name in field_names:
            ext_field_name = EXTENDED_FIELDS_PREFIX_NAME + field_name
            _field = getattr(cls, field_name)
            _field.field_name = field_name
            _field.ext_field_name = ext_field_name

            getx, setx = generate_getter_and_setter(ext_field_name)
            prop = property(getx, setx)
            setattr(cls, ext_field_name, _field)
            setattr(cls, field_name, prop)


class Measurement(metaclass=MeasurementMeta):

    class Meta:
        db_table = None

    @property
    def table_name(self):
        return self.Meta.db_table or self.__class__.__name__.lower()

    def __init__(self, **kwargs):
        self.check_fields_values(**kwargs)
        self.clone_fields()
        self.fill_values(**kwargs)

    def check_fields_values(self, **kwargs):
        def filter_required_fields(x):
            return not x.default and not x.is_nullable

        fields = self._get_fields()
        required_fields = list(filter(
            filter_required_fields,
            fields,
        ))
        required_fields_names = [
            r.field_name
            for r in required_fields
        ]
        for key in required_fields_names:
            if key not in kwargs:
                raise InfluxDBFieldValueError(
                    'The fields \'{}\' cannot be nullable'.format(key)
                )

    def clone_fields(self):
        fields = self._get_fields()
        for attr in fields:
            cloned_fields = attr.clone()
            cloned_fields.field_name = attr.field_name
            cloned_fields.ext_field_name = attr.ext_field_name
            setattr(self, attr.ext_field_name, cloned_fields)

    def dict(self):
        dict_values = {}
        fields = self.get_fields()
        for attr in fields:
            dict_values[attr.field_name] = getattr(self, attr.field_name)
        return dict_values

    def get_fields(self):
        def filter_func(x):
            return isinstance(x, BaseField)
        variables = self.__dict__.values()
        fields = list(filter(filter_func, variables))
        return fields

    def get_field_names(self):
        fields = self.get_fields()
        field_names = [attr.field_name for attr in fields]
        return field_names

    def get_ext_field_names(self):
        fields = self.get_fields()
        field_names = [attr.ext_field_name for attr in fields]
        return field_names

    def get_timestamp_fields(self):
        def filter_func(x):
            return isinstance(x, TimestampField)
        fields = self.get_fields()
        timestamp_fields = list(filter(filter_func, fields))
        return timestamp_fields

    # def get_prep_value(self):
    #     def factory_filter_func(cls):
    #         def filter_func(attr):
    #             return isinstance(attr, cls)
    #         return filter_func
    #
    #     fields = self.get_fields()
    #     field_fields = list(filter(
    #         factory_filter_func(GenericField),
    #         fields,
    #     ))
    #     tag_fields = list(filter(
    #         factory_filter_func(TagField),
    #         fields,
    #     ))
    #     timestamp_fields = list(filter(
    #         factory_filter_func(TimestampField),
    #         fields,
    #     ))
    #
    #     prep_value_groups = []
    #     fields_groups = [tag_fields, field_fields, timestamp_fields]
    #     for attr_group in fields_groups:
    #         prep_value_group = []
    #         for attr in attr_group:
    #             attr_prep_value = attr.get_prep_value()
    #             attr_name = attr.name or attr.field_name
    #             if not isinstance(attr, TimestampField):
    #                 prep_value = '{}={}'.format(attr_name, attr_prep_value)
    #             else:
    #                 prep_value = '{}'.format(attr_prep_value)
    #             prep_value_group.append(prep_value)
    #         str_prep_value_group = ','.join(prep_value_group)
    #         prep_value_groups.append(str_prep_value_group)
    #
    #     if prep_value_groups[0]:
    #         prep_value_groups[0] = ','.join(
    #             [self.table_name] + [prep_value_groups[0]]
    #         )
    #     else:
    #         prep_value_groups[0] = self.table_name
    #     final_prep_value = ' '.join(prep_value_groups)
    #     return final_prep_value

    def get_point_data(self):
        def factory_filter_func(cls):
            def filter_func(attr):
                return isinstance(attr, cls)

            return filter_func

        fields = self.get_fields()
        field_fields = list(filter(
            factory_filter_func(GenericField),
            fields,
        ))
        tag_fields = list(filter(
            factory_filter_func(TagField),
            fields,
        ))
        timestamp_fields = list(filter(
            factory_filter_func(TimestampField),
            fields,
        ))

        point_data = {
            "measurement": self.table_name
        }
        point_data_fields_types = {
            "tags": [tag_fields],
            "fields": [field_fields, timestamp_fields]
        }
        for point, fields_types in point_data_fields_types.items():
            _fields = dict()
            for attr in fields_types:
                for f in attr:
                    attr_prep_value = f.get_prep_value()
                    attr_name = f.name or f.field_name
                    _fields.update({attr_name: attr_prep_value})

            if _fields:
                point_data.update({point: _fields})

        return point_data

    def fill_values(self, **kwargs):
        try:
            for key, value in kwargs.items():
                setattr(self, key, value)
        except Exception as err:
            print('key', key)
            msg = '<\'{key}\'> : {msg}'.format(key=key, msg=err)
            raise InfluxDBFieldValueError(msg)

    def items(self):
        return self.dict().items()

    # @staticmethod
    # def bulk_save(points):
    #     if not isinstance(points, list):
    #         raise InfluxDBFieldValueError('points must be a list')
    #     str_points = ''
    #     for point in points:
    #         if not isinstance(point, Measurement):
    #             raise InfluxDBFieldValueError(
    #                 'type of point must be Measurement'
    #             )
    #         prep_value = point.get_prep_value()
    #         str_points += prep_value
    #         str_points += '\n'
    #     return BulkInsertQuery(str_points).execute()
