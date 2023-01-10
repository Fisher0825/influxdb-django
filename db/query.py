import copy
from collections import namedtuple
from copy import deepcopy
from functools import lru_cache

from .criteria import Field
from .function import aggregations
from ..response import InfluxDBResponse
from ..serializers import BaseSerializer
from .. import exceptions
from ..app import Influxable


class RawQuery:
    def __init__(self, str_query):
        self.str_query = str_query

    def execute(self):
        return self.raw_response

    @property
    def raw_response(self):
        return self._resolve()

    def _resolve(self, *args, **kwargs):
        instance = Influxable.get_instance()
        return instance.execute_query(query=self.str_query, method='post')


class Query(RawQuery):

    def __init__(self, model=None):
        self.model = model
        self.initial_query = '{select_clause} {from_clause} {where_clause} {order_clause} {limit_offset}'
        self.initial_delete = 'delete from {measurement} where time={time}'
        self.from_clause = 'FROM {measurements}'
        self.select_clause = 'SELECT {fields}'
        self.order_by_clause = 'ORDER BY {order_by}'
        self.where_clause = ' WHERE {criteria}'
        self.selected_fields = []
        self.selected_criteria = []
        self.search_keys = []
        self.order_by = '-time'
        self.is_distinct = False
        self.limit_value = None
        self.slimit_value = None
        self.offset_value = None
        self.soffset_value = None
        self._result_cache = None

    @property
    def selected_measurement(self):
        if self.model:
            return self.model.Meta.db_table or self.__name__.lower()
        return 'default'

    def select(self, *fields):
        query = self._clone()
        for f in fields:
            evaluated_field = f.evaluate() if hasattr(f, 'evaluate') else "\"{}\"".format(f)
            query.selected_fields.append(evaluated_field)
        return query

    def filter(self, *criteria, **kwargs):
        query = self._clone()
        query.selected_criteria.extend(list(criteria))
        for field, value in kwargs.items():
            query.selected_criteria.append(Field(field) == value)
        return query

    def search_query(self, *criteria, **kwargs):
        query = self._clone()
        query.selected_criteria.extend(list(criteria))
        for field, value in kwargs.items():
            query.search_keys.append({field: value})
        return query

    def where(self, *criteria, **kwargs):
        query = self._clone()
        query.selected_criteria = list(criteria)
        return query

    def limit(self, value):
        query = self._clone()
        # influx COUNT() return null when limit & offset exist
        query.selected_fields = []
        query.limit_value = value
        return query

    def slimit(self, value):
        query = self._clone()
        # influx COUNT() return null when limit & offset exist
        query.selected_fields = []
        query.slimit_value = value
        return query

    def offset(self, value):
        query = self._clone()
        # influx COUNT() return null when limit & offset exist
        query.selected_fields = []
        query.offset_value = value
        return query

    def soffset(self, value):
        query = self._clone()
        # influx COUNT() return null when limit & offset exist
        query.selected_fields = []
        query.soffset_value = value
        return query

    def distinct(self):
        query = self._clone()
        if len(query.selected_fields) == 1:
            if not "DISTINCT" in query.selected_fields[0]:
                query.selected_fields[0] = aggregations.Distinct(query.selected_fields[0]).evaluate()
        return query

    def count(self):
        query = self._clone()
        if query._result_cache is None:
            # influx COUNT() return null when limit & off
            # set exist
            if query.limit_value or query.offset_value:
                query._fetch_all()
            else:
                if len(query.selected_fields) == 1:
                    if not "COUNT" in query.selected_fields[0]:
                        query.selected_fields[0] = aggregations.Count(query.selected_fields[0]).evaluate()
                else:
                    query.selected_fields.insert(0, aggregations.Count("*").evaluate())

                number = query._get_count()
                query.selected_fields = []
                return number

        return len(query._result_cache)

    def sum(self):
        query = self._clone()
        if len(query.selected_fields) == 1:
            if not "SUM" in query.selected_fields[0]:
                query.selected_fields[0] = aggregations.Sum(query.selected_fields[0]).evaluate()
        else:
            query.selected_fields.insert(0, aggregations.Sum("*").evaluate())

        return query._get_sum()

    def total(self):
        query = self._clone()
        return query.count()

    def all(self):
        query = self._clone()
        return query

    def _clone(self):
        query = self.__class__(model=self.model)
        copy_attrs = (
            "order_by", "selected_fields", "selected_criteria", "search_keys", "is_distinct",
            "limit_value", "slimit_value", "offset_value", "soffset_value"
        )
        for attr in copy_attrs:
            v = copy.deepcopy(getattr(self, attr))
            setattr(query, attr, v)
        return query

    def clear_cache(self):
        self._result_cache = None

    def integral(self, value='*'):
        return self.select(aggregations.Integral(value))

    def mean(self, value='*'):
        return self.select(aggregations.Mean(value))

    def median(self, value='*'):
        return self.select(aggregations.Median(value))

    def mode(self, value='*'):
        return self.select(aggregations.Mode(value))

    def spread(self, value='*'):
        return self.select(aggregations.Spread(value))

    def std_dev(self, value='*'):
        return self.select(aggregations.StdDev(value))

    # def sum(self, value='*'):
    #     return self.select(aggregations.Sum(value))

    def format_oder(self):
        if self.order_by.startswith("-"):
            order_field = self.order_by.split("-")[-1]
            return f"{order_field} DESC"

        return self.order_by

    def _prepare_select_clause(self):
        # if self.is_distinct:
        #     pass
        _clause = ', '.join(self.selected_fields) if self.selected_fields else '*'

        return self.select_clause.format(fields=_clause)

    def _prepare_where_clause(self):

        if not self.selected_criteria and not self.search_keys:
            return ''

        where_clause = ''
        if len(self.selected_criteria):
            criteria = [c.evaluate() for c in self.selected_criteria]
            where_clause = ' AND '.join(criteria)

        format_list = []
        if len(self.search_keys):
            for dict in self.search_keys:
                for field, value in dict.items():
                    if type(value) == bool:
                        value = '1' if value == True else '0'
                    elif type(value) == str:
                        value = "~/{}/".format(value)
                    field = '"{}"'.format(field)
                    format_list.append(str(field) + '=' + str(value))
            if len(self.selected_criteria):
                where_clause += ' AND '
            else:
                where_clause = ''
            or_str = ' OR '.join(format_list)
            or_str = '(' + or_str + ')'
            where_clause += or_str

        _clause = where_clause
        return self.where_clause.format(criteria=_clause)

    def _prepare_limit_offset(self):
        _clause = ''
        if self.limit_value is not None:
            self.limit_clause = ' LIMIT {}'.format(self.limit_value)
            _clause += self.limit_clause
        if self.offset_value is not None:
            self.offset_clause = ' OFFSET {}'.format(self.offset_value)
            _clause += self.offset_clause
        if self.slimit_value is not None:
            self.slimit_clause = ' SLIMIT {}'.format(self.slimit_value)
            _clause += self.slimit_clause
        if self.soffset_value is not None:
            self.soffset_clause = ' SOFFSET {}'.format(self.soffset_value)
            _clause += self.soffset_clause

        return _clause

    def _prepare_query(self):
        select_clause = self._prepare_select_clause()
        from_clause = self.from_clause.format(measurements=self.selected_measurement)
        where_clause = self._prepare_where_clause()
        order_clause = self.order_by_clause.format(order_by=self.format_oder())
        limit_offset_clause = self._prepare_limit_offset()
        prepared_query = self.initial_query.format(
            select_clause=select_clause,
            from_clause=from_clause,
            where_clause=where_clause,
            order_clause=order_clause,
            limit_offset=limit_offset_clause,
        )

        print('prepared_query', prepared_query)
        return prepared_query

    def __iter__(self):
        if self._result_cache is None:
            self._fetch_all()
        return iter(self._result_cache)

    def __len__(self):
        if self._result_cache is None:
            self._fetch_all()
        return len(self._result_cache)

    def __bool__(self):
        self._fetch_all()
        return bool(self._result_cache)

    def __getstate__(self):
        self._fetch_all()
        return {**self.__dict__}

    def create(self, **kwargs):
        obj = self.model(**kwargs)
        point_data = obj.get_point_data()
        return BulkInsertQuery(point_data).execute()

    def bulk_create(self, objs):
        assert isinstance(objs, list), \
            exceptions.InfluxDBFieldValueError('bulk_create expect a list data.')

        str_points = ''
        for obj in objs:
            try:
                prep_value = obj.get_prep_value()
            except Exception:
                raise exceptions.InfluxDBFieldValueError('type of obj must be Measurement')

            str_points += prep_value
            str_points += '\n'
        return BulkInsertQuery(str_points).execute()

    def bulk_save(self, points):
        if not isinstance(points, list):
            raise exceptions.InfluxDBFieldValueError('points must be a list')
        str_points = ''
        for point in points:
            # if not isinstance(point, Measurement):
            #     raise InfluxDBFieldValueError(
            #         'type of point must be Measurement'
            #     )
            prep_value = point.get_prep_value()
            str_points += prep_value
            str_points += '\n'
        return BulkInsertQuery(str_points).execute()

    def delete(self, *args, **kwargs):
        # 1, 获取查询结果
        if not self._result_cache:
            if not self._fetch_all():
                return False

        delete_objects = []
        # 筛选删除记录
        if kwargs:
            for obj in self._fetch_all():
                for k,v in kwargs.items():
                    if getattr(obj, k) != v:
                        break
                else:
                    delete_objects.append(obj)

        instance = Influxable.get_instance()
        # 2, 删除查询结果
        times = [getattr(obj, 'time') for obj in delete_objects]
        for time in times:
            query_str = self.initial_delete.format(measurement=self.selected_measurement, time=time)
            instance.delete_points(query_str)
        return True

    def _fetch_all(self):
        query_result = InfluxDBResponse(self.execute())
        measurement_objs = self.query_to_objects(query_result)
        self._result_cache = measurement_objs
        return measurement_objs

    def execute(self):
        prepared_query = self._prepare_query()
        self.str_query = prepared_query
        return super().execute()

    def query_to_objects(self, query_result):
        objects = []

        columns = query_result.columns
        raws = query_result.raws
        for raw in raws:
            obj = self.raw_to_object(columns, raw)
            objects.append(obj)

        return objects

    def _get_count(self):
        query_result = InfluxDBResponse(self.execute())
        raws = deepcopy(query_result.raws)
        if not raws:
            return 0

        count_raw = raws[0]
        if 'time' == query_result.columns[0]:
            count_raw.pop(0)

        count_raw = filter(lambda x: x is not None, count_raw)
        number = max(count_raw) if count_raw else 0
        return number

    def _get_sum(self):
        query_result = InfluxDBResponse(self.execute())
        raws = deepcopy(query_result.raws)
        if not raws:
            return 0

        sum_raw = raws[0]
        if 'time' == query_result.columns[0]:
            sum_raw.pop(0)

        return sum_raw[0]

    def raw_to_object(self, columns, raw):
        obj = namedtuple(self.selected_measurement, ' '.join(columns))
        for i, column in enumerate(columns):
            v = raw[i]
            # if column == 'time':
            #     v = utc2local(raw[i])
            setattr(obj, column, v)

        return obj

    def format(self, result, parser_class=BaseSerializer, **kwargs):
        return parser_class(result, **kwargs).convert()

    def evaluate(self, parser_class=BaseSerializer, **kwargs):
        result = InfluxDBResponse(self.execute())
        self.query_to_objects(result)
        result.raise_if_error()
        formatted_result = self.format(result, parser_class, **kwargs)
        return formatted_result


class BulkInsertQuery(RawQuery):

    def execute(self):
        instance = Influxable.get_instance()
        return instance.write_points(self.str_query)

    # @lru_cache(maxsize=None)
    # def _resolve(self, *args, **kwargs):
    #     instance = Influxable.get_instance()
    #     return instance.write_points(points=self.str_query)
