import inspect

from django_cloudapp_common.influx.db import Query


class BaseManager:

    def __init__(self):
        self.model = None
        self.name = None

    @classmethod
    def _get_query_methods(cls, query_class):
        def create_method(name, method):
            def manager_method(self, *args, **kwargs):
                return getattr(self.get_query(), name)(*args, **kwargs)
            manager_method.__name__ = method.__name__
            manager_method.__doc__ = method.__doc__
            return manager_method

        new_methods = {}
        for name, method in inspect.getmembers(query_class, predicate=inspect.isfunction):
            # Only copy missing methods.
            if hasattr(cls, name):
                continue
            # Only copy public methods or methods with the attribute `queryset_only=False`.
            # queryset_only = getattr(method, 'queryset_only', None)
            # if queryset_only or (queryset_only is None and name.startswith('_')):
            #     continue
            # Copy the method onto the manager.
            new_methods[name] = create_method(name, method)
        return new_methods

    @classmethod
    def from_query(cls, query_class, class_name=None):
        if class_name is None:
            class_name = '%sFrom%s' % (cls.__name__, query_class.__name__)
        return type(class_name, (cls,), {
            '_query_class': query_class,
            **cls._get_query_methods(query_class)
        })

    def contribute_to_class(self, model, name):
        self.name = self.name or name
        self.model = model
        setattr(model, name, self)

    def get_query(self):
        return self._query_class(model=self.model)

    def all(self):
        return self.get_query()


class Manager(BaseManager.from_query(Query)):
    pass
