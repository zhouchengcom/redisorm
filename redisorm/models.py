from six import add_metaclass, iteritems

from schematics import models
from .types import Hash, Field
from .transforms import save, load, rkeys

from six import PY2, PY3,  text_type as unicode

if PY2:
    b2s = lambda v: v
elif PY3:
    b2s = lambda v: unicode(v, "utf-8")


class ModelMeta(models.ModelMeta):

    def __init__(cls, name, bases, attrs, **kwargs):
        super(ModelMeta, cls).__init__(name, bases, attrs)
        cls.prefix_key = (name + ":%s") if "namespace" not in attrs else (attrs['namespace'] + ":" + name + ":%s")
        internal_model = {}
        for field_name, field in cls._fields.iteritems():
            if isinstance(field, Field):
                prefix = (name + ":%s:" + field_name) if "namespace" not in attrs else (attrs['namespace'] + ":" + name + ":%s:" + field_name)
                field.pkey = prefix
            if isinstance(field, Hash):
                prefix = (name + ":%s") if "namespace" not in attrs else (attrs['namespace'] + ":" + name + ":%s")
                field.pkey = prefix
                field.skey = field_name
            if isinstance(field, ModelType):
                internal_model[field_name] = lambda value: [value]

            if (isinstance(field, ListType) or isinstance(field, SetType)) and isinstance(field.field, ModelType):
                internal_model[field_name] = lambda value: value

            if isinstance(field, DictType) and isinstance(field.field, ModelType):
                internal_model[field_name] = lambda value: value.itervalues()
        cls._internal_model = internal_model


@add_metaclass(ModelMeta)
class Model(models.Model):

    def __init__(self, pk, raw_data=None, deserialize_mapping=None, strict=True):
        super(Model, self).__init__(raw_data, deserialize_mapping, strict)
        self.pk = pk

    def save(self, db):
        return save(self.__class__, self, db, self.pk)

    def load(self, db):
        load(self.__class__, self, self.pk, db)

    def load_pipe_result(self, result):
        data = {k: result[v - 1] for k, v in iteritems(self._data)}
        for field_name, field in iteritems(self._fields):
            if field_name not in data:
                continue
            
            if data[field_name] is None:
                self._data[field_name] = None

            if hasattr(field, "load_pipe_result"):
                value = field.load_pipe_result(data[field_name])
                if value:
                    data[field_name] = value

        self.import_data(data)

    def key(self):
        return self.prefix_key % self.pk

    def rkeys(self):
        return rkeys(self.__class__, self)

    def internal_model_instances(self):
        internal = []
        for field_name, f in self._internal_model.iteritems():
            if field_name in self._data:
                internal.extend(f(self._data[field_name]))

        return internal

from .types.compound import ModelType, ListType, SetType, DictType
