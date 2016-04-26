from six import add_metaclass, iteritems

from schematics import models
from .types import Hash, Field
from .transforms import save, load

from six import PY2, PY3,  text_type as unicode

if PY2:
    b2s = lambda v: v
elif PY3:
    b2s = lambda v: unicode(v, "utf-8")
    
class ModelMeta(models.ModelMeta):

    def __init__(cls, name, bases, attrs, **kwargs):
        super(ModelMeta, cls).__init__(name, bases, attrs)
        cls.prefix_key = (name + ":%s") if "namespace" not in attrs else (attrs['namespace'] + ":" + name + ":%s")
        for field_name, field in cls._fields.iteritems():
            if isinstance(field, Field):
                prefix = (name + ":%s:" + field_name) if "namespace" not in attrs else (attrs['namespace'] + ":" + name + ":%s:" + field_name)
                field.pkey = prefix
            if isinstance(field, Hash):
                prefix = (name + ":%s") if "namespace" not in attrs else (attrs['namespace'] + ":" + name + ":%s")
                field.pkey = prefix
                field.skey = field_name


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
            if isinstance(field, ModelType) and data[field_name]:
                pk = b2s(data[field_name])
                data[field_name] = field.model_class(pk=pk)
            
        self.import_data(data)
        
        
from .types.compound import ModelType
