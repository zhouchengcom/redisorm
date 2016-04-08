from six import add_metaclass

from schematics import models
from .types import Hash, Field
from .transforms import save, load

class ModelMeta(models.ModelMeta):

    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)

        for field_name, field in cls._fields.iteritems():
            if isinstance(field, Field):
                prefix = (name + ":" + field_name + ":%s") if "namespace" not in attrs else (attrs['namespace'] + ":" + name + ":" + field_name + ":%s")
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

    def save(self, db, role=None):
        return save(self.__class__, self, db, role=role)

    def load(self, db):
        load(self.__class__, self, db)
