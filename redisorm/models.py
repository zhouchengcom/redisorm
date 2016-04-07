from six import add_metaclass

from schematics import models
from .types import Hash, Field
from .transforms import save, load
from redis.client import BasePipeline

# class FieldDescriptor(models.FieldDescriptor):

#     def __init__(self, name, prefix, t):
#         super(FieldDescriptor, self).__init__(name)

#         if isinstance(t, FieldType):
#             self.pkey = prefix + ":" + name + ":%s"
#             self.skey = None
#         else:
#             self.pkey = prefix + ":%s"
#             self.skey = name

#         print(self.pkey, self.skey)


class ModelMeta(models.ModelMeta):

    def __init__(cls, name, bases, attrs, **kwargs):
        print('  Meta.__init__(cls=%s, name=%r, bases=%s, attrs=[%s], **%s)' % (cls, name, bases, ', '.join(attrs), kwargs))
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
        data = load(self.__class__, self, db)
        