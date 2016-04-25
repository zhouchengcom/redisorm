
from six import iteritems

from schematics.transforms import wholelist, atoms, allow_none, sort_dict, _list_or_string
from schematics.exceptions import ModelConversionError, ConversionError


from redis.client import BasePipeline


def save(cls, instance, db, pk):

    data = save_loop(cls, instance, db, pk)
    return data


def save_loop(cls, instance, db, pk):

    for field_name, field, value in atoms(cls, instance):
        # Value found, apply transformation and store it
        if value is None:
            continue

        if hasattr(field, 'save_loop'):
            shaped = field.save_loop(value, db, pk)
            if shaped:
                db.hset(cls.prefix_key % pk, field_name, shaped)
        else:
            shaped = field.to_primitive(value)
            field.save(db, pk, shaped)

    return pk


def load_loop(cls, instance, pk, db):
    data = {}

    for field_name, field in iteritems(cls._fields):
        if hasattr(field, "load_loop"):
            if isinstance(field, ModelType):
                value = db.hget(cls.prefix_key % pk, field_name)
                if value:
                    value = value.decode(encoding='UTF-8')
                    data[field_name] = field.load_loop(None, value, db)
            else:
                data[field_name] = field.load_loop(None, pk, db)
        else:
            data[field_name] = field.load(pk, db)

    return data


def load(cls, instance, pk, db):
    data = load_loop(cls, instance, pk, db)
    instance._data.update(data)


from .types.compound import ModelType
