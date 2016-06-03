
from six import iteritems

from schematics.transforms import wholelist, atoms, allow_none, sort_dict, _list_or_string
from schematics.exceptions import ModelConversionError, ConversionError

from schematics.types.serializable import Serializable
from redis.client import BasePipeline


def save(cls, instance, db, pk):

    data = save_loop(cls, instance, db, pk)
    return data


def save_loop(cls, instance, db, pk):

    for field_name, field, value in atoms(cls, instance):
        if isinstance(field, Serializable):
            continue
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
        if isinstance(field, Serializable):
            continue
        if hasattr(field, "load_loop"):
            if isinstance(field, ModelType):
                value = db.hget(cls.prefix_key % pk, field_name)
                if value:
                    value = value.decode(encoding='UTF-8')
                    data[field_name] = field.load_loop(None, value, db)
            else:
                data[field_name] = field.load_loop(None, pk, db)
        else:
            value = field.load(db, pk)
            if value:
                value = field.to_native(value)
                data[field_name] = value

    return data


def load(cls, instance, pk, db):
    if isinstance(db, BasePipeline):
        data = pipe_load_loop(cls, instance, pk, db)
    else:
        data = load_loop(cls, instance, pk, db)

    instance._data.update(data)


def pipe_load_loop(cls, instance, pk, db):
    data = {}

    for field_name, field in iteritems(cls._fields):
        if isinstance(field, Serializable):
            continue
        if hasattr(field, "load_loop"):
            if isinstance(field, ModelType):
                data[field_name] = len(db.hget(cls.prefix_key % pk, field_name))
            else:
                data[field_name] = field.pipe_load_loop(None, pk, db)
        else:
            data[field_name] = len(field.load(db, pk))

    return data


def rkeys(cls, instance):
    keys = []
    for field_name, field, value in atoms(cls, instance):
        if isinstance(field, Serializable):
            continue
        # Value found, apply transformation and store it
        if value is None:
            continue

        shaped = field.rkeys(instance.pk, value)
        if shaped:
            keys.extend(shaped)

    return keys


def pipe_load_all(instances, db):
    p = db.pipeline()
    for i in instances:

        i.load(p)

    result = p.execute()

    subs = []
    for i in instances:
        i.load_pipe_result(result)
        subs.extend(i.internal_model_instances())

    if subs:
        pipe_load_all(subs, db)

from .types.compound import ModelType
