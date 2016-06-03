from schematics.types import compound
from .base import Field
from six import iteritems
from ..transforms import save_loop
from six import PY2, PY3,  text_type as unicode


if PY2:
    b2s = lambda v: v
elif PY3:
    b2s = lambda v: unicode(v, "utf-8")


class DictType(compound.DictType, Field):

    def save_loop(self, instance, db, pk):
        for key, value in iteritems(instance):
            if hasattr(self.field, 'save_loop'):
                shaped = self.field.save_loop(value)
                if shaped:
                    db.hset(self.pkey % pk, key, shaped)
            else:
                shaped = self.field.to_primitive(value)
                self.field.pkey = self.pkey
                self.field.save(db, pk, shaped, key)

    def load_loop(self, instance, pk, db):
        values = db.hgetall(self.pkey % pk)
        if values:
            data = {self.coerce_key(k, "utf-8"): self.field.to_native(v) for k, v in iteritems(values)}
            return data

    def pipe_load_loop(self, instance, pk, db):
        return len(db.hgetall(self.pkey % pk))

    def rkeys(self, pk, instance):
        if isinstance(self.field, ModelType):
            keys = []
            for _, value in iteritems(instance):
                keys.extend(value.rkeys())
            keys.append(self.pkey % pk)
            return keys
        else:
            return [self.pkey % pk]

    def load_pipe_result(self, values):
        if isinstance(self.field, ModelType):
            return {key: self.model_class(b2s(value)) for key, value in iteritems(values)}


class SetType(compound.ListType, Field):

    def save_loop(self, instance, db, pk):
        values = set()
        for value in instance:
            if hasattr(self.field, 'save_loop'):
                shaped = self.field.save_loop(value, db, pk)
                if shaped:
                    values.add(shaped)
            else:
                shaped = self.field.to_primitive(value)
                #
                values.add(shaped)

        if values:
            db.sadd(self.pkey % pk, *values)

    def load_loop(self, instance, pk, db):
        values = db.smembers(self.pkey % pk)
        if values:
            if hasattr(self.field, 'load_loop'):
                return set([self.field.load_loop(None, db, v) for v in values])
            else:
                return set([self.field.to_native(v) for v in values])

    def pipe_load_loop(self, instance, pk, db):
        return len(db.smembers(self.pkey % pk))

    def rkeys(self, pk, value):
        if isinstance(self.field, ModelType):
            keys = []
            for v in value:
                keys.extend(v.rkeys())
            keys.append(self.pkey % pk)
            return keys
        else:
            return [self.pkey % pk]

    def load_pipe_result(self, values):
        if isinstance(self.field, ModelType):
            return set([self.model_class(b2s(v)) for v in values])


class ListType(compound.ListType, Field):

    def save_loop(self, instance, db, pk):
        values = []
        for value in instance:
            if hasattr(self.field, 'save_loop'):
                shaped = self.field.save_loop(value, db, pk)
                if shaped:
                    values.append(shaped)
            else:
                shaped = self.field.to_primitive(value)
                #
                values.append(shaped)

        if values:
            db.lpush(self.pkey % pk, *values)

    def load_loop(self, instance, pk, db):
        values = db.lrange(self.pkey % pk, 0, -1)
        if values:
            if hasattr(self.field, 'load_loop'):
                return [self.field.load_loop(None, db, v) for v in values]
            else:
                return [self.field.to_native(v) for v in values]

    def pipe_load_loop(self, instance, pk, db):
        return len(db.lrange(self.pkey % pk, 0, -1))

    def rkeys(self, pk, value):
        if isinstance(self.field, ModelType):
            keys = []
            for v in value:
                keys.extend(v.rkeys())
            keys.append(self.pkey % pk)
            return keys
        else:
            return [self.pkey % pk]

    def load_pipe_result(self, values):
        if isinstance(self.field, ModelType):
            return [self.model_class(b2s(v)) for v in values]


class ModelType(compound.ModelType, Field):

    def save_loop(self, model_instance, db, pk):
        """
        Calls the main `export_loop` implementation because they are both
        supposed to operate on models.
        """
        if isinstance(model_instance, self.model_class):
            model_class = model_instance.__class__
        else:
            model_class = self.model_class

        shaped = save_loop(model_class, model_instance, db, model_instance.pk)
        return shaped

    def load_loop(self, instance, db, pk):
        model = instance if instance else self.model_class(pk)
        model.load(db)
        return model

    def rkeys(self, pk, value):
        return value.rkeys()

    def load_pipe_result(self, value):
        return self.model_class(pk=value)
