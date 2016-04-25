from schematics.types import compound
from .base import Field
from six import iteritems
from ..transforms import save_loop


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
            data = {k: self.field.to_native(v) for k, v in iteritems(values)}
            return data


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

        shaped = save_loop(model_class, model_instance, db, pk)
        return shaped

    def load_loop(self, instance, db, pk):
        model = instance if instance else self.model_class(pk)
        model.load(db)
        return model
