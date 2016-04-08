
from six import iteritems

from schematics.transforms import wholelist, atoms, allow_none, sort_dict, _list_or_string
from schematics.exceptions import ModelConversionError, ConversionError


from redis.client import BasePipeline

def save(cls, instance, db, role=None, raise_error_on_role=True):
    field_converter = lambda field, value: field.save(db, instance.pk, field.to_primitive(value))
    data = save_loop(cls, instance, db, field_converter,
                     role=role, raise_error_on_role=raise_error_on_role)
    return data


def save_loop(cls, instance, db, field_converter,
              role=None, raise_error_on_role=False, print_none=False):
    """
    The export_loop function is intended to be a general loop definition that
    can be used for any form of data shaping, such as application of roles or
    how a field is transformed.

    :param cls:
        The model definition.
    :param instance_or_dict:
        The structure where fields from cls are mapped to values. The only
        expectionation for this structure is that it implements a ``dict``
        interface.
    :param field_converter:
        This function is applied to every field found in ``instance_or_dict``.
    :param role:
        The role used to determine if fields should be left out of the
        transformation.
    :param raise_error_on_role:
        This parameter enforces strict behavior which requires substructures
        to have the same role definition as their parent structures.
    :param print_none:
        This function overrides ``serialize_when_none`` values found either on
        ``cls`` or an instance.
    """
    data = {}

    # Translate `role` into `gottago` function
    gottago = wholelist()
    if hasattr(cls, '_options') and role in cls._options.roles:
        gottago = cls._options.roles[role]
    elif role and raise_error_on_role:
        error_msg = u'%s Model has no role "%s"'
        raise ValueError(error_msg % (cls.__name__, role))
    else:
        gottago = cls._options.roles.get("default", gottago)

    fields_order = (getattr(cls._options, 'fields_order', None)
                    if hasattr(cls, '_options') else None)

    for field_name, field, value in atoms(cls, instance):
        serialized_name = field.serialized_name or field_name

        # Skipping this field was requested
        if gottago(field_name, value):
            continue

        # Value found, apply transformation and store it
        elif value is not None:
            if hasattr(field, 'export_loop'):
                shaped = field.export_loop(value, field_converter,
                                           role=role,
                                           print_none=print_none)
            else:
                shaped = field_converter(field, value)

            # Print if we want none or found a value
            if shaped is None and allow_none(cls, field):
                data[serialized_name] = shaped
            elif shaped is not None:
                data[serialized_name] = shaped
            elif print_none:
                data[serialized_name] = shaped

        # Store None if reqeusted
        elif value is None and allow_none(cls, field):
            data[serialized_name] = value
        elif print_none:
            data[serialized_name] = value

    # Return data if the list contains anything
    if len(data) > 0:
        if fields_order:
            return sort_dict(data, fields_order)
        return data
    elif print_none:
        return data


def load_loop(cls, pk, db):
    pipe  =  isinstance(db, BasePipeline)
    data = {}

    for field_name, field in iteritems(cls._fields):
        value = field.load(db, pk)
        data[field_name] = (len(value) -1) if pipe else value

    return data
    
    

def load(cls, instance, db):
    data = load_loop(cls, instance.pk, db )
    
    instance.import_data(data)


