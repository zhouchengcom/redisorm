from schematics import models






class ModelMeta(type):

    """
    Meta class for Models.
    """

    def __new__(mcs, name, bases, attrs):
        """
        This metaclass adds four attributes to host classes: mcs._fields,
        mcs._serializables, mcs._validator_functions, and mcs._options.

        This function creates those attributes like this:

        ``mcs._fields`` is list of fields that are schematics types
        ``mcs._serializables`` is a list of functions that are used to generate
        values during serialization
        ``mcs._validator_functions`` are class level validation functions
        ``mcs._options`` is the end result of parsing the ``Options`` class
        """

        # Structures used to accumulate meta info
        fields = OrderedDictWithSort()
        serializables = {}
        validator_functions = {}  # Model level

        # Accumulate metas info from parent classes
        for base in reversed(bases):
            if hasattr(base, '_fields'):
                fields.update(metacopy(base._fields))
            if hasattr(base, '_serializables'):
                serializables.update(metacopy(base._serializables))
            if hasattr(base, '_validator_functions'):
                validator_functions.update(base._validator_functions)

        # Parse this class's attributes into meta structures
        for key, value in iteritems(attrs):
            if key.startswith('validate_') and callable(value):
                validator_functions[key[9:]] = value
            if isinstance(value, BaseType):
                fields[key] = value
            if isinstance(value, Serializable):
                serializables[key] = value

        # Parse meta options
        options = mcs._read_options(name, bases, attrs)

        # Convert list of types into fields for new klass
        fields.sort(key=lambda i: i[1]._position_hint)
        for key, field in iteritems(fields):
            attrs[key] = FieldDescriptor(key)

        # Ready meta data to be klass attributes
        attrs['_fields'] = fields
        attrs['_serializables'] = serializables
        attrs['_validator_functions'] = validator_functions
        attrs['_options'] = options
        
        import pprint
        pprint.pprint(mcs) 
        pprint.pprint(name) 
        pprint.pprint(bases) 
        pprint.pprint(attrs)
        klass = type.__new__(mcs, name, bases, attrs)

        # Add reference to klass to each field instance
        def set_owner_model(field, klass):
            field.owner_model = klass
            if hasattr(field, 'field'):
                set_owner_model(field.field, klass)
        for field_name, field in fields.items():
            set_owner_model(field, klass)
            field.name = field_name

        # Register class on ancestor models
        klass._subclasses = []
        for base in klass.__mro__[1:]:
            if isinstance(base, ModelMeta):
                base._subclasses.append(klass)

        return klass
