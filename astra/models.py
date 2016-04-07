 # coding=utf-8
import datetime as dt
import redis
import sys


class Model:
    """
    Parent class for all user-objects to be managed
    class Stream(models.Model):
        database = strict_redis_instance(..)
        name = models.CharHash(max_length=128)
        ...
    """

    prefix = 'astra'
    # pk = None
    _fields = dict()

    def __init__(self, pk=None, **kwargs):
        print("init")
        self._fields = dict()
        print("init")
        self._helpers = set()
        self._hash = {}  # Hash-object cache
        self._hash_loaded = False
        assert isinstance(self.database, redis.StrictRedis)
        if not pk:
            raise ValueError('You\'re must pass pk for object')
        self.pk = str(pk)  # Always convert to str for key-safe ops.
        for k, v in vars(self.__class__).items():
            if isinstance(v, ModelField):
                new_instance_of_field = v.__class__(instance=True, model=self,
                                                    name=k, **v._options)
                self._fields[k] = new_instance_of_field

        # Assign by kwargs value's
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __setattr__(self, key, value): 
        print(key)
        if hasattr(self, '_fields') and key in self._fields.keys():
            field = self._fields[key]  # self.__class__.__dict__.get(key)
            field._assign(value)
        else:
            print("dddd") 
            return super().__setattr__(key, value)

    def __getattribute__(self, key):
        if key == '_fields':
            print(key)
            return object.__getattribute__(self, key)

        if key in self._fields:
            field = self._fields[key]
            print("are you test")
            return field._obtain()

        # If key is not in the fields, we're attempt call helper
        key_elements = key.split('_', )
        method_name = key_elements.pop()
        field_name = '_'.join(key_elements)
        if field_name in self._fields:
            field = self._fields[field_name]
            return field._helper(method_name)

        # Otherwise, default behavior:
        return object.__getattribute__(self, key)

    def __dir__(self):  # TODO: Check it
        return self._fields

    def __eq__(self, other):
        """
        Compare two models
        More magic is here: http://www.rafekettler.com/magicmethods.html
        """
        if isinstance(other, Model):
            return self.pk == other.pk
        return super().__eq__(other)

    def __repr__(self):
        return '<Model %s(pk=%s)>' % (self.__class__.__name__, self.pk)

    def remove(self):
        for field in self._fields.values():
            field._remove()


# All model fields inherited from this
class ModelField:
    # _model = None  # type: Model
    # _name = None  # type: str
    _directly_redis_helpers = ()  # Direct method helpers
    _field_type_name = '--'

    def __init__(self, **kwargs):
        if 'instance' in kwargs:
            self._name = kwargs['name']
            self._model = kwargs['model']
        self._options = kwargs

    def _get_key_name(self, field=None):
        """
        Метод для получения ключа поля/хэша или сета. Формируется как
        prefix::object_name::field_type::id::field_name, например:
            prefix::user::table::12::login
            prefix::author::hash::54
        """
        parent_class_name = self._model.__class__.__name__.lower()
        items = [self._model.prefix, parent_class_name, self._field_type_name,
                 str(self._model.pk)]
        if field:
            items.append(field)
        return '::'.join(items)

    def _assign(self, value):
        raise NotImplementedError("Subclasses must implement _assign")

    def _obtain(self):
        raise NotImplementedError("Subclasses must implement _obtain")

    def _helper(self, method_name):
        if method_name not in self._directly_redis_helpers:
            raise AttributeError('Invalid attribute with name "%s"'
                                 % (method_name,))
        original_command = getattr(self._model.database, method_name)
        current_key = self._get_key_name(self._name)

        def _method_wrapper(*args, **kwargs):
            new_args = [current_key]
            for v in args:
                new_args.append(v)
            return original_command(*new_args, **kwargs)

        self._value = None  # Reset cached value
        return _method_wrapper

    def _remove(self):
        self._model.database.delete(self._get_key_name())


# Validation rules common between hash and fields
class CharValidatorMixin:
    def _validate(self, value):
        if isinstance(value, bool):  # otherwise we've got "False" as value
            raise ValueError('Invalid type of field %s: %s.' %
                             (self._name, type(value).__name__))
        return value

    def _convert(self, value):
        return value  # may be none if hash is not exists


class BooleanValidatorMixin:
    def _validate(self, value):
        if not isinstance(value, bool):
            raise ValueError('Invalid type of field %s: %s. Expected is bool' %
                             (self._name, type(value).__name__))
        return '1' if bool(value) else '0'

    def _convert(self, value):
        return True if value == '1' else False


class IntegerValidatorMixin:
    def _validate(self, value):
        if not isinstance(value, int):
            raise ValueError('Invalid type of field %s: %s. Expected is int' %
                             (self._name, type(value).__name__))
        return value

    def _convert(self, value):
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None


class DateValidatorMixin:
    """
        We're store only seconds on redis. Using microseconds leads to subtle
        errors:
            import datetime
            datetime.datetime.fromtimestamp(t)
            (2016, 3, 3, 12, 20, 30, 2) when t = 1457007630.000002, but
            (2016, 3, 3, 12, 20, 30) when t = 1457007630.000001
        """

    def _validate(self, value):
        if not isinstance(value, dt.datetime) and not \
                isinstance(value, dt.date):
            raise ValueError('Invalid type of field %s: %s. Expected '
                             'is datetime.datetime or datetime.date' %
                             (self._name, type(value).__name__))

        # return round(value.timestamp())  # without microseconds
        return value.strftime("%s")  # both class implements it

    def _convert(self, value):
        if not value:
            return value
        try:
            value = int(value)
        except ValueError:
            return None
        # TODO: maybe use utcfromtimestamp?.
        return dt.date.fromtimestamp(value)


class DateTimeValidatorMixin(DateValidatorMixin):
    def _convert(self, value):
        if not value:
            return value
        try:
            value = int(value)
        except ValueError:
            return None
        # TODO: maybe use utcfromtimestamp?.
        return dt.datetime.fromtimestamp(value)


class EnumValidatorMixin:
    def __init__(self, enum=list(), **kwargs):
        if 'instance' not in kwargs:
            # Instant when user define EnumHash. Definition test
            if len(enum) < 1:
                raise AttributeError("You're must define enum list")
            for item in enum:
                if not isinstance(item, str) or item == '':
                    raise ValueError("Enum list item must be string")
        self._enum = enum
        super().__init__(enum=enum, **kwargs)

    def _validate(self, value):
        if value not in self._enum:
            raise ValueError('This value is not enumerate')
        return value

    def _convert(self, value):
        return value if value in self._enum else None


class ForeignObjectValidatorMixin:
    def __init__(self, to, **kwargs):
        super().__init__(to=to, **kwargs)  # Load other first
        self._to = None
        if 'instance' not in kwargs:
            # First check
            if not isinstance(to, str) and not isinstance(to, Model):
                raise AttributeError("You're must define to as string"
                                     " or Model class")
        else:
            # When object constructed, relation model can be loaded
            to_path = to.split('.')
            object_rel = to_path.pop()
            package_rel = '.'.join(to_path)
            if package_rel not in sys.modules.keys():
                package_rel = self._model.__class__.__module__
            if package_rel not in sys.modules.keys():
                raise AttributeError('Package "%s" is not loaded yet' % (to,))
            try:
                self._to = getattr(sys.modules[package_rel], object_rel)
            except AttributeError:
                pass  # TODO

    def _validate(self, value):
        if isinstance(value, bool):
            raise ValueError('Invalid type of field %s: %s.' %
                             (self._name, type(value).__name__))
        return value

    def _convert(self, value):
        return value


# Fields:
class BaseField(ModelField):
    _field_type_name = 'fld'

    def __init__(self, **kwargs):
        self._value = None
        super().__init__(**kwargs)

    def _assign(self, value):
        if value is None:
            raise ValueError("You're cannot save None value for %s"
                             % (self._name,))
        self._value = self._validate(value)
        self._model.database.set(self._get_key_name(self._name), self._value)

    def _obtain(self):
        if self._value is None:
            self._value = self._convert(self._model.database.get(
                self._get_key_name(self._name)))
        return self._value

    def _validate(self, value):
        """ Check saved value before send to server """
        raise NotImplementedError("Subclasses must implement _validate")

    def _convert(self, value):
        """ Convert server answer to user type """
        raise NotImplementedError("Subclasses must implement _convert")


class CharField(CharValidatorMixin, BaseField):
    _directly_redis_helpers = ('setex', 'setnx', 'append', 'bitcount',
                               'getbit', 'getrange', 'setbit', 'setrange',
                               'strlen',)


class BooleanField(BooleanValidatorMixin, BaseField):
    _directly_redis_helpers = ('setex', 'setnx',)


class IntegerField(IntegerValidatorMixin, BaseField):
    _directly_redis_helpers = ('setex', 'setnx', 'incr', 'incrby', 'decr',
                               'decrby', 'getset',)


class ForeignKey(ForeignObjectValidatorMixin, BaseField):
    def _assign(self, value):
        """
        Support remove hash field if None passed as value
        """
        if isinstance(value, Model):
            super()._assign(value.pk)
        elif value is None:
            self._model.database.delete(self._get_key_name(self._name))
            self._value = None
        else:
            super()._assign(value)

    def _obtain(self):
        """
        Convert saved pk to target object
        """
        if not self._to:
            raise RuntimeError('Relation model is not loaded')
        value = super()._obtain()
        return None if value is None else self._to(value)


class DateField(DateValidatorMixin, BaseField):
    _directly_redis_helpers = ('setex', 'setnx',)


class DateTimeField(DateTimeValidatorMixin, BaseField):
    _directly_redis_helpers = ('setex', 'setnx',)


# Hashes
class BaseHash(ModelField):
    _field_type_name = 'hash'

    def __init__(self, **kwargs):
        self._updated = False
        super().__init__(**kwargs)

    def _assign(self, value):
        if value is None:
            raise ValueError("You're cannot save None value for %s"
                             % (self._name,))
        saved_value = self._validate(value)
        self._model.database.hset(self._get_key_name(),
                                  self._name, saved_value)
        if self._model._hash_loaded:
            self._model._hash[self._name] = saved_value

    def _obtain(self):
        self._load_hash()
        return self._convert(self._model._hash.get(self._name, None))

    def _load_hash(self):
        if self._model._hash_loaded:
            return
        self._model._hash_loaded = True
        self._model._hash = \
            self._model.database.hgetall(
                self._get_key_name())
        if not self._model._hash:  # None if hash field is not exist
            self._model._hash = {}

    def _validate(self, value):
        """ Check saved value before send to server """
        raise NotImplementedError("Subclasses must implement _validate")

    def _convert(self, value):
        """ Convert server answer to user type """
        raise NotImplementedError("Subclasses must implement _convert")


class CharHash(CharValidatorMixin, BaseHash):
    pass


class BooleanHash(BooleanValidatorMixin, BaseHash):
    pass


class IntegerHash(IntegerValidatorMixin, BaseHash):
    pass


class DateHash(DateValidatorMixin, BaseHash):
    pass


class DateTimeHash(DateTimeValidatorMixin, BaseHash):
    pass


class EnumHash(EnumValidatorMixin, BaseHash):
    pass


class ForeignKeyHash(ForeignObjectValidatorMixin, BaseHash):
    def _assign(self, value):
        """
        Support remove hash field if None passed as value
        """
        if isinstance(value, Model):
            super()._assign(value.pk)
        elif value is None:
            self._model.database.hdel(self._get_key_name(), self._name)
            if self._model._hash_loaded:
                del self._model._hash[self._name]
        else:
            super()._assign(value)

    def _obtain(self):
        """
        Convert saved pk to target object
        """
        if not self._to:
            raise RuntimeError('Relation model is not loaded')
        value = super()._obtain()
        return None if value is None else self._to(value)


# Implements for three types of lists
class BaseCollection(ForeignObjectValidatorMixin, ModelField):
    _field_type_name = ''
    _allowed_redis_methods = ()
    _single_object_answered_redis_methods = ()
    _list_answered_redis_methods = ()
    # Other methods will be answered directly

    def _obtain(self):
        return self  # for delegate to __getattr__ on this class

    def _assign(self, value):
        if value is None:
            self._remove()
        else:
            raise ValueError("Collections fields is not possible "
                             "assign directly")

    def __getattr__(self, item):
        if item not in self._allowed_redis_methods:
            return super().__getattribute__(item)

        original_command = getattr(self._model.database, item)
        current_key = self._get_key_name(self._name)

        def _method_wrapper(*args, **kwargs):
            # Scan passed args and convert to models is possible
            new_args = [current_key]
            new_kwargs = dict()
            for v in args:
                new_args.append(v.pk if isinstance(v, Model) else v)
            for k, v in kwargs.items():
                new_kwargs[k] = v.pk if isinstance(v, Model) else v

            # Call original method on the database
            answer = original_command(*new_args, **new_kwargs)

            if item in self._single_object_answered_redis_methods:
                return None if not answer else self._to(answer)
            if item in self._list_answered_redis_methods:
                wrapper_answer = []
                for pk in answer:
                    wrapper_answer.append(None if not pk else self._to(pk))

                return wrapper_answer
            return answer  # Direct answer

        return _method_wrapper

    def _remove(self):
        self._model.database.delete(self._get_key_name(self._name))


class List(BaseCollection):
    """
    :type lpush: attribute
    """
    _field_type_name = 'list'
    _allowed_redis_methods = ('lindex', 'linsert', 'llen', 'lpop', 'lpush',
                              'lpushx', 'lrange', 'lrem', 'lset', 'ltrim',
                              'rpop', 'rpoplpush', 'rpush', 'rpushx',)
    _single_object_answered_redis_methods = ('lindex', 'lpop', 'rpop',)
    _list_answered_redis_methods = ('lrange',)

    def __len__(self):
        return self.llen()

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.lrange(item.start, item.stop)
        else:
            ret = self.lrange(item, item)
            return ret[0] if len(ret) == 1 else None


class Set(BaseCollection):
    _field_type_name = 'set'
    _allowed_redis_methods = ('sadd', 'scard', 'sdiff', 'sdiffstore', 'sinter',
                              'sinterstore', 'sismember', 'smembers', 'smove',
                              'spop', 'srandmember', 'srem', 'sscan', 'sunion',
                              'sunionstore')
    _single_object_answered_redis_methods = ('spop',)
    _list_answered_redis_methods = ('sdiff', 'sinter', 'smembers',
                                    'srandmember', 'sscan', 'sunion',)

    def __len__(self):
        return self.scard()


class SortedSet(BaseCollection):
    _field_type_name = 'zset'
    _allowed_redis_methods = ('zadd', 'zcard', 'zcount', 'zincrby',
                              'zinterstore', 'zlexcount', 'zrange',
                              'zrangebylex', 'zrangebyscore', 'zrank', 'zrem',
                              'zremrangebylex', 'zremrangebyrank',
                              'zremrangebyscore', 'zrevrange',
                              'zrevrangebylex', 'zrevrangebyscore', 'zrevrank',
                              'zscan', 'zscore', 'zunionstore')
    _single_object_answered_redis_methods = ()
    _list_answered_redis_methods = ('zrange', 'zrangebylex', 'zrangebyscore',
                                    'zrevrange', 'zrevrangebylex',
                                    'zrevrangebyscore', 'zscan', )

    def __len__(self):
        return self.zcard()

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.zrangebyscore(item.start or '-inf',
                                      item.stop or '+inf')
        else:
            ret = self.zrangebyscore(item, item)
            return ret[0] if len(ret) == 1 else None
