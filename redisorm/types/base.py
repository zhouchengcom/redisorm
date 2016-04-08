from schematics.types import StringType, IntType, BaseType

from schematics.exceptions import ConversionError, ValidationError

import re


try:
    unicode #PY2
except:
    import codecs
    unicode = str #PY3
    
class Hash():
    def __init__(self):
        self.pkey = None
        self.skey = None
    
class Field():
    def __init__(self):
        self.pkey = None
        
        
        
        
class ByteType(BaseType):

    """A unicode string field. Default minimum length is one. If you want to
    accept empty strings, init with ``min_length`` 0.
    """

    allow_casts = (int, str, bytes)

    MESSAGES = {
        'convert': u"Couldn't interpret '{0}' as string.",
        'max_length': u"String value is too long.",
        'min_length': u"String value is too short.",
        'regex': u"String value did not match validation regex.",
    }

    def __init__(self, regex=None, max_length=None, min_length=None, **kwargs):
        self.regex = regex
        self.max_length = max_length
        self.min_length = min_length

        super(ByteType, self).__init__(**kwargs)

    # def _mock(self, context=None):
    #     return random_string(get_value_in(self.min_length, self.max_length))

    def to_native(self, value, context=None):
        if value is None:
            return None

        if not isinstance(value, unicode):
            if isinstance(value, self.allow_casts):
                if not isinstance(value, str):
                    value = str(value)
                # value = utf8_decode(value) #unicode(value, 'utf-8')
            else:
                raise ConversionError(self.messages['convert'].format(value))

        return value

    def validate_length(self, value):
        len_of_value = len(value) if value else 0

        if self.max_length is not None and len_of_value > self.max_length:
            raise ValidationError(self.messages['max_length'])

        if self.min_length is not None and len_of_value < self.min_length:
            raise ValidationError(self.messages['min_length'])

    def validate_regex(self, value):
        if self.regex is not None and re.match(self.regex, value) is None:
            raise ValidationError(self.messages['regex'])


class IntegerCountField(IntType, Field):

    def save(self, db, pk, value):
        return db.incr(self.pkey % pk, value)
    
    def load(self, db, pk):
        return db.incr(self.pkey % pk)



class HyperloglogField(BaseType, Field):
    def save(self, db, pk, value):
        print(type(value), value)
        print("ddddddddddddddddddddd")
        return db.pfadd(self.pkey % pk, *value)
    
    def load(self, db, pk):
        return db.pfcount(self.pkey % pk)
        
        
class StringHash(ByteType, Hash):
    def save(self, db, pk, value):
        return db.hset(self.pkey % pk, self.skey, value)
    
    def load(self, db, pk):
        return db.hget(self.pkey % pk, self.skey)
        
        