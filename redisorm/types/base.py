from schematics import types

from schematics.exceptions import ConversionError, ValidationError

import re


try:
    unicode #PY2
except:
    import codecs
    unicode = str #PY3
        
class Field():
    def __init__(self):
        self.pkey = None
        
class Hash(Field):
    def __init__(self):
        self.pkey = None
        self.skey = None
        
        
        
class StringType(types.StringType):

    """A unicode string field. Default minimum length is one. If you want to
    accept empty strings, init with ``min_length`` 0.
    """

    allow_casts = (int, str, bytes)


    def to_native(self, value, context=None):
        if value is None:
            return None

        if not isinstance(value, unicode):
            if isinstance(value, self.allow_casts):
                if isinstance(value, bytes):
                    value = value.decode(encoding='UTF-8')
                if not isinstance(value, str):
                    value = str(value)
                # value = utf8_decode(value) #unicode(value, 'utf-8')
            else:
                raise ConversionError(self.messages['convert'].format(value))

        return value


class IntegerCountField(types.IntType, Field):

    def save(self, db, pk, value):
        return db.incr(self.pkey % pk, value)
    
    def load(self, db, pk):
        return db.incr(self.pkey % pk)



class HyperloglogField(types.BaseType, Field):
    def save(self, db, pk, value):
        return db.pfadd(self.pkey % pk, *value)
    
    def load(self, db, pk):
        return db.pfcount(self.pkey % pk)
        
        
class StringHash(StringType, Hash):
    def save(self, db, pk, value, skey=None):
        return db.hset(self.pkey % pk, skey or self.skey, value)
    
    def load(self, db, pk):
        return db.hget(self.pkey % pk, self.skey)
        
        