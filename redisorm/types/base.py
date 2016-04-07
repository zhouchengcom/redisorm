from schematics.types import StringType, IntType, BaseType



class Hash():
    def __init__(self):
        self.pkey = None
        self.skey = None
    
class Field():
    def __init__(self):
        self.pkey = None
        

class IntegerCountField(IntType, Field):

    def save(self, db, pk, value):
        return db.incr(self.pkey % pk, value)
    
    def load(self, db, pk):
        return db.incr(self.pkey % pk)



class HyperloglogField(BaseType, Field):
    def save(self, db, pk, value):
        return db.padd(self.pkey % pk, *value)
    
    def load(self, db, pk):
        return db.pfcount(self.pkey % pk)
        
        
class StringHash(StringType, Hash):
    def save(self, db, pk, value):
        return db.hset(self.pkey % pk, self.skey, value)
    
    def load(self, db, pk):
        return db.hget(self.pkey % pk, self.skey)
        