import redis
import schematics

import redisorm
import redisorm.types.compound



class C(redisorm.models.Model):
    name = redisorm.types.StringHash() 

class B(redisorm.models.Model):
    name = redisorm.types.StringHash()
    # c = redisorm.types.compound.ModelType(C)


class A(redisorm.models.Model):
    test = redisorm.types.compound.ModelType(B)
    aa = redisorm.types.compound.DictType(redisorm.types.StringHash)
    bb = redisorm.types.compound.DictType(redisorm.types.StringHash)


print(isinstance(redisorm.types.StringHash, schematics.types.BaseType))
a = A(pk=1)


c = C(pk=1)
c.name =  "123123"
xxx = B(pk=1)
xxx.c  = c
xxx.name = "111"
a.test = xxx
a.aa = {"dddd":"123123"}
a.bb = {"dddd":"123123"}

r = redis.StrictRedis("10.20.78.72")
aa = A(pk=1)

a.save(db=r)
import sys
print(sys.version)
c= A(pk=1)
p=  r.pipeline()
c.load(p)
result = p.execute()
c.load_pipe_result(result)
# c.save(r)
print (c.aa)
print(c.test.name)