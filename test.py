from redisorm import models
from  redisorm.types import *
import redis
import uuid
r = redis.StrictRedis("10.20.78.72")
class Test(models.Model):
    namespace = "xxxx"
    aa = IntegerCountField()
    cc = StringHash()
    count = HyperloglogField()



b = Test(1)
# print(Test.aa)
# for v in b.atoms():
#     print(v)
# print(Test(1).__dict__)
# print (Test.aa.pkey)
# print(b.__getattribute__("aa"))
# print(".".join(["", "1"]))

# print(Test.cc.pkey, Test.cc.skey)

# b.aa = 1000
# b.cc = "ddd"
# print (uuid.uuid4().hex)
b.count = [uuid.uuid4().hex]

a = (uuid.uuid4().hex,)
print(a)
def test(b):
    print(b)
    
test(*a)
print (b.count)
b.save(r)
b.load(r)
# r.incr("dfdsf", "2222")c

print(b.aa)
print(b.cc)
print(b.count)