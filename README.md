
redisorm
======

Redisorm  is Python light ORM for Redis

Example:
======


    import redis
    from redisorm.models import Model
    from redisorm.types import *
    from redisorm.types.compound import *

    db = redis.StrictRedis(host='127.0.0.1')

    class Person(Model):
        name = StringHash() 
        
    class School(Model):
        name = StringHash()
        principal = ModelType(Person)


So you can use it like this:



    >>> shool = School(pk=1)
    >>> shool.name = 'aa'
    >>> shool.principal = Person(pk=1, raw_data={"name":"Mark"})
    >>> shool.save(db)
    School:1
        name aa
        principal 1
    Person:1
        name Mark



Install
=======

Python versions 3.3, 3.4, 3.5 are supported.

schematics versions >= 1.1.1


    python setup.py install
