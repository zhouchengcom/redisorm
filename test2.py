from astra import models
import redis
d= redis.StrictRedis("10.20.78.72")

class SiteObject(models.Model):
    prefix = 'custom_prefix'

    name = models.CharHash()
    database = d


class UserObject(models.Model):
    status_choice = (
        'REGISTERED',
        'ACTIVATED',
        'BANNED',
    )
    database = d

    name = models.CharHash()
    login = models.CharHash()
    rating = models.IntegerHash()
    paid = models.BooleanHash()
    registration_date = models.DateHash()
    last_login = models.DateTimeHash()
    status = models.EnumHash(enum=status_choice)
    inviter = models.ForeignKeyHash(to='tests.sample_model.UserObject')
    site_id = models.ForeignKey(to='SiteObject')

    credits_test = models.IntegerField()
    is_admin = models.BooleanField()  # Other types like hash

    sites_list = models.List(to='tests.sample_model.SiteObject')  # Redis Lists
    sites_set = models.Set(to='tests.sample_model.SiteObject')  # Redis Sets
    sites_sorted_set = models.SortedSet(
        to='tests.sample_model.SiteObject')  # Redis Sorted Sets
        
        
        
# s = SiteObject(pk = 1)
# s.name  = "123123"
 
 
# u = UserObject(pk = 1)
# # u.sites_sorted_set.add(s)

# u.name = "models.CharHash()"
# u.login = "models.CharHash()"
# u.rating = 123
# u.paid = True
# # registration_date = models.DateHash()
# # last_login = models.DateTimeHash()
# u.status = "REGISTERED"
# u.inviter = s
# u.site_id = s




# u.sites_list.lpush(s)
# u.sites_set.sadd(s)
# # u.sites_sorted_set = set([s])

b = UserObject.l