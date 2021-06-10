import datetime
from dateutil.parser import parse
import math

from pymongo import MongoClient
from ooop import OOOP

from check import run_test, push_test
from config import config

NAMES = ['cchfact','cchval', 'f1', 'p1', 'cch_autocons', 'cch_gennetabeta']


def size_to_human(size):
   size_name= ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i= int(math.floor(math.log(size, 1024)))
   p= math.pow(1024, i)
   s= round(size/p,2)
   if (s > 0):
       return '%s %s' % (s, size_name[i])
   else:
       return '0B'


class CCHStats(object):
    names = ['cchfact','cchval', 'f1', 'p1', 'cch_autocons', 'cch_gennetabeta']
    def __init__(self, db, erp):
        self.db = db
        self.erp = erp

    def get_n_entries(self, name, start=None):
        if name not in self.names:
            raise NotImplementedError()
        tg_name = 'tg_%s' % name

        if not start:
            return db.somenergia[tg_name].count()
        else:
            return db.somenergia[tg_name].find({'create_at': {'$gte': start}}).count()

    def get_storage(self, name):
        if name not in self.names:
            raise NotImplementedError()
        tg_name = 'tg_%s' % name

        stats = self.db.somenergia.command('collstats', tg_name)
        return {
            'avgObjSize': size_to_human(stats['avgObjSize']),
            'count': stats['count'],
            'size': size_to_human(stats['size']),
            'storageSize': size_to_human(stats['storageSize'])
        }

    def get_providers(self, name):
        cch_to_name = {
            'cchfact': ['f5d_enabled', 'f5d_date'],
            'cchval': ['p5d_enabled', 'p5d_date'],
            'f1': ['f1_enabled', 'f1_date'],
            'p1': ['p1_enabled', 'p1_date'],
            'cch_autocons': ['a5d_enabled','a5d_date'],
            'cch_gennetabeta': ['b5d_enabled', 'b5d_date']
        }
        fields_to_read = ['name', 'distribuidora', 'enabled']
        fields_to_read += cch_to_name[name]

        provider_ids = self.erp.TgComerProvider.search([])
        return self.erp.TgComerProvider.read(provider_ids, fields_to_read)


    def get_update_providers(self, name):
        cch_to_name = {
            'cchfact': 'f5d_date',
            'cchval': 'p5d_date',
            'f1': 'f1_date',
            'p1': 'p1_date',
            'cch_autocons': 'a5d_date',
            'cch_gennetabeta': 'b5d_date'
        }
        providers = self.get_providers(name)
        return [{'provider': provider['name'], 'name': name, 'date': provider[cch_to_name[name]]}
            for provider in providers
            if provider[cch_to_name[name]] and not parse(provider[cch_to_name[name]]).date() == datetime.datetime.today().date()]


conn_str = 'mongodb://{username}:{password}@{hostname}/{dbname}?replicaSet={replica}' \
    if 'replica' in config['mongodb'] \
    else 'mongodb://{username}:{password}@{hostname}/{dbname}'

db = MongoClient(conn_str.format(
    **{'username': config['mongodb']['username'],
       'password': config['mongodb']['password'],
       'hostname': config['mongodb']['hostname'],
       'port': config['mongodb']['port'],
       'dbname': config['mongodb']['dbname'],
       'replica': config['mongodb'].get('replica'),
}))

O = OOOP(dbname=config['erp']['dbname'],
         user=config['erp']['username'],
         pwd=config['erp']['password'],
         port=config['erp']['port'],
         uri=config['erp']['hostname'])

cch_stats = CCHStats(db, O)

today = datetime.datetime.today()
today_sof = datetime.datetime(today.year, today.month, today.day, 0, 0, 0)

out = {name: {} for name in NAMES}
results = []
for name in NAMES:
    results.append(run_test(name, cch_stats.get_n_entries,[name]))
    results.append(run_test(name, cch_stats.get_n_entries,[name, today_sof]))
    results.append(run_test(name, cch_stats.get_storage,[name]))
    results.append(run_test(name, cch_stats.get_providers,[name]))
    results.append(run_test(name, cch_stats.get_update_providers,[name]))
push_test('CCH loader status %s' % datetime.datetime.now(), results)
