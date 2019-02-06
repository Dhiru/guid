'''
Installation required tornado, redis and sqlite3 via pip install <app>
execute command: python guid.py
browse on 127.0.01:8888/guid
'''


import tornado.ioloop
import tornado.web
import sqlite3
import uuid
import time
import json
import redis
from datetime import datetime, timedelta
from cache import RedisCacheBackend, CacheMixin


def dict_factory(cursor, row):
    """convert sql object to dictionary"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def _execute(query):
    """
    database connection and execute query
    :param query: query to be execute
    :return: query result
    """
    dbPath = 'guid.sqlite3'
    connection = sqlite3.connect(dbPath)
    connection.row_factory = dict_factory
    cursorobj = connection.cursor()
    try:
        cursorobj.execute(query)
        result = cursorobj.fetchall()
        connection.commit()
    except sqlite3.IntegrityError:
        raise tornado.web.HTTPError(500, "Already exist")
    except Exception:
        raise tornado.web.HTTPError(500)
    connection.close()
    return result

def validate_uuid4(uuid_string):
    """
    Validate that a UUID string is in
    fact a valid uuid4.
    Happily, the uuid module does the actual
    checking for us.
    It is vital that the 'version' kwarg be passed
    to the UUID() call, otherwise any 32-character
    hex string is considered valid=
    """
    try:
        val = uuid.UUID(uuid_string, version=4)
    except ValueError:
        # If it's a value error, then the string
        # is not a valid hex code for a UUID.
        return False
    return val.hex == uuid_string

def generate_uuid():
    """
    generate uuid
    """
    return uuid.uuid4().hex.upper()

def get_default_expire_date():
    """
    :return: default expire date for GUID
    """
    ex_date = datetime.now() + timedelta(days=30)
    return time.mktime(ex_date.timetuple())


class MainHandler(CacheMixin, tornado.web.RequestHandler):
    def get(self, slug=""):
        self.expires = 60
        if slug:
            query = ''' select guid, user, expire from table_guid where guid='%s' ''' % slug
        else:
            query = ''' select guid, user, expire from table_guid '''
        result = _execute(query)
        if len(result) == 0:
            raise tornado.web.HTTPError(404)
        # filter for expired object
        result = list(filter(lambda x: datetime.fromtimestamp(x['expire']) > datetime.now(), result))
        if len(result) == 0:
            self.write("expired")
        self.write(json.dumps(result))

    def post(self, slug=""):
        body = eval(self.request.body)
        guid = slug or generate_uuid()
        if validate_uuid4(guid.lower()) is False:
            raise tornado.web.HTTPError(400)
        if "user" in body:
            user = body["user"]
        else:
            raise tornado.web.HTTPError(400)
        if "expire" in body:
            expire = body["expire"]
        else:
            expire = get_default_expire_date()
        query = ''' insert into table_guid (guid, user, expire) values ('%s', '%s', '%s'); ''' % (guid, user, expire)
        _execute(query)
        self.finish()

    def put(self, slug=""):
        query_get = ''' select guid, user, expire from table_guid where guid='%s' ''' % slug
        result = _execute(query_get)
        body = eval(self.request.body)
        r_user = r_expire = ""
        if "user" in body:
            r_user = body["user"]
        if "expire" in body:
            r_expire = body["expire"]
        user = r_user or result[0]['user']
        expire = r_expire or result[0]['expire']
        query = ''' update table_guid set user='%s', expire='%s' where guid='%s' ''' % (user, expire, slug)
        _execute(query)
        self.finish()

    def delete(self, slug=""):
        if not slug:
            raise tornado.web.HTTPError(400)
        query = ''' delete from table_guid where guid='%s' ''' % slug
        _execute(query)
        self.finish()


class Application(tornado.web.Application):

    def __init__(self):

        settings = dict(debug=True)
        self.redis = redis.Redis()
        self.cache = RedisCacheBackend(self.redis)
        handlers = [(r"/guid", MainHandler), (r"/guid/([^/]+)", MainHandler)]
        super(Application, self).__init__(handlers=handlers, **settings)


application = tornado.web.Application([
    (r"/guid", MainHandler),
    (r"/guid/([^/]+)", MainHandler),
])

if __name__ == "__main__":
    application = Application()
    application.listen(8888)
    tornado.ioloop.IOLoop.current().start()
