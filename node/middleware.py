# -*- coding: utf-8 -*-
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, event
from db_util import DBUtil

# conf.yaml
# db_url: mysql://root@localhost:3306/
# db_name: mydb
# db_charset: utf8mb4
# db_collate: utf8mb4_unicode_ci
# engine_params:
#     pool_recycle: 3600
#     echo: False

class NodeMiddleware(object):

    def __init__(self, app, environ_key='node.session', **kws):
        self.app = app
        self.config = kws
        self.environ_key = environ_key
        self.sessionmaker = sessionmaker(autoflush=False)
        self.engine = create_engine(DBUtil.get_engine_url(self.config), **self.config.get('engine_params'))
        self.sessionmaker.configure(bind=self.engine)

        def on_engine_connect(dbapi_conn, conn_record):
            print "on_engine_connect() SET charset & collate"
            cursor = dbapi_conn.cursor()
            cursor.execute("SET NAMES '{0}' COLLATE '{1}'".format(self.config.get('db_charset'), self.config.get('db_collate')))
            cursor.execute("SET CHARACTER SET {0}".format(self.config.get('db_charset')))
            cursor.execute("SET character_set_connection={0}".format(self.config.get('db_charset')))

        event.listen(self.engine, 'connect', on_engine_connect)

    def __call__(self, environ, start_response):
        try:
            # get session
            environ[self.environ_key] = self.sessionmaker()
            # wsgi call
            response = self.app(environ, start_response)
            # return response
            return response
        finally:
            # close session
            environ[self.environ_key].close()
