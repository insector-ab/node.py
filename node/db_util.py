# -*- coding: utf-8 -*-
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, event

from node import Base

# conf.yaml
# db_url: mysql://root@localhost:3306/
# db_name: mydb
# db_charset: utf8mb4
# db_collate: utf8mb4_unicode_ci
# engine_params:
#     pool_recycle: 3600
#     echo: False

class DBUtil(object):

    def __init__(self, conf, *args, **kw):
        assert 'db_url' in conf
        assert 'db_name' in conf
        # defaults
        self.config = {
            u'db_charset': u'utf8mb4',
            u'db_collate': u'utf8mb4_unicode_ci'
        }
        self.config.update(conf)

    def init_sessionmaker(self):
        self.sessionmaker = sessionmaker(autoflush=False)
        self.engine = create_engine(self.get_engine_url(self.config), **self.config.get('engine_params', {}))
        self.sessionmaker.configure(bind=self.engine)

        def on_engine_connect(dbapi_conn, conn_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("SET NAMES '{db_charset}' COLLATE '{db_collate}'".format(**self.config))
            cursor.execute("SET CHARACTER SET {db_charset}".format(**self.config))
            cursor.execute("SET character_set_connection={db_charset}".format(**self.config))

        event.listen(self.engine, 'connect', on_engine_connect)

    def recreate_db(self):
        assert 'db_url' in self.config
        assert 'db_name' in self.config

        # connect to DB
        conn = create_engine(self.get_engine_url(self.config, include_db_name=False)).connect()
        try:
            conn.execute("DROP DATABASE {db_name}".format(**self.config))
        except Exception:
            pass
        default_character = "DEFAULT CHARACTER SET {db_charset}".format(**self.config)
        default_collate = "DEFAULT COLLATE {db_collate}".format(**self.config)
        conn.execute("CREATE DATABASE {0} {1} {2}".format(self.config.get('db_name'), default_character, default_collate))
        conn.close()
        print "Recreated database: {db_name}".format(**self.config)

    def recreate_tables(self):
        sess = self.new_session()
        self.drop_tables(sess)
        self.create_tables(sess)
        sess.close()

    def create_tables(self, sess):
        Base.metadata.create_all(checkfirst=True, bind=sess.bind)
        sess.commit()
        print "Tables created"

    def drop_tables(self, sess):
        Base.metadata.drop_all(bind=sess.bind)
        sess.commit()
        print "Tables dropped"

    def new_session(self):
        return self.sessionmaker()

    @classmethod
    def get_engine_url(cls, conf, include_db_name=True):
        # mysql://root@localhost:3306/DB_NAME?charset=DB_CHARSET
        return u'{0}{1}?charset={2}'.format(conf.get('db_url'), conf.get('db_name') if include_db_name else '', conf.get('db_charset'))
