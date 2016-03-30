# -*- coding: utf-8 -*-
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, event

from node import Base

class DBUtil(object):

    def __init__(self, conf, *args, **kw):
        self.config = conf
        self.sessionmaker = sessionmaker(autoflush=False)
        # mysql://root@localhost:3306/DB_NAME?charset=DB_CHARSET
        url = u'{0}{1}?charset={2}'.format(self.config.get('url'), self.config.get('db_name'), self.config.get('db_charset'))
        self.engine = create_engine(url, **self.config.get('engine'))
        self.sessionmaker.configure(bind=self.engine)

        def on_engine_connect(dbapi_conn, conn_record):
            print "on_engine_connect() SET charset & collate"
            cursor = dbapi_conn.cursor()
            cursor.execute("SET NAMES '{0}' COLLATE '{1}'".format(self.config.get('db_charset'), self.config.get('db_collate')))
            cursor.execute("SET CHARACTER SET {0}".format(self.config.get('db_charset')))
            cursor.execute("SET character_set_connection={0}".format(self.config.get('db_charset')))

        event.listen(self.engine, 'connect', on_engine_connect)

    def recreate_db(self):
        # connect to DB
        conn = create_engine(self.config.get('url')).connect()
        try:
            conn.execute("DROP DATABASE {0}".format(self.config.get('db_name')))
        except Exception:
            pass
        default_character = "DEFAULT CHARACTER SET {0}".format(self.config.get('db_charset'))
        default_collate = "DEFAULT COLLATE {0}".format(self.config.get('db_collate'))
        conn.execute("CREATE DATABASE {0} {1} {2}".format(self.config.get('db_name'), default_character, default_collate))
        conn.close()
        print "Recreated database: {0}".format(self.config.get('db_name'))

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
