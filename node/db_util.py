# -*- coding: utf-8 -*-
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from node import Base
# from node.model import *

class DBUtil(object):

    def __init__(self, conf, *args, **kw):
        self.config = conf
        self.sessionmaker = sessionmaker(autoflush=False)
        self.engine = create_engine(self.config.get('url'), **self.config.get('engine'))
        self.sessionmaker.configure(bind=self.engine)

    def recreate_db(self):
        conn = self.engine.connect()
        # conn.execute("commit")
        try:
            conn.execute("DROP DATABASE {0}".format(self.config.db_name))
        except Exception:
            pass
        conn.execute("CREATE DATABASE {0}".format(self.config.db_name))
        conn.close()
        print "Recreated database: {0}".format(self.config.db_name)

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
