# -*- coding: utf-8 -*-
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from node import Base

class DBUtil(object):

    def __init__(self, conf, *args, **kw):
        self.config = conf
        self.sessionmaker = sessionmaker(autoflush=False)
        url = u'{0}{1}'.format(self.config.get('url'), self.config.get('db_name'))
        self.engine = create_engine(url, **self.config.get('engine'))
        self.sessionmaker.configure(bind=self.engine)

    def recreate_db(self):
        # connect to DB
        conn = create_engine(self.config.get('url')).connect()
        try:
            conn.execute("DROP DATABASE {0}".format(self.config.get('db_name')))
        except Exception:
            pass
        default_character = "DEFAULT CHARACTER SET utf8"
        default_collate = "DEFAULT COLLATE utf8_general_ci"
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
