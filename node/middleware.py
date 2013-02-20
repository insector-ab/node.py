# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from node.model import Database


# ===============
# = Put in tea? =
# ===============

class NodeMiddleware(object):

    def __init__(self, app, config):
        self.app = app
        # SQLAlchemy 
        url = config['sqlalchemy']['url']
        engine = create_engine(url, **config['sqlalchemy'])
        self.Session = sessionmaker(bind=engine)
        # environ key
        self.key = config['sqlalchemy'].get('key', 'node.db')

    def __call__(self, environ, start_response):
        # new node db wrapper
        db = Database(session=self.Session())
        # make accessible in environ
        environ[self.key] = db
        # handle request
        try:
            resp = self.app(environ, start_response)
        finally:
            db.session.close()

        return resp
