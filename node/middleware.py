# -*- coding: utf-8 -*-
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

class NodeMiddleware(object):

    def __init__(self, app, environ_key='node.session', **kws):
        self.app = app
        self.environ_key = environ_key
        self.sessionmaker = sessionmaker(autoflush=False)
        conf = kws.copy()
        url = conf.get('url')
        del conf['url']

        engine = create_engine(url, **conf)
        self.sessionmaker.configure(bind=engine)


    def __call__(self, environ, start_response):
        # get session
        environ[self.environ_key] = self.sessionmaker()
        # wsgi call
        response = self.app(environ, start_response)
        # close session
        environ[self.environ_key].close()
        # return response
        return response

