# -*- coding: utf-8 -*-
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

class NodeMiddleware(object):

    def __init__(self, app, **kw):
        self.app = app
        self.sessionmaker = sessionmaker(autoflush=False)
        conf = kw.copy()
        url = conf.get('url')
        del conf['url']

        engine = create_engine(url, **conf)
        self.sessionmaker.configure(bind=engine)


    def __call__(self, environ, start_response):
        environ['node.session'] = self.sessionmaker()
        response = self.app(environ, start_response)
        environ['node.session'].close()
        return response

