# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from node.model import Database


# ===============
# = Put in tea? =
# ===============

"""Work in progress"""

# class DBSession():
# 
#     def __init__(self, config):
#         # SQLAlchemy 
#         url = config['sqlalchemy'].pop('url')
#         engine = create_engine(url, **config['sqlalchemy'])
#         self.Session = sessionmaker(bind=engine)
#         # environ key
#         self.key = config['sqlalchemy'].get('key', 'node.db')
        

# class DBMiddleware(object):
# 
#     def __init__(self, app, dbsession):
#         self.app = app
#         self.Session = dbsession
# 
#     def __call__(self, environ, start_response):
#         # new node db wrapper
#         db = Database(session=self.Session())
#         # make accessible in environ
#         environ[self.key] = db
#         # handle request
#         try:
#             resp = self.app(environ, start_response)
#         finally:
#             db.session.close()
# 
#         return resp
