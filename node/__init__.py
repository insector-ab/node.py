# -*- coding: utf-8 -*-
from sqlalchemy.ext.declarative import declarative_base

__all__ = ['Base']

# Node(Base)
Base = declarative_base()


# from sqlalchemy.orm import scoped_session, sessionmaker
# from sqlalchemy import engine_from_config

# # Session manager
# Session = scoped_session(sessionmaker(autoflush=False))

# config = {  'sqlalchemy.url':'mysql://root@localhost:3306/nodes',
#             'sqlalchemy.pool_recycle':3600,
#             'sqlalchemy.echo':False}

# # Configure engine
# engine = engine_from_config(config, 'sqlalchemy.')
# Session.configure(bind=engine)



# from sqlalchemy.orm import sessionmaker
# from sqlalchemy import create_engine

# # configure Session class with desired options
# Session = sessionmaker()

# # later, we create the engine
# engine = create_engine('postgresql://...')

# # associate it with our custom Session class
# Session.configure(bind=engine)

# # work with the session
# session = Session()