# -*- coding: utf-8 -*-
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import engine_from_config

__all__ = ['Base', 'Session']

# Session manager
Session = scoped_session(sessionmaker(autoflush=False))

# Node(Base)
Base = declarative_base()

config = {  'sqlalchemy.url':'mysql://root@localhost:3306/node',
            'sqlalchemy.pool_recycle' = 3600,
            'sqlalchemy.echo':False}

# Configure engine
engine = engine_from_config(config, 'sqlalchemy.')
Session.configure(bind=engine)


def create_db_tables():
    # sync all tables
    Base.metadata.create_all(checkfirst=True, bind=Session.bind)
    Session.commit()
