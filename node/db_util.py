# -*- coding: utf-8 -*-
from node import Base
from node.model import *

def create_tables(Session):
    Base.metadata.create_all(checkfirst=True, bind=Session.bind)
    Session.commit()
    print "Tables created"


def drop_tables(Session):
    Base.metadata.drop_all(bind=Session.bind)
    Session.commit()
    print "Tables dropped"
