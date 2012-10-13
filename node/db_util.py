# -*- coding: utf-8 -*-
from node.init import Session, Base
from node.model import *

def create_tables():
    Base.metadata.create_all(checkfirst=True, bind=Session.bind)
    Session.commit()
    print "Tables created"


def drop_tables():
    Base.metadata.drop_all(bind=Session.bind)
    Session.commit()
    print "Tables dropped"
