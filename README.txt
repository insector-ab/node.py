node README

Test package:

.: python setup.py develop
.: pip install ipython
.: mysql -u root
    .: create database nodes
.: ipython
    .: import node.init
    .: from node.db_util import *
    .: create_tables()