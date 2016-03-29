node README

Test package:

.: python setup.py develop
.: pip install ipython
.: ipython
    from node.db_util import DBUtil
    dbutil = DBUtil(config) # config object
    dbutil.recreate_db()
    dbutil.recreate_tables()

-----------------------

config.yaml

sqlalchemy:
    url: mysql://root@localhost:3306/
    db_name: DB_NAME
    engine: 
        pool_recycle: 3600
        echo: False

-----------------------