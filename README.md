# node.py

## Install

```sh
pip install git+https://github.com/insector-ab/node.py.git
```

---

`config.yaml`
```
sqlalchemy:
    db_url: mysql://user:pass@mysql:3306/
    db_name: mydatabase
    db_charset: utf8mb4
    db_collate: utf8mb4_unicode_ci
    engine_params:
        pool_recycle: 3600
        echo: false
```
