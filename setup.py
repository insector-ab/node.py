from setuptools import setup, find_packages

setup(
    name='node',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'SQLAlchemy',
        'MySQL-python',
        'simplejson',
        'dateutil'
    ]
)
