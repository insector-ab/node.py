from setuptools import setup, find_packages

setup(
    name='node',
    version='0.1.0',
    description='Graph model lib for SQLAlchemy',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'SQLAlchemy >= 1.3',
        'mysql-connector-python => 8.0.13',
        'simplejson',
        'python-dateutil'
    ]
)
