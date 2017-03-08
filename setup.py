from setuptools import setup, find_packages

# Box requirments:
# clean minimal ubuntu
# aptitude install virtualenv
# (aptitude install git)
# (aptitude install nfs-kernel-server)
#   .: nano /etc/exports
# aptitude install python-dev
# aptitude install mysql-server
# aptitude install python-mysqldb

requires = [
    'SQLAlchemy>=1.1.0',
    'MySQL-python>=1.2.5',
    'simplejson>=3.8.2'
]

setup(name='node',
      version='1.0',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires
      )
