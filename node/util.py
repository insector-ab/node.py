# -*- coding: utf-8 -*-
from uuid import UUID
import sys
import simplejson
import inspect
from sqlalchemy import UnicodeText
from sqlalchemy.orm import EXT_CONTINUE
from sqlalchemy.orm.interfaces import MapperExtension
from sqlalchemy.types import TypeDecorator

class ConstantMeta(type):
    """Collects unicode constants set on class"""
    def __init__(cls, class_name, bases, class_dict):
        cls.values = []
        cls.keys = {}
        for k, v in class_dict.iteritems():
            if type(v) == unicode or type(v) == int:
                cls.values.append(v)
                cls.keys[v] = k

# class UnicodeConstantMeta(type):
#     """Collects unicode constants set on class"""
#     def __init__(cls, class_name, bases, class_dict):
#         cls.values = []
#         for k,v in class_dict.iteritems():
#             if type(v) == unicode:
#                 cls.values.append(v)

def get_subclasses(cls):
    subclasses = cls.__subclasses__()
    for d in list(subclasses):
        subclasses.extend(get_subclasses(d))
    return subclasses

def get_discriminator_map(cls):
    obj = {}
    for c in get_subclasses(cls):
        obj[c.get_polymorphic_identity()] = c

    return obj

def get_discriminators(*args, **kws):
    include_subclasses = kws.get('include_subclasses', True)
    items = []
    for cls in args:
        items.append(cls.get_polymorphic_identity())
        if include_subclasses:
            for subcls in get_subclasses(cls):
                items.append(subcls.get_polymorphic_identity())
    if len(items) > 0:
        return items
    return None

InputMismatchError = TypeError("Inputs must be both unicode or both bytes")

# Always takes the same time comparing a & b
# regardless of similarity. Prevents learning
# when brute forcing password.
def constant_time_compare(a, b):
    if isinstance(a, unicode):
        if not isinstance(b, unicode):
            raise InputMismatchError
        is_py3_bytes = False
    elif isinstance(a, bytes):
        if not isinstance(b, bytes):
            raise InputMismatchError
        is_py3_bytes = sys.version_info >= (3, 0)
    else:
        raise InputMismatchError

    result = 0
    if is_py3_bytes:
        for x, y in zip(a, b):
            result |= x ^ y
    else:
        for x, y in zip(a, b):
            result |= ord(x) ^ ord(y)
    return result == 0

def validate_uuid4(uuid_string):
    """
    Validate that a UUID string is in
    fact a valid uuid4.
    Happily, the uuid module does the actual
    checking for us.
    It is vital that the 'version' kwarg be passed
    to the UUID() call, otherwise any 32-character
    hex string is considered valid.
    """
    try:
        uuid_string = str(uuid_string)
        val = UUID(uuid_string, version=4)
    except ValueError:
        # If it's a value error, then the string
        # is not a valid hex code for a UUID.
        return False

    # If the uuid_string is a valid hex code,
    # but an invalid uuid4,
    # the UUID.__init__ will convert it to a
    # valid uuid4. This is bad for validation purposes.
    return str(val) == uuid_string

class Callback(MapperExtension):
    """ Extention to add pre-commit hooks.

    __mapper_args__ = {'polymorphic_on': discriminator, 'extension':Callback(), 'polymorphic_identity': u'node'}


    Hooks will be called in Mapped classes if they define any of these
    methods:
      * _pre_insert()
      * _post_insert()
      * _pre_delete()
      * _pre_update()
    """
    def before_insert(self, mapper, connection, instance):
        f = getattr(instance, "_pre_insert", None)
        if f:
            f()
        return EXT_CONTINUE

    def after_insert(self, mapper, connection, instance):
        f = getattr(instance, "_post_insert", None)
        if f:
            f()
        return EXT_CONTINUE

    def before_delete(self, mapper, connection, instance):
        f = getattr(instance, "_pre_delete", None)
        if f:
            f()
        return EXT_CONTINUE

    def before_update(self, mapper, connection, instance):
        f = getattr(instance, "_pre_update", None)
        if f:
            f()
        return EXT_CONTINUE


class JSONEncodedObj(TypeDecorator):
    """Represents an immutable structure as a json-encoded string."""
    impl = UnicodeText

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = unicode(simplejson.dumps(value, use_decimal=True))
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = simplejson.loads(value, use_decimal=True)
        return value

# list functions defined in module
def get_module_functions(module):
    return [func for func in module.__dict__.itervalues() if is_module_function(module, func)]

def is_module_function(module, func):
    return inspect.isfunction(func) and inspect.getmodule(func) == module
