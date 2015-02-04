# -*- coding: utf-8 -*-
import json

from sqlalchemy import Text
from sqlalchemy.orm import EXT_CONTINUE
from sqlalchemy.orm.interfaces import MapperExtension
from sqlalchemy.types import TypeDecorator



class ConstantMeta(type):
    """Collects unicode constants set on class"""
    def __init__(self, class_name, bases, class_dict):
        self.values = []
        self.keys = {}
        for k,v in class_dict.iteritems():
            if type(v) == unicode or type(v) == int:
                self.values.append(v)
                self.keys[v] = k

# class UnicodeConstantMeta(type):
#     """Collects unicode constants set on class"""
#     def __init__(cls, class_name, bases, class_dict):
#         cls.values = []
#         for k,v in class_dict.iteritems():
#             if type(v) == unicode:
#                 cls.values.append(v)


class Callback(MapperExtension):
    """ Extention to add pre-commit hooks.

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
    impl = Text

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value, use_decimal=True)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value, use_decimal=True)
        return value
