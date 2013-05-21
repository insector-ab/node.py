# -*- coding: utf-8 -*-

# class ConstantMeta(type):
#     """Collects unicode constants set on class"""
#     def __init__(cls, class_name, bases, class_dict):
#         cls.values = []
#         for k,v in class_dict.iteritems():
#             if type(v) == unicode:
#                 cls.values.append(v)
#             elif type(v) == int:
#                 cls.values.append(v)

# class UnicodeConstantMeta(type):
#     """Collects unicode constants set on class"""
#     def __init__(cls, class_name, bases, class_dict):
#         cls.values = []
#         for k,v in class_dict.iteritems():
#             if type(v) == unicode:
#                 cls.values.append(v)