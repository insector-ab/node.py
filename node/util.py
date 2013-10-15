# -*- coding: utf-8 -*-

class ConstantMeta(type):
    """Collects unicode constants set on class"""
    def __init__(cls, class_name, bases, class_dict):
        cls.values = []
        for k,v in class_dict.iteritems():
            if type(v) == unicode:
                cls.values.append(v)
            elif type(v) == int:
                cls.values.append(v)

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