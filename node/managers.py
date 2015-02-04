# -*- coding: utf-8 -*-


class EdgeManager(object):

    @staticmethod
    def check_circular_reference(parent, child):
        """docstring for check_circular_reference"""
        if parent and parent == child:
            raise ReferenceError('Cirular reference found.')
        # recursive find parent as child or grandchild
        if parent and child:
            for grandchild in child.children:
                EdgeManager.check_circular_reference( parent, grandchild )

    # @staticmethod
    # def create(EdgeCls, parent, child, group=None, relation_type=None, **kws):
    #     # raises ReferenceError if fail
    #     EdgeManager.check_circular_reference(parent,child)
    #     # Create
    #     edge = Edge()
    #     edge._parent = parent
    #     edge._child = child
    #     if group:
    #         edge._group = group
    #     if relation_type:
    #         edge._relation_type = relation_type
    #     if kws:
    #         for key in kws:
    #             edge._data[key] = kws.get(key)
    #     return edge



class NodeManager(object):

    @staticmethod
    def get_subclasses(cls):
        subclasses = cls.__subclasses__()
        for d in list(subclasses):
            subclasses.extend(NodeManager.get_subclasses(d))
        return subclasses

    @staticmethod
    def get_discriminator_map(cls):
        obj = {}
        for c in NodeManager.get_subclasses(cls):
            obj[c.get_polymorphic_identity()] = c
        return obj

    @staticmethod
    def get_discriminators(*node_classes, **kws):
        include_subclasses = kws.get('include_subclasses', True)
        items = []
        for cls in node_classes:
            items.append(cls.get_polymorphic_identity())
            if include_subclasses:
                for subcls in NodeManager.get_subclasses(cls):
                    items.append(subcls.get_polymorphic_identity())
        return items