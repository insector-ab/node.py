# -*- coding: utf-8 -*-

from node.managers import NodeManager




class Relation(object):
    CHILD = u"child"
    PARENT = u"parent"        


class RelatedNodes(object):

    def __init__(self, relation, *node_classes, **kws):
        self.relation = relation
        self.discriminators = NodeManager.get_discriminators(*node_classes, include_subclasses=kws.get('include_subclasses', True))
        self.group = kws.get('group')
        self.relation_type = kws.get('relation_type')
        self.is_list = kws.get('is_list', False)

    def _get_related_node_query(self, query_cls, relation=Edge.CHILD, discriminators=None, group=None, relation_type=None, order_by=None):
        query = self.session.query(query_cls)
        if query_cls == Edge:
            query = query.select_from(Node)

        clauses = self._get_related_node_query_clauses(relation, discriminators, group, relation_type)
        node_edge = (Edge, Node._id==Edge._child_id) if relation==Edge.CHILD else (Edge, Node._id==Edge._parent_id)
        query = query.join(node_edge).filter(and_(*clauses))
        if order_by:
            if isinstance(order_by, list):
                query = query.order_by(*order_by)
            else:
                query = query.order_by(order_by)
        return query

    def _get_related_node_query_clauses(self, relation=Edge.CHILD, discriminators=None, group=None, relation_type=None):
        """docstring for _get_related_node_query_clauses"""
        clauses = []
        if relation==Edge.CHILD:
            clauses.append(Edge.parent==self)
        else:
            clauses.append(Edge.child==self)

        if discriminators:
            clauses.append(Node.discriminator.in_(discriminators))

        if not group == False:
            clauses.append(Edge._group==group)

        if not relation_type == False:
            clauses.append(Edge._relation_type==relation_type)

        return clauses



# attr = Children(Node, Article, group=None, relation_type=None, include_subclasses=True)
class Children(RelatedNodes):

    def __init__(self, *node_classes, **kws):
        super(Children, self).__init__(Relation.CHILD, *node_classes, **kws)

    def __get__(self, instance, cls):
        
        query = self._get_related_nodes_query()

        return instance._get_children( discriminators=self.discriminators, group=self.group, relation_type=self.relation_type )

    def __set__(self, instance, value):
        if self.discriminators:
            for child in value:
                if not child.discriminator in self.discriminators:
                    raise ValueError("One of the children passed as argument is of wrong type")
        instance._set_children( value, discriminators=self.discriminators, group=self.group, relation_type=self.relation_type )


# attr = Parents(Node, Article, group=None, relation_type=None, include_subclasses=True)
class Parents(RelatedNodes):

    def __init__(self, *node_classes, **kws):
        super(Children, self).__init__(Relation.PARENT, *node_classes, **kws)

    def __get__(self, instance, cls):
        return instance._get_parents( discriminators=self.discriminators, group=self.group, relation_type=self.relation_type )

    def __set__(self, instance, value):
        if self.discriminators:
            for parent in value:
                if not parent.discriminator in self.discriminators:
                    raise ValueError("One of the parents passed as argument is of wrong type")
        instance._set_parents( value, discriminators=self.discriminators, group=self.group, relation_type=self.relation_type )



class DictProperty(object):

    def __init__(self, key, dict_name=None, default_value=None):
        self.dict_name = dict_name
        self.key = key
        self.default_value = default_value

    def __get__(self, instance, cls):
        item = self._get_dict(instance)
        return item.get(self.key, self.default_value)

    def __set__(self, instance, value):
        item = self._get_dict(instance)
        item[self.key] = value

    def __delete__(self, instance):
        item = self._get_dict(instance)
        del item[self.key]

    def _get_dict(self, instance):
        if not hasattr(instance, self.dict_name):
            raise Exception(u'Dict "{0}" not found on {1}'.format(self.dict_name, instance))

        item = getattr(instance, self.dict_name)

        if not isinstance(item, dict):
            raise Exception(u'Attribute "{0}" not a dict'.format(self.dict_name))

        return item