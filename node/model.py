# -*- coding: utf-8 -*-
import datetime

from sqlalchemy import Column, Integer, Unicode, ForeignKey, DateTime, and_, or_
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import object_session
from sqlalchemy.ext.mutable import MutableDict

from node import Base
from node.util import get_discriminators, JSONEncodedObj

class Edge(Base):
    CHILD = u"child"
    PARENT = u"parent"

    __tablename__ = "edges"
    id = Column(Integer, primary_key=True)
    _edge_key = Column('edge_key', Unicode(100), unique=True)
    _name = Column('name', Unicode(255))
    _group_name = Column('group_name', Unicode(100))
    _relation_type = Column('relation_type', Unicode(100))
    _index = Column('index', Integer)
    _meta_data = Column('meta_data', MutableDict.as_mutable(JSONEncodedObj))

    left_id = Column(Integer, ForeignKey('nodes.id'))
    parent = relationship("Node",
                          backref="children_relations",
                          primaryjoin="Edge.left_id==Node.id")

    right_id = Column(Integer, ForeignKey('nodes.id'))
    child = relationship("Node",
                         backref="parent_relations",
                         primaryjoin="Edge.right_id==Node.id",
                         lazy='joined')

    def __init__(self, *args, **kw):
        super(Edge, self).__init__(*args, **kw)

    # def __getattribute__(self, attr):
    #     # _attr = '_{0}'.format(attr)
    #     # if not hasattr(self, attr) and hasattr(self, _attr):
    #     #     return super(AbstractNode, self).__getattribute__(_attr)

    #     return super(AbstractNode, self).__getattribute__(attr)

    def __getattr__(self, attr):
        attr = '_{0}'.format(attr)
        if attr in self.__class__.__dict__:
            return getattr(self, attr)
        else:
            raise AttributeError

    def __setattr__(self, attr, value):
        if attr in self.__class__.__dict__ or attr == '_sa_instance_state':
            object.__setattr__(self, attr, value)
        else:
            object.__setattr__(self, '_{0}'.format(attr), value)

    @property
    def session(self):
        return object_session(self)

    def _set_metadata_value(self, key, value):
        if not self._meta_data:
            self._meta_data = {}
        # Remove keys with value None
        if value is None:
            try:
                del(self._meta_data[key])
            except Exception:
                pass
        else:
            self._meta_data[key] = value

        # remove dict
        if len(self._meta_data.keys()) == 0:
            self._meta_data = None

    # ==================
    # = STATIC METHODS =
    # ==================

    @staticmethod
    def check_circular_reference(parent, child):
        """docstring for check_circular_reference"""
        if parent and parent == child:
            raise Exception('Cirular reference')
        # recursive find parent as child or grandchild
        if parent and child:
            for grandchild in child._get_children():
                Edge.check_circular_reference(parent, grandchild)

    @staticmethod
    def create_edge(parent, child, group=None, relation_type=None, metadata=None):
        if isinstance(parent, int):
            parent = AbstractNode.get(parent)
        if isinstance(child, int):
            child = AbstractNode.get(child)
        Edge.check_circular_reference(parent, child)  # raises error if fail
        edge = Edge(metadata=metadata)
        edge.parent = parent
        edge.child = child
        edge._group_name = group
        edge._relation_type = relation_type
        return edge

    @staticmethod
    def update_child_edges(parent, children, **kws):
        """Update parent's child edges. child_ids=[list of children], discriminators=[list of discriminators], group="name of edge group", metadata=[list of dicts] """
        Edge.update_related_edges(parent, children, Edge.CHILD, **kws)

    @staticmethod
    def update_parent_edges(child, parents, **kws):
        """Update child's parent edges. parents=[list of parents], discriminators=[list of discriminators], group="name of edge group", metadata=[list of dicts] """
        Edge.update_related_edges(child, parents, Edge.PARENT, **kws)

    @staticmethod
    def update_related_edges(node, related_nodes, relation=CHILD, discriminators=None, group=None, relation_type=None, metadata=None):
        """Update node's related parent or child edges. related_nodes=[list of related nodes],
        discriminators=[list of discriminators], group="name of edge group", relation_type="type of relation",
        metadata=[list of dicts] """
        assert isinstance(related_nodes, list)
        clauses = (Edge.parent == node, ) if relation == Edge.CHILD else (Edge.child == node, )
        if group is not False:
            clauses = clauses + (Edge._group_name == group, )
        if relation_type is not False:
            clauses = clauses + (Edge._relation_type == relation_type, )
        if discriminators:
            # temp fix ?
            subclasses = AbstractNode.__subclasses__()
            if len(subclasses) > 1:
                raise Exception('More than one subclass found for AbstractClass')
            nodeCls = subclasses[0]
            clauses = clauses + (nodeCls.discriminator.in_(discriminators), )

        # iterate existing edges
        s = node.session
        existing_edges = s.query(Edge).join(Edge.child if relation == Edge.CHILD else Edge.parent).filter(and_(*clauses)).all()
        for edge in existing_edges:
            related_node = edge.child if relation == Edge.CHILD else edge.parent
            if related_node in related_nodes:
                i = related_nodes.index(related_node)  # index in list
                if metadata:
                    if i < len(metadata):
                        edge.meta_data = metadata[i]  # set metadata on edge
                    metadata = metadata[:i] + metadata[i + 1:]  # remove index
                related_nodes = related_nodes[:i] + related_nodes[i + 1:]  # remove same index
            else:
                s.delete(edge)

        # iterate non existing nodes and create new edges
        for i, related_node in enumerate(related_nodes):
            md = metadata[i] if metadata and i < len(metadata) else None
            if relation == Edge.CHILD:
                new_edge = Edge.create_edge(node, related_node, group=group, metadata=md)
            else:
                new_edge = Edge.create_edge(related_node, node, group=group, metadata=md)
            s.add(new_edge)

    @staticmethod
    def remove_all_edges(node):
        node.session.query(Edge).filter(or_(Edge.left_id == node.id, Edge.right_id == node.id)).delete()


class AbstractNode(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    discriminator = Column(Unicode(50))
    _node_key = Column('node_key', Unicode(100), unique=True)

    # Dates
    _created_at = Column('created_at', DateTime(), default=datetime.datetime.now)
    _modified_at = Column('modified_at', DateTime(), default=datetime.datetime.now)

    def __init__(self, *args, **kw):
        super(AbstractNode, self).__init__(*args, **kw)

    def __unicode__(self):
        return ".id {0}".format(self.id)

    # def __getattribute__(self, attr):
    #     _attr = '_{0}'.format(attr)
    #     if not hasattr(self, attr) and hasattr(self, _attr):
    #     #     return super(AbstractNode, self).__getattribute__(_attr)

    #     return super(AbstractNode, self).__getattribute__(attr)

    def __getattr__(self, attr):
        attr = '_{0}'.format(attr)
        if attr in self.__class__.__dict__:
            return getattr(self, attr)
        else:
            raise AttributeError

    def __setattr__(self, attr, value):
        if attr in self.__class__.__dict__ or attr == '_sa_instance_state':
            object.__setattr__(self, attr, value)
        else:
            object.__setattr__(self, '_{0}'.format(attr), value)

    @property
    def classname(self):
        return self.__class__.__name__

    @property
    def singular(self):
        return self.__class__.get_singular()

    @property
    def plural(self):
        return self.__class__.get_plural()

    @property
    def session(self):
        return object_session(self)

    def _get_children(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(self.__class__, Edge.CHILD, discriminators, group, relation_type, order_by).all()

    def _set_children(self, children=[], discriminators=None, group=None, relation_type=None, metadata=[]):
        Edge.update_child_edges(self, children, discriminators=discriminators, group=group, relation_type=relation_type, metadata=metadata)

    def _get_parents(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(self.__class__, Edge.PARENT, discriminators, group, relation_type, order_by).all()

    def _set_parents(self, parents=[], discriminators=None, group=None, relation_type=None, metadata=[]):
        Edge.update_parent_edges(self, parents, discriminators=discriminators, group=group, relation_type=relation_type, metadata=metadata)

    def _get_child(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(self.__class__, Edge.CHILD, discriminators, group, relation_type, order_by).first()

    def _get_parent(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(self.__class__, Edge.PARENT, discriminators, group, relation_type, order_by).first()

    def _get_child_edges(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(Edge, Edge.CHILD, discriminators, group, relation_type, order_by).all()

    def _get_parent_edges(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(Edge, Edge.PARENT, discriminators, group, relation_type, order_by).all()

    def _get_child_edge(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(Edge, Edge.CHILD, discriminators, group, relation_type, order_by).first()

    def _get_parent_edge(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(Edge, Edge.PARENT, discriminators, group, relation_type, order_by).first()

    def _get_related_node_query(self, query_cls, relation=Edge.CHILD, discriminators=None, group=None, relation_type=None, order_by=None):

        query = self.session.query(query_cls)
        if query_cls == Edge:
            query = query.select_from(self.__class__)

        clauses = self._get_related_node_query_clauses(relation, discriminators, group, relation_type)
        node_edge = (Edge, self.__class__.id == Edge.right_id) if relation == Edge.CHILD else (Edge, self.__class__.id == Edge.left_id)
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
        if relation == Edge.CHILD:
            clauses.append(Edge.parent == self)
        else:
            clauses.append(Edge.child == self)

        if discriminators:
            clauses.append(self.__class__.discriminator.in_(discriminators))

        if group is not False:
            clauses.append(Edge._group_name == group)

        if relation_type is not False:
            clauses.append(Edge._relation_type == relation_type)

        return clauses

    @classmethod
    def get_polymorphic_identity(cls):
        return cls.__mapper_args__['polymorphic_identity']

    @classmethod
    def get_plural(cls):
        return cls.__tablename__

    @classmethod
    def get_singular(cls):
        return cls.get_polymorphic_identity()

# ===============
# = Descriptors =
# ===============

class DictProperty(object):

    def __init__(self, key, dict_name=None, default_value=None):
        super(DictProperty, self).__init__()
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

class Children(object):

    def __init__(self, *args, **kws):
        super(Children, self).__init__()
        # Node, Article, group_name=None, relation_type=None
        self.include_subclasses = kws.get('include_subclasses', True)
        self.classes = args
        self.group_name = kws.get('group_name', None)
        self.relation_type = kws.get('relation_type', None)

    def __get__(self, instance, cls):
        return instance._get_children(discriminators=self.discriminators, group=self.group_name, relation_type=self.relation_type)

    def __set__(self, instance, value):
        discriminators = self.discriminators
        if discriminators:
            for child in value:
                if child.discriminator not in discriminators:
                    raise ValueError("One of the children passed as argument is of wrong type")
        instance._set_children(value, discriminators=discriminators, group=self.group_name, relation_type=self.relation_type)

    @property
    def discriminators(self):
        return get_discriminators(*self.classes, include_subclasses=self.include_subclasses)

class Parents(object):

    def __init__(self, *args, **kws):
        super(Parents, self).__init__()
        # Node, Article, group_name=None, relation_type=None
        self.include_subclasses = kws.get('include_subclasses', True)
        self.classes = args
        self.group_name = kws.get('group_name', None)
        self.relation_type = kws.get('relation_type', None)

    def __get__(self, instance, cls):
        return instance._get_parents(discriminators=self.discriminators, group=self.group_name, relation_type=self.relation_type)

    def __set__(self, instance, value):
        discriminators = self.discriminators
        if discriminators:
            for parent in value:
                if parent.discriminator not in discriminators:
                    raise ValueError("One of the parents passed as argument is of wrong type")
        instance._set_parents(value, discriminators=discriminators, group=self.group_name, relation_type=self.relation_type)

    @property
    def discriminators(self):
        return get_discriminators(*self.classes, include_subclasses=self.include_subclasses)


# AbstractNode.children = Children(AbstractNode)
# AbstractNode.parents = Parents(AbstractNode)
