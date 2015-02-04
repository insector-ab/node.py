# -*- coding: utf-8 -*-
import bcrypt
import sys
import datetime

from sqlalchemy import Column, Integer, Unicode, String, ForeignKey, DateTime #, and_, or_
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import object_session
from sqlalchemy.ext.mutable import MutableDict
# from sqlalchemy.ext.mutable import Mutable

from node import Base
from node.descriptors import Children, Parents
from node.util import JSONEncodedObj # Callback




class BaseHelpers(object):

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

    @classmethod
    def get_polymorphic_identity(cls):
        return cls.__mapper_args__['polymorphic_identity']

    @classmethod
    def get_plural(cls):
        return cls.__tablename__

    @classmethod
    def get_singular(cls):
        return cls.get_polymorphic_identity()




class Edge(Base, BaseHelpers):
    __tablename__ = "edges"

    _id = Column('id', Integer, primary_key=True)
    _discriminator = Column('discriminator', Unicode(50))

    __mapper_args__ = {
        'polymorphic_on': _discriminator,
        'polymorphic_identity': u'edge'
    }

    _key = Column('key', Unicode(100), unique=True)
    _name = Column('name', Unicode(255))
    _group = Column('group', Unicode(100))
    _relation_type = Column('relation_type', Unicode(100))
    _index = Column('index', Integer)
    _data = Column('data',  MutableDict.as_mutable(JSONEncodedObj), default={} )

    _parent_id = Column(Integer, ForeignKey('nodes.id'))
    _parent = relationship( "Node",
                            backref="children_relations",
                            primaryjoin="Edge._parent_id==Node._id")

    _child_id = Column(Integer, ForeignKey('nodes.id'))
    _child = relationship(  "Node",
                            backref="parent_relations",
                            primaryjoin="Edge._child_id==Node._id",
                            lazy='joined')

    @property
    def session(self):
        return object_session(self)

    # ==================
    # = STATIC METHODS =
    # ==================


    # @staticmethod
    # def update_child_edges(parent, children, **kws):
    #     """Update parent's child edges. child_ids=[list of children], discriminators=[list of discriminators], group="name of edge group", metadata=[list of dicts] """
    #     Edge.update_related_edges( parent, children, Edge.CHILD, **kws )

    # @staticmethod
    # def update_parent_edges(child, parents, **kws):
    #     """Update child's parent edges. parents=[list of parents], discriminators=[list of discriminators], group="name of edge group", metadata=[list of dicts] """
    #     Edge.update_related_edges( child, parents, Edge.PARENT, **kws )

    # @staticmethod
    # def update_related_edges(node, related_nodes, relation=CHILD, discriminators=None, group=None, relation_type=None, metadata=None):
    #     """Update node's related parent or child edges. related_nodes=[list of related nodes], discriminators=[list of discriminators], group="name of edge group", relation_type="type of relation", metadata=[list of dicts] """
    #     assert isinstance(related_nodes, list)
    #     clauses = (Edge.parent==node, ) if relation == Edge.CHILD else (Edge.child==node, )

    #     if not group == False:
    #         clauses = clauses + (Edge._group==group, )
    #     if not relation == False:
    #         clauses = clauses + (Edge._relation_type==relation_type, )
    #     if discriminators:
    #         clauses = clauses + (Node.discriminator.in_(discriminators), )

    #     # iterate existing edges
    #     s = node.session
    #     existing_edges = s.query(Edge).join(Edge.child if relation == Edge.CHILD else Edge.parent).filter(and_(*clauses)).all()
    #     for edge in existing_edges:
    #         related_node = edge.child if relation==Edge.CHILD else edge.parent
    #         if related_node in related_nodes:
    #             i = related_nodes.index(related_node) # index in list
    #             if metadata:
    #                 if i < len(metadata):
    #                     edge.meta_data = metadata[i] # set metadata on edge
    #                 metadata = metadata[:i] + metadata[i+1:] # remove index
    #             related_nodes = related_nodes[:i] + related_nodes[i+1:] # remove same index
    #         else:
    #             s.delete(edge)
    #     # iterate non existing nodes and create new edges
    #     for i, related_node in enumerate(related_nodes):
    #         md = metadata[i] if metadata and i < len(metadata) else None
    #         if relation == Edge.CHILD:
    #             new_edge = Edge.create_edge( node, related_node, group=group, metadata=md )
    #         else:
    #             new_edge = Edge.create_edge( related_node, node, group=group, metadata=md )
    #         s.add( new_edge )

    # @staticmethod
    # def remove_all_edges(node):
    #     node.session.query(Edge).filter(or_(Edge._parent_id==node.id, Edge._child_id==node.id)).delete()



class Node(Base, BaseHelpers):
    __tablename__ = "nodes"
    # Class to use with relationships
    __edge_cls__ = Edge

    id = Column(Integer, primary_key=True)
    discriminator = Column(Unicode(50))

    __mapper_args__ = {
        'polymorphic_on': discriminator,
        'polymorphic_identity': u'node'
    }
    # name = Column(Unicode(255))
    # description = Column(UnicodeText)
    node_key = Column(Unicode(100), unique=True)
    # Dates
    created_at = Column(DateTime(), default=datetime.datetime.now)
    modified_at = Column(DateTime(), default=datetime.datetime.now)
    # Users
    created_by_id = Column(Integer, ForeignKey('users.id'))
    created_by = relationship(  "User",
                                primaryjoin="Node.created_by_id==User.id")

    modified_by_id = Column(Integer, ForeignKey('users.id'))
    modified_by = relationship( "User",
                                primaryjoin="Node.modified_by_id==User.id")


    def _get_children(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(Node, Edge.CHILD, discriminators, group, relation_type, order_by).all()
    def _set_children(self, children=[], discriminators=None, group=None, relation_type=None, metadata=[]):
        Edge.update_child_edges(self, children, discriminators=discriminators, group=group, relation_type=relation_type, metadata=metadata)

    def _get_parents(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(Node, Edge.PARENT, discriminators, group, relation_type, order_by).all()
    def _set_parents(self, parents=[], discriminators=None, group=None, relation_type=None, metadata=[]):
        Edge.update_parent_edges( self, parents, discriminators=discriminators, group=group, relation_type=relation_type, metadata=metadata)

    # def _get_child(self, discriminators=None, group=None, relation_type=False, order_by=None):
    #     return self._get_related_node_query(Node, Edge.CHILD, discriminators, group, relation_type, order_by).first()

    # def _get_parent(self, discriminators=None, group=None, relation_type=False, order_by=None):
    #     return self._get_related_node_query(Node, Edge.PARENT, discriminators, group, relation_type, order_by).first()

    def _get_child_edges(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(Edge, Edge.CHILD, discriminators, group, relation_type, order_by).all()

    def _get_parent_edges(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(Edge, Edge.PARENT, discriminators, group, relation_type, order_by).all()

    def _get_child_edge(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(Edge, Edge.CHILD, discriminators, group, relation_type, order_by).first()

    def _get_parent_edge(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(Edge, Edge.PARENT, discriminators, group, relation_type, order_by).first()

    # def _get_related_node_query(self, query_cls, relation=Edge.CHILD, discriminators=None, group=None, relation_type=None, order_by=None):
    #     query = self.session.query(query_cls)
    #     if query_cls == Edge:
    #         query = query.select_from(Node)

    #     clauses = self._get_related_node_query_clauses(relation, discriminators, group, relation_type)
    #     node_edge = (Edge, Node._id==Edge._child_id) if relation==Edge.CHILD else (Edge, Node._id==Edge._parent_id)
    #     query = query.join(node_edge).filter(and_(*clauses))
    #     if order_by:
    #         if isinstance(order_by, list):
    #             query = query.order_by(*order_by)
    #         else:
    #             query = query.order_by(order_by)
    #     return query

    # def _get_related_node_query_clauses(self, relation=Edge.CHILD, discriminators=None, group=None, relation_type=None):
    #     """docstring for _get_related_node_query_clauses"""
    #     clauses = []
    #     if relation==Edge.CHILD:
    #         clauses.append(Edge.parent==self)
    #     else:
    #         clauses.append(Edge.child==self)

    #     if discriminators:
    #         clauses.append(Node.discriminator.in_(discriminators))

    #     if not group == False:
    #         clauses.append(Edge._group==group)

    #     if not relation_type == False:
    #         clauses.append(Edge._relation_type==relation_type)

    #     return clauses


Node._children = Children(Node)
Node._parents = Parents(Node)




InputMismatchError = TypeError("Inputs must be both unicode or both bytes")


class User(Base, BaseHelpers):
    __tablename__ = "users"
    __mapper_args__ = {'polymorphic_identity': u'user'}
    id = Column(Integer, primary_key=True)

    _username = Column(Unicode(100), unique=True)
    _last_login = Column(DateTime())
    _digest = Column(String(100))
    
    def __init__(self, username=None, password=None):
        if username:
            self._username = username
        if password:
            self.set_password( password )

    @property
    def discriminator(self):
        return u'user'

    @property
    def username(self):
        return self._username
    @username.setter
    def username(self, value):
        self._username = value

    @property
    def last_login(self):
        return self._last_login
    @last_login.setter
    def last_login(self, value):
        self._last_login = value


    def set_password(self, password):
        self._digest = self._generate_digest( password )

    def check_password(self, password):
        # unicode?
        if isinstance(password, unicode):
            password = password.encode('utf-8')
        # No digest set, return false
        if not self._digest:
            return self._constant_time_compare('returns', 'false')
        # check stored digest
        return self._constant_time_compare( bcrypt.hashpw(password, self._digest), self._digest)

    def _generate_digest(self, password):
        # unicode?
        if isinstance(password, unicode):
            password = password.encode('utf-8')
        # generate        
        return bcrypt.hashpw(password, bcrypt.gensalt())

    # Helper method
    # Always takes the same time comparing a & b
    # regardless of similarity. Prevents learning
    # when brute forcing password.
    def _constant_time_compare(self, a, b):
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
