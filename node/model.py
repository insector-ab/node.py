# -*- coding: utf-8 -*-
import datetime
import uuid

from sqlalchemy import Column, Integer, Unicode, ForeignKey, DateTime, and_, UnicodeText, join, desc, PickleType
from sqlalchemy.orm import relationship, backref, EXT_CONTINUE, deferred
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.interfaces import MapperExtension

from webtools.model.meta import Session, Base

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
        if f: f()
        return EXT_CONTINUE
        
    def after_insert(self, mapper, connection, instance):
        f = getattr(instance, "_post_insert", None)
        if f: f()
        return EXT_CONTINUE

    def before_delete(self, mapper, connection, instance):
        f = getattr(instance, "_pre_delete", None)
        if f: f()
        return EXT_CONTINUE

    def before_update(self, mapper, connection, instance):
        f = getattr(instance, "_pre_update", None)
        if f: f()
        return EXT_CONTINUE


class AlchemyQuery(object):

    @classmethod
    def get(cls, id):
        try:
            result = Session.query(cls).get(id) 
        except NoResultFound:
            result = None
            
        return result

    @classmethod
    def query(cls, *args, **kw):
        query = Session.query(cls)

        if len(args) > 0:
            query = query.filter(*args)
        
        if kw.has_key('order_by'):
            query = query.order_by(kw.get('order_by'))

        return query

    @classmethod
    def first(cls, *args, **kw):
        query = cls.query(*args, **kw)
        return query.first()

    @classmethod
    def one(cls, *args, **kw):
        query = cls.query(*args, **kw)
        return query.one()

    @classmethod
    def all(cls, *args, **kw):
        query = cls.query(*args, **kw)
        return query.all()


class Edge(Base, AlchemyQuery):
    CHILD = u"child"
    PARENT = u"parent"
        
    __tablename__ = "edges"
    id = Column(Integer, primary_key=True)
    uuid = Column(Unicode(255), unique=True)
    name = Column(Unicode(255))
    group_key = Column(Unicode(100))
    edge_key = Column(Unicode(100), unique=True)
    relation_type = Column(Unicode(100))
    meta_data = Column(PickleType(mutable=True))

    left_id = Column(Integer, ForeignKey('nodes.id'))
    parent = relationship(  "Node",
                            backref="children_relations",
                            primaryjoin="Edge.left_id==Node.id")

    right_id = Column(Integer, ForeignKey('nodes.id'))
    child = relationship(   "Node",
                            backref="parent_relations",
                            primaryjoin="Edge.right_id==Node.id",
                            lazy='joined')
                            
    reference_id = Column(Integer, ForeignKey('edges.id')) 
    reference_edge = relationship(  "Edge",
                                    primaryjoin="Edge.reference_id==Edge.id",
                                    uselist=False,
                                    remote_side=[id])

 
    def __init__(self, *args, **kw):
        super(Edge, self).__init__(*args, **kw)
        self.uuid = unicode(uuid.uuid1().get_hex())


    def _set_metadata_value(self, key, value):
        if not self.meta_data:
            self.meta_data = {}
        # Remove keys with value None 
        if value == None:
            try:
                del(self.meta_data[key])                
            except Exception, e:
                pass
        else:
            self.meta_data[key] = value

        # remove dict
        if len(self.meta_data.keys()) == 0:
            self.meta_data = None


class Node(Base, AlchemyQuery):
    __tablename__ = "nodes"
    id = Column(Integer, primary_key=True)
    uuid = Column(Unicode(255), unique=True)
    discriminator = Column(Unicode(50))
    __mapper_args__ = {'polymorphic_on': discriminator, 'extension':Callback()}
    name = Column(Unicode(255))
    description = Column(UnicodeText)
    node_key = Column(Unicode(100), unique=True)
    
    # Dates
    created_at = Column(DateTime(), default=datetime.datetime.now)
    modified_at = Column(DateTime(), default=datetime.datetime.now)

    # Users 
    created_by_id = Column(Integer)
    modified_by_id = Column(Integer)

    def __init__(self, *args, **kw):
        super(Node, self).__init__(*args, **kw)
        self.uuid = unicode(kw.get('uuid', uuid.uuid1().get_hex()))      

    def __unicode__(self):
        return self.name or ""

    def _get_discriminators(self, cls):
        if isinstance(cls, list):
            return [cl.__mapper_args__['polymorphic_identity'] for cl in cls]
        return [cls.__mapper_args__['polymorphic_identity']]

    def _get_children(self, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        return self._get_related_nodes( Edge.CHILD, cls, group, relation_type, exclude_subclasses, order_by )

    def _get_child_ids(self, cls=None, group=None, relation_type=None, exclude_subclasses=False):
        return self._get_related_node_ids( Edge.CHILD, cls, group, relation_type, exclude_subclasses )

    def _get_parents(self, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        return self._get_related_nodes( Edge.PARENT, cls, group, relation_type, exclude_subclasses, order_by )

    def _get_parent_ids(self, cls=None, group=None, relation_type=None, exclude_subclasses=False):
        return self._get_related_node_ids( Edge.PARENT, cls, group, relation_type, exclude_subclasses )

    def _get_related_nodes(self, relation=Edge.CHILD, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        if cls == None:
            cls = Node
        elif isinstance(cls,list):
            exclude_subclasses = True
        clauses = self._get_related_nodes_query_clauses(relation, group, relation_type, cls if exclude_subclasses else None)
        query = Session.query(Node if isinstance(cls,list) else cls)
        node_edge = (Edge, Node.id==Edge.right_id) if relation==Edge.CHILD else (Edge, Node.id==Edge.left_id)
        query = query.join(node_edge).filter(and_(*clauses))
        if order_by:
            query = query.order_by(order_by)
        return query.all()
    
    def _get_related_node_ids(self, relation=Edge.CHILD, cls=None, group=None, relation_type=None, exclude_subclasses=False):
        if cls == None:
            cls = Node
        elif isinstance(cls,list):
            exclude_subclasses = True
        clauses = self._get_related_nodes_query_clauses(relation, group, relation_type, cls if exclude_subclasses else None)
        query = Session.query(Node.id if isinstance(cls,list) else cls.id)
        node_edge = (Edge, Node.id==Edge.right_id) if relation==Edge.CHILD else (Edge, Node.id==Edge.left_id)
        return query.join(node_edge).filter(and_(*clauses)).all()

    def _get_related_nodes_query_clauses(self, relation=Edge.CHILD, group=None, exclusive_cls=None):
        """docstring for _get_related_nodes_query_clauses"""
        clauses = []
        if relation==Edge.CHILD:
            clauses.append(Edge.parent==self)
        else:
            clauses.append(Edge.child==self)
        if exclusive_cls:
            clauses.append(Node.discriminator.in_(self._get_discriminators(exclusive_cls)))
        
        if not group == False:
            if group:
                clauses.append(Edge.group_key==group)
            else:
                clauses.append(Edge.group_key==None)

        if not relation_type == False:
            if relation_type:
                clauses.append(Edge.relation_type==relation_type)
            else:
                clauses.append(Edge.relation_type==None)        

        return clauses

    def _get_child(self, cls = None):
        nodes = self._get_children(cls)
        if len(nodes) == 1:
            return nodes[0]
        return None

    def _get_parent(self, cls = None):
        nodes = self._get_parents(cls)
        if len(nodes) == 1:
            return nodes[0]
        return None

    def _get_child_edges(self, cls=None, group=None, exclude_subclasses=False):
        return self._get_related_edges(Edge.CHILD, cls, group, exclude_subclasses)

    def _get_parent_edges(self, cls=None, group=None, exclude_subclasses=False):
        return self._get_related_edges(Edge.PARENT, cls, group, exclude_subclasses)

    def _get_related_edges(self, relation=Edge.CHILD, cls=None, group=None, exclude_subclasses=False):
        # print "_get_related_nodes", relation, cls, group, exclude_subclasses
        if cls == None:
            cls = Node
        elif isinstance(cls,list):
            exclude_subclasses = True
        clauses = self._get_related_nodes_query_clauses(relation, group, cls if exclude_subclasses else None)
        # print clauses
        query = Session.query(Edge)
        cls = Node if isinstance(cls,list) else cls
        node_edge = (cls, cls.id==Edge.right_id) if relation==Edge.CHILD else (cls, cls.id==Edge.left_id)
        query = query.join(node_edge).filter(and_(*clauses))
        return query.all()

        
    @classmethod
    def get_polymorphic_identity(cls):
        return cls.__mapper_args__['polymorphic_identity']