# -*- coding: utf-8 -*-
import datetime

from sqlalchemy import Column, Integer, Unicode, ForeignKey, DateTime, and_, UnicodeText, PickleType, or_
from sqlalchemy.orm import relationship, EXT_CONTINUE
from sqlalchemy.orm.interfaces import MapperExtension
from sqlalchemy.orm.session import object_session

from node import Base

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


class Edge(Base):
    CHILD = u"child"
    PARENT = u"parent"

    __tablename__ = "edges"
    id = Column(Integer, primary_key=True)
    edge_key = Column(Unicode(100), unique=True)
    name = Column(Unicode(255))
    group_name = Column(Unicode(100))
    relation_type = Column(Unicode(100))
    index = Column(Integer)
    meta_data = Column(PickleType()) # Possible we need to add Mutable Dict

    left_id = Column(Integer, ForeignKey('nodes.id'))
    parent = relationship(  "Node",
                            backref="children_relations",
                            primaryjoin="Edge.left_id==Node.id")

    right_id = Column(Integer, ForeignKey('nodes.id'))
    child = relationship(   "Node",
                            backref="parent_relations",
                            primaryjoin="Edge.right_id==Node.id",
                            lazy='joined')

    def __init__(self, *args, **kw):
        super(Edge, self).__init__(*args, **kw)

    @property
    def session(self):
        return object_session(self)

    def _set_metadata_value(self, key, value):
        if not self.meta_data:
            self.meta_data = {}
        # Remove keys with value None
        if value == None:
            try:
                del(self.meta_data[key])
            except Exception:
                pass
        else:
            self.meta_data[key] = value

        # remove dict
        if len(self.meta_data.keys()) == 0:
            self.meta_data = None

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
            for grandchild in child.children:
                Edge.check_circular_reference(parent,grandchild)

    @staticmethod
    def create_edge(parent, child, group=None, relation_type=None, metadata=None):
        if isinstance(parent, int):
            parent = Node.get(parent)
        if isinstance(child, int):
            child = Node.get(child)
        Edge.check_circular_reference(parent,child) # raises error if fail
        edge = Edge(metadata=metadata)
        edge.parent = parent
        edge.child = child
        edge.group_name = group
        edge.relation_type = relation_type
        return edge

    @staticmethod
    def update_child_edges(parent, children, **kws):
        """Update parent's child edges. child_ids=[list of children], group="name of edge group", metadata=[list of dicts] """
        Edge.update_related_edges( parent, children, Edge.CHILD, **kws )

    @staticmethod
    def update_parent_edges(child, parents, **kws):
        """Update child's parent edges. parents=[list of parents], group="name of edge group", discriminator="class discriminator", metadata=[list of dicts] """
        Edge.update_related_edges( child, parents, Edge.PARENT, **kws )

    @staticmethod
    def update_related_edges(node, related_nodes, relation=CHILD, group=None, relation_type=None, discriminator=None, metadata=None):
        """Update node's related parent or child edges. related_nodes=[list of related nodes], group="name of edge group", relation_type="type of relation", discriminator="class discriminator", metadata=[list of dicts] """
        assert isinstance(related_nodes, list)
        clauses = (Edge.parent==node, ) if relation == Edge.CHILD else (Edge.child==node, )
        if group:
            clauses = clauses + (Edge.group_name==group, )
        if relation_type:
            clauses = clauses + (Edge.relation_type==relation_type, )
        if discriminator:
            clauses = clauses + (Node.discriminator==discriminator, )

        # iterate existing edges
        s = node.session
        existing_edges = s.query(Edge).join(Edge.parent if relation == Edge.CHILD else Edge.child).filter(and_(*clauses)).all()
        for edge in existing_edges:
            related_node = edge.child if relation==Edge.CHILD else edge.parent
            if related_node in related_nodes:
                i = related_nodes.index(related_node) # index in list
                if metadata:
                    if i < len(metadata):
                        edge.meta_data = metadata[i] # set metadata on edge
                    metadata = metadata[:i] + metadata[i+1:] # remove index
                related_nodes = related_nodes[:i] + related_nodes[i+1:] # remove same index
            else:
                s.delete(edge)
        # iterate non existing nodes and create new edges
        for i, related_node in enumerate(related_nodes):
            md = metadata[i] if metadata and i < len(metadata) else None
            if relation == Edge.CHILD:
                new_edge = Edge.create_edge( node, related_node, group, md )
            else:
                new_edge = Edge.create_edge( related_node, node, group, md )
            s.add( new_edge )

    @staticmethod
    def update_child_edges_by_id(parent, child_ids, **kws):
        """Update parent's child edges. child_ids=[list of child ids], group="name of edge group", metadata=[list of dicts] """
        Edge.update_related_edges_by_id( parent, child_ids, Edge.CHILD, **kws )

    @staticmethod
    def update_parent_edges_by_id(child, parent_ids, **kws):
        """Update child's parent edges. parent_ids=[list of parent ids], group="name of edge group", discriminator="class discriminator", metadata=[list of dicts] """
        Edge.update_related_edges_by_id( child, parent_ids, Edge.PARENT, **kws )

    @staticmethod
    def update_related_edges_by_id(node, related_ids, relation=CHILD, group=None, relation_type=None, discriminator=None, metadata=None):
        """Update node's related parent or child edges. related_ids=[list of related node ids], group="name of edge group", relation_type="type of relation", discriminator="class discriminator", metadata=[list of dicts] """
        assert isinstance(related_ids, list)
        if len(related_ids):
            related_ids = map(int, related_ids)
        clauses = (Edge.parent==node, ) if relation == Edge.CHILD else (Edge.child==node, )
        if group:
            clauses = clauses + (Edge.group_name==group, )
        if relation_type:
            clauses = clauses + (Edge.relation_type==relation_type, )
        if discriminator:
            clauses = clauses + (Node.discriminator==discriminator, )

        # iterate existing edges
        s = node.session
        existing_edges = s.query(Edge).join(Edge.parent if relation == Edge.CHILD else Edge.child).filter(and_(*clauses)).all()
        for edge in existing_edges:
            related_node_id = edge.right_id if relation==Edge.CHILD else edge.left_id
            if related_node_id in related_ids:
                i = related_ids.index(related_node_id) # index in list
                if metadata:
                    if i < len(metadata):
                        edge.meta_data = metadata[i] # set metadata on edge
                    metadata = metadata[:i] + metadata[i+1:] # remove index
                related_ids = related_ids[:i] + related_ids[i+1:] # remove same index
            else:
                s.delete(edge)
        # iterate non existing nodes and create new edges
        for i, related_node_id in enumerate(related_ids):
            md = metadata[i] if metadata and i < len(metadata) else None
            create_edge_args = ((node, related_node_id) if relation == Edge.CHILD else (related_node_id, node)) + (group, relation_type, md)
            new_edge = Edge.create_edge( *create_edge_args )
            s.add( new_edge )

    @staticmethod
    def remove_all_edges(node):
        node.session.query(Edge).filter(or_(Edge.left_id==node.id, Edge.right_id==node.id)).delete()



class Node(Base):
    __tablename__ = "nodes"
    id = Column(Integer, primary_key=True)
    discriminator = Column(Unicode(50))
    __mapper_args__ = {'polymorphic_on': discriminator, 'extension':Callback(), 'polymorphic_identity': u'node'}
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

    def __unicode__(self):
        return self.name or ""

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

    def _get_discriminators(self, cls):
        if isinstance(cls, list):
            return [cl.__mapper_args__['polymorphic_identity'] for cl in cls]
        return [cls.__mapper_args__['polymorphic_identity']]


    def _get_children(self, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        return self._get_related_node_query( Edge.CHILD, cls, group, relation_type, exclude_subclasses, order_by ).all()
    def _set_children(self, children=[], group=None, relation_type=None, metadata=[], discriminator=None):
        Edge.update_child_edges( self, children, group=group, relation_type=relation_type, metadata=metadata, discriminator=discriminator )

    def _get_child_ids(self, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        return self._get_related_node_id_query( Edge.CHILD, cls, group, relation_type, relation_type, exclude_subclasses, order_by).all()
    def _set_child_ids(self, child_ids=[], group=None, relation_type=None, metadata=[], discriminator=None):
        Edge.update_child_edge_by_id( self, child_ids, group=group, relation_type=relation_type, metadata=metadata, discriminator=discriminator )


    def _get_parents(self, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        return self._get_related_node_query( Edge.PARENT, cls, group, relation_type, exclude_subclasses, order_by ).all()
    def _set_parents(self, parents=[], group=None, relation_type=None, metadata=[], discriminator=None):
        Edge.update_parent_edges( self, parents, group=group, relation_type=relation_type, metadata=metadata, discriminator=discriminator )

    def _get_parent_ids(self, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        return self._get_related_node_id_query( Edge.PARENT, cls, group, exclude_subclasses, order_by ).all
    def _set_parent_ids(self, parent_ids=[], group=None, relation_type=None, metadata=[], discriminator=None):
        Edge.update_parent_edge_by_id( self, parent_ids, group=group, relation_type=relation_type, metadata=metadata, discriminator=discriminator )


    def _get_child(self, cls=None, group=None, relation_type=False, exclude_subclasses=False, order_by=None):
        return self._get_related_node_query(Edge.CHILD, cls, group, relation_type, exclude_subclasses, order_by).first()

    def _get_parent(self, cls=None, group=None, relation_type=False, exclude_subclasses=False, order_by=None):
        return self._get_related_node_query(Edge.PARENT, cls, group, relation_type, exclude_subclasses, order_by).first()


    def _get_related_node_query(self, relation=Edge.CHILD, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        if cls == None:
            cls = Node
        elif isinstance(cls,list):
            exclude_subclasses = True
        clauses = self._get_related_node_query_clauses(relation, group, relation_type, cls if exclude_subclasses else None)
        query = self.session.query(Node if isinstance(cls,list) else cls)
        node_edge = (Edge, Node.id==Edge.right_id) if relation==Edge.CHILD else (Edge, Node.id==Edge.left_id)
        query = query.join(node_edge).filter(and_(*clauses))
        if order_by:
            if isinstance(order_by, list):
                query = query.order_by(*order_by)
            else:
                query = query.order_by(order_by)
        return query

    def _get_related_node_id_query(self, relation=Edge.CHILD, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        if cls == None:
            cls = Node
        elif isinstance(cls,list):
            exclude_subclasses = True
        clauses = self._get_related_node_query_clauses(relation, group, relation_type, cls if exclude_subclasses else None)
        query = self.session.query(Node.id if isinstance(cls,list) else cls.id)
        node_edge = (Edge, Node.id==Edge.right_id) if relation==Edge.CHILD else (Edge, Node.id==Edge.left_id)
        query = query.join(node_edge).filter(and_(*clauses))
        if order_by:
            if isinstance(order_by, list):
                query = query.order_by(*order_by)
            else:
                query = query.order_by(order_by)
        return query

    def _get_related_node_query_clauses(self, relation=Edge.CHILD, group=None, relation_type=None, exclusive_cls=None):
        """docstring for _get_related_node_query_clauses"""
        clauses = []
        if relation==Edge.CHILD:
            clauses.append(Edge.parent==self)
        else:
            clauses.append(Edge.child==self)
        if exclusive_cls:
            clauses.append(Node.discriminator.in_(self._get_discriminators(exclusive_cls)))

        if not group == False:
            if group:
                clauses.append(Edge.group_name==group)
            else:
                clauses.append(Edge.group_name==None)

        if not relation_type == False:
            if relation_type:
                clauses.append(Edge.relation_type==relation_type)
            else:
                clauses.append(Edge.relation_type==None)

        return clauses


    def _get_child_edges(self, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        return self._get_related_edge_query(Edge.CHILD, cls, group, relation_type, exclude_subclasses, order_by).all()

    def _get_parent_edges(self, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        return self._get_related_edge_query(Edge.PARENT, cls, group, relation_type, exclude_subclasses, order_by).all()

    def _get_child_edge(self, cls=None, group=None, relation_type=False, exclude_subclasses=False, order_by=None):
        return self._get_related_edge_query(Edge.CHILD, cls, group, relation_type, exclude_subclasses, order_by).first()

    def _get_parent_edge(self, cls=None, group=None, relation_type=False, exclude_subclasses=False, order_by=None):
        return self._get_related_edge_query(Edge.PARENT, cls, group, relation_type, exclude_subclasses, order_by).first()

    def _get_related_edge_query(self, relation=Edge.CHILD, cls=None, group=None, relation_type=None, exclude_subclasses=False, order_by=None):
        if cls == None:
            cls = Node
        elif isinstance(cls,list):
            exclude_subclasses = True
        clauses = self._get_related_node_query_clauses(relation, group, relation_type, cls if exclude_subclasses else None)
        # print clauses
        query = self.session.query(Edge)
        cls = Node if isinstance(cls,list) else cls
        node_edge = (cls, cls.id==Edge.right_id) if relation==Edge.CHILD else (cls, cls.id==Edge.left_id)
        query = query.join(node_edge).filter(and_(*clauses))
        if order_by:
            if isinstance(order_by, list):
                query = query.order_by(*order_by)
            else:
                query = query.order_by(order_by)
        return query

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

    def __init__(self, cls=None, group_name=None, relation_type=None):
        super(Children, self).__init__()
        self.cls = cls
        self.group_name = group_name
        self.relation_type = relation_type

    def __get__(self, instance, cls):
        return instance._get_children( self.cls or Node, self.group_name, self.relation_type )

    def __set__(self, instance, value):
        if self.cls:
            for child in value:
                if not isinstance(child, self.cls):
                    raise ValueError("One of the children passed as argument is of wrong type")
        instance._set_children( value, self.group_name, self.relation_type )


class ChildIDs(object):

    def __init__(self, cls=None, group_name=None, relation_type=None):
        super(ChildIDs, self).__init__()
        self.cls = cls
        self.group_name = group_name
        self.relation_type = relation_type

    def __get__(self, instance, cls):
        return instance._get_child_ids( self.cls or Node, self.group_name, self.relation_type )

    def __set__(self, instance, value):
        ids = map(int, value)
        instance._set_child_ids( ids, self.group_name, self.relation_type )



class Parents(object):

    def __init__(self, cls=None, group_name=None, relation_type=None):
        super(Parents, self).__init__()
        self.cls = cls
        self.group_name = group_name
        self.relation_type = relation_type

    def __get__(self, instance, cls):
        return instance._get_parents( self.cls or Node, self.group_name, self.relation_type )

    def __set__(self, instance, value):
        if self.cls:
            for parent in value:
                if not isinstance(parent, self.cls):
                    raise ValueError("One of the parents passed as argument is of wrong type")
        instance._set_parents( value, self.group_name, self.relation_type )


class ParentIDs(object):

    def __init__(self, cls=None, group_name=None, relation_type=None):
        super(ParentIDs, self).__init__()
        self.cls = cls
        self.group_name = group_name
        self.relation_type = relation_type

    def __get__(self, instance, cls):
        return instance._get_parent_ids( self.cls or Node, self.group_name, self.relation_type )

    def __set__(self, instance, value):
        ids = map(int, value)
        instance._set_parent_ids( ids, self.group_name, self.relation_type )



Node.children = Children(Node)
Node.child_ids = ChildIDs(Node)

Node.parents = Parents(Node)
Node.parent_ids = ParentIDs(Node)

