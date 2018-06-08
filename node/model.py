# -*- coding: utf-8 -*-
import datetime
import uuid
from dateutil.tz import tzutc

from sqlalchemy import Column, Integer, Unicode, ForeignKey, and_, or_, TypeDecorator, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import object_session
from sqlalchemy.ext.mutable import MutableDict

from node import Base
from node.util import get_discriminators, JSONEncodedObj, validate_uuid4


class UTCDateTime(TypeDecorator):
    # MySQL Datetime dialect, for microsecond precision
    # sqlalchemy.dialects.mysql.DATETIME
    # dateCreated = Column(DATETIME(fsp=6))
    impl = DateTime

    def process_bind_param(self, value, engine):
        if value is not None:
            value = value.astimezone(tzutc())
            return value.replace(tzinfo=None)

    def process_result_value(self, value, engine):
        if value is not None:
            return datetime.datetime(value.year, value.month, value.day,
                                     value.hour, value.minute, value.second,
                                     value.microsecond, tzinfo=tzutc())

tz = tzutc()
def datetime_utc_now():
    return datetime.datetime.now(tz)


class Edge(Base):
    CHILD = u'child'
    PARENT = u'parent'

    __tablename__ = 'edges'
    id = Column(Integer, primary_key=True)
    _edge_key = Column('edge_key', Unicode(100), unique=True)
    _name = Column('name', Unicode(191))
    _group_name = Column('group_name', Unicode(100))
    _relation_type = Column('relation_type', Unicode(100))
    _index = Column('index', Integer)
    _meta_data = Column('meta_data', MutableDict.as_mutable(JSONEncodedObj))

    # Dates
    _created_at = Column('created_at', UTCDateTime())
    _modified_at = Column('modified_at', UTCDateTime())

    left_uuid = Column(Unicode(36), ForeignKey('nodes.uuid'))
    parent = relationship('Node',
                          backref='children_relations',
                          primaryjoin='Edge.left_uuid==Node.uuid')

    right_uuid = Column(Unicode(36), ForeignKey('nodes.uuid'))
    child = relationship('Node',
                         backref='parent_relations',
                         primaryjoin='Edge.right_uuid==Node.uuid',
                         lazy='joined')

    def __init__(self, *args, **kw):
        super(Edge, self).__init__(*args, **kw)
        now = datetime_utc_now()
        self._created_at = now
        self._modified_at = now

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
        if parent and parent == child:
            raise Exception('Cirular reference')
        # recursive find parent as child or grandchild
        if parent and child:
            for grandchild in child._get_children():
                Edge.check_circular_reference(parent, grandchild)

    @staticmethod
    def create_edge(parent, child, group=None, relation_type=None, index=None, metadata=None):
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
        edge._index = index
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
        """
        Update node's related parent or child edges. related_nodes=[list of related nodes],
        discriminators=[list of discriminators], group="name of edge group", relation_type="type of relation",
        metadata=[list of dicts]
        """
        assert isinstance(related_nodes, list)
        # only allow unique nodes
        uuids = [related_node.uuid for related_node in related_nodes]
        assert len(uuids) == len(set(uuids))
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
        updated_nodes = []
        for edge in existing_edges:
            related_node = edge.child if relation == Edge.CHILD else edge.parent
            if related_node in related_nodes:
                i = related_nodes.index(related_node)  # index in list
                if metadata:
                    if i < len(metadata):
                        edge.meta_data = metadata[i]  # set metadata on edge
                edge._index = i
                updated_nodes.append(related_node)
            else:
                s.delete(edge)

        # iterate non existing nodes and create new edges
        for i, related_node in enumerate(related_nodes):
            # already updated
            if related_node in updated_nodes:
                continue
            md = metadata[i] if metadata and i < len(metadata) else None
            if relation == Edge.CHILD:
                new_edge = Edge.create_edge(node, related_node, group=group, relation_type=relation_type, index=i, metadata=md)
            else:
                new_edge = Edge.create_edge(related_node, node, group=group, relation_type=relation_type, index=i, metadata=md)
            s.add(new_edge)

    @staticmethod
    def remove_all_edges(node):
        node.session.query(Edge).filter(or_(Edge.left_uuid == node.uuid, Edge.right_uuid == node.uuid)).delete()


class AbstractNode(Base):
    __abstract__ = True
    uuid = Column(Unicode(36), primary_key=True)
    discriminator = Column(Unicode(50))
    _node_key = Column('node_key', Unicode(100), unique=True)

    # Dates
    _created_at = Column('created_at', UTCDateTime())
    _modified_at = Column('modified_at', UTCDateTime())

    def __init__(self, *args, **kw):
        super(AbstractNode, self).__init__(*args, **kw)
        if 'uuid' in kw:
            uuid_string = kw.get('uuid')
            if not validate_uuid4(uuid_string):
                raise Exception('Invalid uuid')
        else:
            uuid_string = uuid.uuid4()
        self.uuid = unicode(uuid_string)
        now = datetime_utc_now()
        self._created_at = now
        self._modified_at = now

    def __unicode__(self):
        return '.uuid {0}'.format(self.uuid)

    # def __getattribute__(self, attr):
    #     if not (attr == '__dict__' or attr == '__class__'):
    #         print "Getting ", attr , "on", self, attr in self.__class__.__dict__
    #     return super(AbstractNode, self).__getattribute__(attr)

    def __getattr__(self, attr):
        attr = '_{0}'.format(attr)
        if attr in self.__class__.__dict__:
            return object.__getattribute__(self, attr)
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
        return self._get_related_node_query(None, Edge.CHILD, discriminators, group, relation_type, order_by).all()

    def _set_children(self, children=[], discriminators=None, group=None, relation_type=None, metadata=[]):
        Edge.update_child_edges(self, children, discriminators=discriminators, group=group, relation_type=relation_type, metadata=metadata)

    def _get_parents(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(None, Edge.PARENT, discriminators, group, relation_type, order_by).all()

    def _set_parents(self, parents=[], discriminators=None, group=None, relation_type=None, metadata=[]):
        Edge.update_parent_edges(self, parents, discriminators=discriminators, group=group, relation_type=relation_type, metadata=metadata)

    def _get_child(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(None, Edge.CHILD, discriminators, group, relation_type, order_by).first()

    def _get_parent(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(None, Edge.PARENT, discriminators, group, relation_type, order_by).first()

    def _get_child_edges(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(Edge, Edge.CHILD, discriminators, group, relation_type, order_by).all()

    def _get_parent_edges(self, discriminators=None, group=None, relation_type=None, order_by=None):
        return self._get_related_node_query(Edge, Edge.PARENT, discriminators, group, relation_type, order_by).all()

    def _get_child_edge(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(Edge, Edge.CHILD, discriminators, group, relation_type, order_by).first()

    def _get_parent_edge(self, discriminators=None, group=None, relation_type=False, order_by=None):
        return self._get_related_node_query(Edge, Edge.PARENT, discriminators, group, relation_type, order_by).first()

    def _get_related_node_query(self, query_cls, relation=Edge.CHILD, discriminators=None, group=None, relation_type=None, order_by=None):
        node_cls = AbstractNode.get_node_cls()
        if query_cls == Edge:
            query = self.session.query(Edge).select_from(node_cls)
        else:
            query = self.session.query(node_cls)

        clauses = self._get_related_node_query_clauses(relation, discriminators, group, relation_type)
        node_edge = (Edge, node_cls.uuid == Edge.right_uuid) if relation == Edge.CHILD else (Edge, node_cls.uuid == Edge.left_uuid)

        query = query.join(node_edge).filter(and_(*clauses))
        if order_by:
            if isinstance(order_by, list):
                query = query.order_by(*order_by)
            else:
                query = query.order_by(order_by)
        return query

    def _get_related_node_query_clauses(self, relation=Edge.CHILD, discriminators=None, group=None, relation_type=None):
        clauses = []
        if relation == Edge.CHILD:
            clauses.append(Edge.left_uuid == self.uuid)
        else:
            clauses.append(Edge.right_uuid == self.uuid)

        if discriminators:
            clauses.append(self.__class__.discriminator.in_(discriminators))

        if group is not False:
            clauses.append(Edge._group_name == group)

        if relation_type is not False:
            clauses.append(Edge._relation_type == relation_type)

        return clauses

    @classmethod
    def get_node_cls(cls):
        subclasses = cls.__subclasses__()
        if len(subclasses) == 0:
            raise Exception('No Node class defined, extension of AbstractNode required.')
        elif len(subclasses) > 1:
            raise Exception('More than one subclass found for AbstractClass')
        return subclasses[0]

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

class RelatedNodes(object):

    CHILDREN = u'children'
    PARENTS = u'parents'

    def __init__(self, direction, *args, **kws):
        super(RelatedNodes, self).__init__()
        # u'children'/'parents', Node, Article, group=None, relation_type=None
        self.direction = direction
        self.classes = args
        self.include_subclasses = kws.get('include_subclasses', True)
        # group_name
        self.group = kws.get('group', None)
        self.relation_type = kws.get('relation_type', None)
        self.order_by = kws.get('order_by', None)

    def __get__(self, instance, cls):
        return getattr(instance, '_get_{0}'.format(self.direction))(discriminators=self.discriminators, group=self.group, relation_type=self.relation_type, order_by=self.order_by)

    def __set__(self, instance, value):
        discriminators = self.discriminators
        if discriminators:
            for node in value:
                if node.discriminator not in discriminators:
                    raise ValueError('One of the {0} passed as argument is of wrong type'.format(self.direction))

        getattr(instance, '_set_{0}'.format(self.direction))(value, discriminators=discriminators, group=self.group, relation_type=self.relation_type)

    @property
    def discriminators(self):
        return get_discriminators(*self.classes, include_subclasses=self.include_subclasses)

class Children(RelatedNodes):

    def __init__(self, *args, **kws):
        super(Children, self).__init__(RelatedNodes.CHILDREN, *args, **kws)

class Parents(RelatedNodes):

    def __init__(self, *args, **kws):
        super(Parents, self).__init__(RelatedNodes.PARENTS, *args, **kws)

# AbstractNode.children = RelatedNodes('children', AbstractNode)
# AbstractNode.parents = RelatedNodes('parents', AbstractNode)
# AbstractNode.children = Children(AbstractNode)
# AbstractNode.parents = Parents(AbstractNode)
