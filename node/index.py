# -*- coding: utf-8 -*-
import os
import inspect
import whoosh

import whoosh.index as index
import whoosh.fields as fields

from whoosh.index import EmptyIndexError
from whoosh.query import And, Or, Term, Prefix, FuzzyTerm, Not
from whoosh.filedb.filestore import FileStorage
from whoosh.writing import AsyncWriter
from whoosh.qparser import MultifieldParser, QueryParser #, WildcardPlugin, PrefixPlugin
from whoosh.analysis import SimpleAnalyzer


def to_unicode(di):
    for k,v in di.iteritems():
        if type(v) is str:
            di[k] = v.decode('ascii')
    return di

def parse_index_string(string):
    return string.lower().replace('.', ' ')
    

def get_index(name, schema, path, clean=False):
    # create dir
    if not os.path.exists(path):
        os.makedirs(path)
    
    storage = FileStorage(path)

    # Create an index object            
    try:
        if clean:
            raise EmptyIndexError()
        return storage.open_index(indexname=name)
    except EmptyIndexError:
        return storage.create_index(schema, indexname=name)  
    
analyzer = SimpleAnalyzer()
search_default_schema = fields.Schema(
        id = fields.ID(stored=True, unique=True),
        uuid = fields.ID(stored=True, unique=True),
        discriminator = fields.TEXT(stored=True),
        title = fields.TEXT(stored=True, analyzer=analyzer),
        search_content = fields.TEXT(stored=True, analyzer=analyzer),
        url = fields.ID(stored=True)
    )
    
def get_default_path():
    return os.path.join('data', 'whoosh')

 
class Index(object):
    
    @staticmethod
    def get_index(indexname=None, schema=None, path=None, clean=False):
        if not indexname:
            indexname=u'default'
            
        if not schema:
            schema = search_default_schema

        if not path:
            path = get_default_path()
            
        return get_index(indexname, schema, path, clean=clean)
        
    @staticmethod
    def get_writer(indexname=None, schema=None):
        return AsyncWriter(Index.get_index(indexname=indexname, schema=schema))

    @staticmethod
    def add(dict, indexname=None, schema=None):
        writer = Index.get_writer(indexname=indexname, schema=schema)
        writer.add_document(**to_unicode(dict))
        writer.commit()
        
    @staticmethod    
    def upsert(dict, indexname=None, schema=None):
        writer = Index.get_writer(indexname=indexname, schema=schema)
        writer.update_document(**to_unicode(dict))
        writer.commit()

    @staticmethod    
    def delete(id, indexname=None, schema=None):
        writer = Index.get_writer(indexname=indexname, schema=schema)
        writer.delete_by_term('id', unicode(id))
        writer.commit()
    
    @staticmethod
    def clean(indexname=None, schema=None):
        Index.get_index(indexname=indexname, clean=True, schema=schema)
                
    @staticmethod
    def build_query(index, query, indexname=None, fields=None):
        schema = index.schema
        if not fields:
            # query all fields (filter out any STORED types. These are not searchable)
            fields = [key for key,field in schema._fields.iteritems() \
                if type(field) in [whoosh.fields.TEXT, whoosh.fields.ID]]
            
        qp = MultifieldParser(fields, schema=schema)
        #qp.remove_plugin_class(WildcardPlugin)
        #qp.add_plugin(WildcardPlugin())
        return qp.parse(query)
            
    @staticmethod
    def search(query, indexname=None, fields=None, limit=10, discriminators=None, exclude_discriminators=None, schema=None):
        index = Index.get_index(indexname=indexname, schema=schema)
        
        if exclude_discriminators and discriminators:
            q = And([Index.build_query(index, query, indexname=indexname, fields=fields), Not(Or([Term("discriminator", discriminator) for discriminator in discriminators]))])            
        elif discriminators:
            q = And([Index.build_query(index, query, indexname=indexname, fields=fields), Or([Term("discriminator", discriminator) for discriminator in discriminators])])
        else:
            q = Index.build_query(index, query, indexname=indexname, fields=fields)
            
        return index.searcher().search(q, limit=limit)
            
    @staticmethod
    def search_page(query, indexname=None, page=1, pagelen=20, fields=None, schema=None):
        index = Index.get_index(indexname=indexname, schema=schema)
        q = Index.build_query(index, query, indexname=indexname, fields=fields)
        return index.searcher().search_page(q, page, pagelen=pagelen)