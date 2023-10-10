# pylint: skip-file
from pprint import pprint 
from DbConnector import DbConnector
import pymongo


class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_many_docs(self, collection_name, documents):
        collection = self.db[collection_name]
        collection.insert_many(documents)
    
    def insert_doc(self, collection_name, document):
        collection = self.db[collection_name]
        collection.insert_one(document)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        return documents
        
    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
    
    def field_query_unique(self, collection_name, field):
        collection = self.db[collection_name]
        return collection.distinct(field)

    def query(self, collection_name, query):
        collection = self.db[collection_name]
        result = collection.find(query).sort('_id', pymongo.DESCENDING)
        return result

    def delete_many_docs(self, collection_name, query):
        collection = self.db[collection_name]
        collection.delete_many(query)
    
    def delete_doc(self, collection_name, query):
        collection = self.db[collection_name]
        collection.delete_one(query)