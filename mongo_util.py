from pymongo import MongoClient


class MongoConnection():

    def __init__(self):
        self.db_client= MongoClient('mongodb+srv://m001-student:m001-mongodb-basics@cluster0.u0wyx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
        self.db = self.db_client['iot_sensors']

    def post(self, collection_name, post_content):
        collection = self.db[collection_name]
        return collection.insert_one(post_content).inserted_id

    def get(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.find(query)

    def get_aggregated(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.aggregate(query)