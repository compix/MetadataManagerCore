import pymongo
from MetadataManagerCore import Keys
from bson import Code
from MetadataManagerCore.DocumentModification import DocumentOperation
import numpy as np

class MongoDBManager:
    def __init__(self, host, databaseName):
        self.host = host
        self.databaseName = databaseName
        self.db = None

    def connect(self):
        try:
            self.client = pymongo.MongoClient(self.host)
            self.db = self.client[self.databaseName]
        except Exception as e:
            print(f"Failed to connect: {str(e)}")

    # entry: dictionary
    def insertOne(self, collectionName, entry : dict):
        self.db[collectionName].insert_one(entry)

    def getCollectionNames(self):
        return self.db.list_collection_names() if self.db != None else []

    def getVisibleCollectionNames(self):
        for cn in self.getCollectionNames():
            if not cn in Keys.hiddenCollections:
                yield cn

    def findOne(self, uid):
        for collectionName in self.getVisibleCollectionNames():
            collection = self.db[collectionName]
            val = collection.find_one({"_id":uid})
            if val != None:
                return val

    @property
    def collectionsMD(self):
        return self.db[Keys.collectionsMD]

    def extractTableHeaderAndDisplayedKeys(self, collectionNames):
        header = []
        keys = []
        # Go through the selected collection metadata, check displayed table info
        # and add unique entries:
        for collectionName in collectionNames:
            cMD = self.collectionsMD.find_one({"_id":collectionName})
            if cMD:
                cTableHeader = cMD.get("tableHeader")
                if cTableHeader != None:
                    assert(len(e) == 2 for e in cTableHeader)
                    header = header + [e[Keys.COLLECTION_MD_DISPLAY_NAME_IDX] for e in cTableHeader if e[Keys.COLLECTION_MD_KEY_IDX] not in keys]
                    keys = keys + [e[Keys.COLLECTION_MD_KEY_IDX] for e in cTableHeader if e[Keys.COLLECTION_MD_KEY_IDX] not in keys]

        return header, keys

    def setTableHeader(self, collectionName, tableHeader):
        self.collectionsMD.update_one({"_id": collectionName}, {'$set': {'tableHeader': tableHeader}}, upsert=True)

    def findAllKeysInCollection(self, collectionName):
        map = Code("function() { for (var key in this) { emit(key, null); } }")
        reduce = Code("function(key, stuff) { return null; }")
        tempResultCollection = f"{collectionName}_keys_temp"
        result = self.db[collectionName].map_reduce(map, reduce, tempResultCollection)

        keys = [v for v in result.distinct('_id')]
        self.db.drop_collection(tempResultCollection)
        return keys

    def insertOrModifyDocument(self, collectionName, sid, dataDict, checkForModifications):
        """
        If checkForModifications is true, the new document will be compared to the old document (if present). 
        If the documents are identical the DB entry for the given sid won't be changed.
        """
        op = DocumentOperation(self.db, collectionName, sid, dataDict)

        op.applyOperation(checkForModifications)

    def getFilteredDocuments(self, collectionName, filterText, distinctionText=''):
        collection = self.db[collectionName]
        filtered = collection.find(filterText)
        distinctKey = distinctionText
        
        if len(distinctKey) > 0:
            distinctKeys = filtered.distinct(distinctKey)
            distinctMap = dict(zip(distinctKeys, np.repeat(False, len(distinctKeys))))
            for item in filtered:
                val = item.get(distinctKey)
                if val != None:
                    if not distinctMap.get(val):
                        distinctMap[val] = True
                        yield item
        else:
            for item in filtered:
                yield item