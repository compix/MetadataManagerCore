import pymongo
from MetadataManagerCore import Keys
from bson import Code
from MetadataManagerCore.DocumentModification import DocOpResult, DocumentOperation
import numpy as np
import json
import logging
from typing import List, Tuple
from MetadataManagerCore.Event import Event

class CollectionHeaderKeyInfo(object):
    MD_KEY = "key"
    MD_DISPLAY_NAME = "displayName"
    MD_DISPLAYED = "displayed"

    def __init__(self, key, displayName, displayed):
        self.key = key
        self.displayName = displayName
        self.displayed = displayed

    def asDict(self):
        return {CollectionHeaderKeyInfo.MD_KEY:self.key, 
                CollectionHeaderKeyInfo.MD_DISPLAY_NAME: self.displayName, 
                CollectionHeaderKeyInfo.MD_DISPLAYED: self.displayed}

class MongoDBManager:
    def __init__(self, host, databaseName):
        self.logger = logging.getLogger(__name__)
        self.host = host
        self.databaseName = databaseName
        self.db = None
        self.onDocumentModifiedEvent = Event()

    def connect(self):
        self.client = pymongo.MongoClient(self.host)
        self.client.server_info()
        self.db = self.client[self.databaseName]

    # entry: dictionary
    def insertOne(self, collectionName, entry : dict):
        self.db[collectionName].insert_one(entry)

    def getCollectionNames(self):
        return self.db.list_collection_names() if self.db != None else []

    def getVisibleCollectionNames(self):
        for cn in self.getCollectionNames():
            if not cn in Keys.hiddenCollections and not cn.endswith(Keys.OLD_VERSIONS_COLLECTION_SUFFIX):
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

    def extractCollectionHeaderInfo(self, collectionNames) -> List[CollectionHeaderKeyInfo]:
        infos : List[CollectionHeaderKeyInfo] = []

        # Go through the selected collection metadata, check displayed table info
        # and add unique entries:
        for collectionName in collectionNames:
            cMD = self.collectionsMD.find_one({"_id":collectionName})
            if cMD:
                cTableHeader = cMD.get("tableHeader")
                if cTableHeader != None:
                    for keyInfo in cTableHeader:
                        key = keyInfo.get(CollectionHeaderKeyInfo.MD_KEY)
                        displayName = keyInfo.get(CollectionHeaderKeyInfo.MD_DISPLAY_NAME)
                        displayed = keyInfo.get(CollectionHeaderKeyInfo.MD_DISPLAYED)

                        infos.append(CollectionHeaderKeyInfo(key, displayName, displayed))
            else:
                # New collection without header info. Extract default info (everything visible) and add it to db
                newInfos = []
                keys = self.findAllKeysInCollection(collectionName)
                for key in keys:
                    newInfos.append(CollectionHeaderKeyInfo(key, key, True))

                self.setCollectionHeaderInfo(collectionName, newInfos)
                infos += newInfos

        return infos

    def setCollectionHeaderInfo(self, collectionName, tableHeader : List[CollectionHeaderKeyInfo]):
        tableHeader = [i.asDict() for i in tableHeader]
        self.collectionsMD.update_one({"_id": collectionName}, {'$set': {'tableHeader': tableHeader}}, upsert=True)

    def findAllKeysInCollection(self, collectionName):
        map = Code("function() { for (var key in this) { emit(key, null); } }")
        reduce = Code("function(key, stuff) { return null; }")
        tempResultCollection = f"{collectionName}_keys_temp"
        result = self.db[collectionName].map_reduce(map, reduce, tempResultCollection)

        keys = [v for v in result.distinct('_id')]
        self.db.drop_collection(tempResultCollection)
        return keys

    def insertOrModifyDocument(self, collectionName, sid, dataDict, checkForModifications) -> Tuple[dict, DocOpResult]:
        """
        If checkForModifications is true, the new document will be compared to the old document (if present). 
        If the documents are identical the DB entry for the given sid won't be changed.
        """
        op = DocumentOperation(self.db, collectionName, sid, dataDict)

        document, result = op.applyOperation(checkForModifications)
        if result == DocOpResult.Successful:
            self.onDocumentModifiedEvent(document)

        return document, result

    def getFilteredDocuments(self, collectionName, documentsFilter : dict, distinctionText=''):
        collection = self.db[collectionName]
        filteredCursor = collection.find(documentsFilter, no_cursor_timeout=True)
        distinctKey = distinctionText
        
        with filteredCursor:
            if len(distinctKey) > 0:
                distinctKeys = filteredCursor.distinct(distinctKey if distinctKey != None else '')
                distinctMap = dict(zip(distinctKeys, np.repeat(False, len(distinctKeys))))
                for item in filteredCursor:
                    val = item.get(distinctKey)
                    if val != None:
                        if not distinctMap.get(val):
                            distinctMap[val] = True
                            yield item
            else:
                for item in filteredCursor:
                    yield item

    def stringToFilter(self, filterString : str) -> dict:
        try:
            if len(filterString) == 0:
                return {}
            
            filter_ = json.loads(filterString)
            return filter_
        except Exception as e:
            self.logger.error(f"Failed to convert filter string {filterString} to a valid filter dictionary. Reason: {str(e)}")
            return {"_id":"None"}

    @property
    def stateCollection(self):
        return self.db[Keys.STATE_COLLECTION]

    @property
    def hostProcessesCollection(self):
        return self.db[Keys.HOST_PROCESSES_COLLECTION]

    @property
    def serviceProcessCollection(self):
        return self.db[Keys.SERVICE_PROCESS_COLLECTION]

    @property
    def serviceCollection(self):
        return self.db[Keys.SERVICE_COLLECTION]