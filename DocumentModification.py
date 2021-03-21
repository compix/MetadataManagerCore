from typing import Tuple
from MetadataManagerCore import Keys
import pymongo
from enum import Enum

import logging

logger = logging.getLogger(__name__)

class DocOpResult(Enum):
    Successful = 1
    MergeConflict = 2

class DocumentOperation:
    """
    Records and applies insert and versioned modification operations for a document.
    If a document with the given sid exists a new version is created with the specified data defined in a dictionary.
    If it doesn't exist a new document is inserted. Both operations must be applied with applyOperation().
    """
    def __init__(self, database, collectionName, sid, dataDict):
        self.sid = sid
        self.collection = database[collectionName]
        self.versionCollection = database[collectionName + Keys.OLD_VERSIONS_COLLECTION_SUFFIX]
        self.dataDict = dataDict
        self.version = 0
        currentDocument = self.getNewestDocument()
        if currentDocument != None:
            self.version = currentDocument[Keys.systemVersionKey]

    def getId(self, version):
        return self.sid + "_" + str(version)

    def applyOperation(self, checkForModifications) -> Tuple[dict, DocOpResult]:
        """
        Applies the document operation. If the operation was successful DocOpResult.Successful is returned otherwise
        there was a merge conflict and thus DocOpResult.MergeConflict is returned.

        If checkForModifications is true, the new document will be compared to the old document (if present). 
        If the documents are identical the DB entry for the given sid won't be changed.
        """
        # Get the newest version and check if it matches the expected version
        currentDocument = self.getNewestDocument()
        if currentDocument != None:
            if currentDocument[Keys.systemVersionKey] == self.version:
                newDict = currentDocument

                # Do nothing if checkForModifications is true and the documents are identical.
                if checkForModifications:
                    allDataEntriesEqual = True
                    for key, val in self.dataDict.items():
                        if not key in newDict or newDict[key] != val:
                            allDataEntriesEqual = False
                            break

                    if allDataEntriesEqual:
                        return newDict, DocOpResult.Successful
                    
                # Move old version to versioning collection:
                self.versionCollection.replace_one({"_id":currentDocument.get('_id')}, currentDocument, upsert=True)
                self.collection.delete_one({'_id':(self.getId(self.version))})

                # Apply modifications:
                for key, val in self.dataDict.items():
                    newDict[key] = val

                newVersion = self.version + 1
                newDict[Keys.systemVersionKey] = newVersion
            else:
                logger.warning("The document with sid " + self.sid + " was modified by a different user.")
                return None, DocOpResult.MergeConflict
        else:
            # Adding a new document, define the values:
            newDict = self.dataDict
            newVersion = 0
            newDict[Keys.systemIDKey] = self.sid
            newDict[Keys.systemVersionKey] = newVersion

        # Note: A race-condition is possible. Two dicts with the same _id might be inserted which raises an error.
        try:
            newDict['_id'] = self.getId(newVersion)
            self.collection.insert_one(newDict)
            return newDict, DocOpResult.Successful
        except pymongo.errors.PyMongoError as e:
            logger.error(str(e))
            return None, DocOpResult.MergeConflict
                
    def getNewestDocument(self):
        return self.collection.find_one({Keys.systemIDKey:self.sid})