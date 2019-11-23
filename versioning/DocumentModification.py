from MetadataManagerCore import Keys
import pymongo
from enum import Enum

class DocOpResult(Enum):
    Successful = 0
    MergeConflict = 1

class DocumentOperation:
    """
    Records and applies insert and versioned modification operations for a document.
    If a document with the given sid exists a new version is created with the specified data defined in a dictionary.
    If it doesn't exist a new document is inserted. Both operations must be applied with applyOperation().
    """
    def __init__(self, collection, sid, dataDict):
        self.sid = sid
        self.collection = collection
        self.dataDict = dataDict
        self.version = 0
        newestDoc = self.getNewestDocument()
        if newestDoc != None:
            self.version = newestDoc[Keys.systemVersionKey]

    def applyOperation(self):
        """
        Applies the document operation. If the operation was successful DocOpResult.Successful is returned otherwise
        there was a merge conflict and thus DocOpResult.MergeConflict is returned.
        """
        # Get the newest version and check if it matches the expected version
        newestDoc = self.getNewestDocument()
        if newestDoc != None:
            if newestDoc[Keys.systemVersionKey] == self.version:
                # Apply modifications:
                newDict = newestDoc.to_mongo()
                for key, val in self.dataDict:
                    newDict[key] = val

                newVersion = self.version + 1
                newDict[Keys.systemVersionKey] = newVersion
            else:
                print("The document with sid " + self.sid + " was modified by a different user.")
                return DocOpResult.MergeConflict
                # TODO: Handle modification conflict, show a side by side comparison of the remote data and local changes.
                #       Note: The user has to actively resolve the conflict.
        else:
            # Adding a new document, define the values:
            newDict = self.dataDict
            newVersion = 0
            newDict[Keys.systemIDKey] = self.sid
            newDict[Keys.systemVersionKey] = newVersion

        # Note: A race-condition is possible. Two dicts with the same _id might be inserted which raises an error.
        try:
            self.collection.insert_one({"_id": self.sid + newVersion}, newDict)
            return DocOpResult.Successful
        except pymongo.errors.PyMongoError as e:
            print(str(e))
            return DocOpResult.MergeConflict
                
    def getNewestDocument(self):
        return self.collection.find_one({Keys.systemIDKey:self.sid}, sort=[(Keys.systemVersionKey, pymongo.DESCENDING)])