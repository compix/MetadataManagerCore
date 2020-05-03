from MetadataManagerCore.task_processor.Task import Task
from MetadataManagerCore.actions.ActionManager import ActionManager
from MetadataManagerCore.mongodb_manager import MongoDBManager
import logging

logger = logging.getLogger(__name__)

class DocumentActionTask(Task):
    def __init__(self, actionManager : ActionManager, dbManager : MongoDBManager):
        super().__init__()

        self.actionManager = actionManager
        self.dbManager = dbManager

    def getEntryVerified(self, dataDict, key):
        value = dataDict.get(key)
        if value == None:
            raise RuntimeError(f"The data dictionary is missing the key: {key}")
        
        return value

    def execute(self, dataDict: dict):
        actionId = self.getEntryVerified(dataDict, 'actionId')
        collectionNames = self.getEntryVerified(dataDict, 'collections')
        documentFilterString = self.getEntryVerified(dataDict, 'documentFilter')
        distinctionFilterString = self.getEntryVerified(dataDict, 'distinctionFilter')

        if actionId and collectionNames and documentFilterString:
            action = self.actionManager.getActionById(actionId)
            
            if action:
                if len(collectionNames) == 0:
                    logger.warn(f"No collections were specified.")
                    return

                documentFilter = self.dbManager.stringToFilter(documentFilterString)
                numDocumentsProcessed = 0

                for collectionName in collectionNames:
                    for document in self.dbManager.getFilteredDocuments(collectionName, documentFilter, distinctionFilterString):
                        numDocumentsProcessed += 1
                        action.execute(document)
                
                if numDocumentsProcessed == 0:
                    logger.warn("No documents were processed.")
            else:
                raise RuntimeError(f'Unknown actionId: {actionId}')