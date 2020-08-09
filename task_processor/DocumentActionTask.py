from MetadataManagerCore.filtering.DocumentFilter import DocumentFilter
from MetadataManagerCore.filtering.DocumentFilterManager import DocumentFilterManager
from MetadataManagerCore.task_processor.Task import Task
from MetadataManagerCore.actions.ActionManager import ActionManager
from MetadataManagerCore.filtering.DocumentFilterManager import DocumentFilterManager
import logging

logger = logging.getLogger(__name__)

class DocumentActionTask(Task):
    def __init__(self, actionManager : ActionManager, documentFilterManager : DocumentFilterManager):
        super().__init__()

        self.actionManager = actionManager
        self.documentFilterManager = documentFilterManager

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

        customPythonFilterDicts = dataDict.get('customDocumentFilters', {})

        customPythonFilters = []
        for filterDict in customPythonFilterDicts:
            uniqueLabel = filterDict['uniqueFilterLabel']
            documentFilter = self.documentFilterManager.getFilterFromLabel(uniqueLabel)
            documentFilter.setFromDict(filterDict)
            customPythonFilters.append(documentFilter)

        if actionId and collectionNames and documentFilterString:
            action = self.actionManager.getActionById(actionId)
            
            if action:
                if len(collectionNames) == 0:
                    logger.warn(f"No collections were specified.")
                    return

                documentFilter = self.documentFilterManager.stringToFilter(documentFilterString)
                numDocumentsProcessed = 0

                for collectionName in collectionNames:
                    for document in self.documentFilterManager.yieldFilteredDocuments(collectionName, documentFilter, distinctionFilterString, customPythonFilters):
                        numDocumentsProcessed += 1
                        action.execute(document)
                
                if numDocumentsProcessed == 0:
                    logger.warn("No documents were processed.")
            else:
                raise RuntimeError(f'Unknown actionId: {actionId}')