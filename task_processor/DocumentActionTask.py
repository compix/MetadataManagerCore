from MetadataManagerCore.filtering.DocumentFilter import DocumentFilter
from MetadataManagerCore.filtering.DocumentFilterManager import DocumentFilterManager
from MetadataManagerCore.task_processor.DataRetrievalType import DataRetrievalType
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

        try:
            dataRetrievalType = DataRetrievalType(dataDict.get('dataRetrievalType', DataRetrievalType.UseDocumentFilter))
        except:
            raise RuntimeError(f'Unknown data retrieval type: {dataDict.get("dataRetrievalType")}')

        action = self.actionManager.getActionById(actionId)
        if action is None:
            raise RuntimeError(f'Unknown actionId: {actionId}')
            
        if dataRetrievalType == DataRetrievalType.UseDocumentFilter:
            collectionNames = self.getEntryVerified(dataDict, 'collections')
            documentFilterString = self.getEntryVerified(dataDict, 'documentFilter')
            distinctionFilterString = self.getEntryVerified(dataDict, 'distinctionFilter')

            customPythonFilterDicts = dataDict.get('customDocumentFilters', [])
            if customPythonFilterDicts != None:
                customPythonFilterDicts = []

            customPythonFilters = []
            for filterDict in customPythonFilterDicts:
                uniqueLabel = filterDict['uniqueFilterLabel']
                documentFilter = self.documentFilterManager.getFilterFromLabel(uniqueLabel, collectionNames)
                documentFilter.setFromDict(filterDict)
                customPythonFilters.append(documentFilter)

            if collectionNames and documentFilterString:
                if len(collectionNames) == 0:
                    raise RuntimeError("No collections were specified.")

                documentFilter = self.documentFilterManager.stringToFilter(documentFilterString)
                numDocumentsProcessed = 0

                for collectionName in collectionNames:
                    for document in self.documentFilterManager.yieldFilteredDocuments(collectionName, documentFilter, distinctionFilterString, customPythonFilters):
                        numDocumentsProcessed += 1
                        action.execute(document)
                
                if numDocumentsProcessed == 0:
                    raise RuntimeError("No documents were processed.")

        elif dataRetrievalType == DataRetrievalType.UseSubmittedData:
            submittedData = dataDict.get('submittedData')
            action.execute(submittedData)