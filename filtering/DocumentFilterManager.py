from MetadataManagerCore.Event import Event
from MetadataManagerCore.filtering.DocumentFilter import DocumentFilter
from typing import List
from MetadataManagerCore.filtering.DocumentFilter import DocumentFilter
from MetadataManagerCore.mongodb_manager import MongoDBManager
import os
import re

class DocumentFilterManager(object):
    def __init__(self, dbManager : MongoDBManager) -> None:
        super().__init__()

        self.dbManager = dbManager
        self.customFilters : List[DocumentFilter] = []

        self.onFilterListUpdateEvent = Event()

        self.addFilter(DocumentFilter(self.hasPreviewFilter, 'Has Preview'))

    def getFilterFromLabel(self, uniqueFilterLabel : str):
        for customFilter in self.customFilters:
            if customFilter.uniqueFilterLabel == uniqueFilterLabel:
                return customFilter
        
        return None

    def deleteFilterByLabel(self, uniqueFilterLabel : str):
        customFilter = self.getFilterFromLabel(uniqueFilterLabel)
        if customFilter:
            self.customFilters.remove(customFilter)

        self.onFilterListUpdateEvent()

    def clearFilters(self):
        self.customFilters.clear()

        self.onFilterListUpdateEvent()

    def addFilter(self, documentFilter : DocumentFilter):
        # Do not add duplicates:
        customFilter = self.getFilterFromLabel(documentFilter.uniqueFilterLabel)
        if customFilter:
            self.customFilters.remove(customFilter)

        self.customFilters.append(documentFilter)

        self.onFilterListUpdateEvent()

    def applyFilters(self, document, filters : List[DocumentFilter]):
        for docFilter in filters:
            if not docFilter.apply(document):
                return False
                
        return True

    def yieldFilteredDocuments(self, collectionName : str, mongodbFilter : dict = {}, distinctionText : str = '', filters : List[DocumentFilter] = None):
        for document in self.dbManager.getFilteredDocuments(collectionName, mongodbFilter, distinctionText):
            if self.applyFilters(document, filters):
                yield document

    def hasPreviewFilter(self, document):
        try:
            previewPath = document.get('preview')
            animPattern = r'(.*)\.(#+)\.(.*)'
            animMatch = re.match(animPattern, previewPath)
            if animMatch:
                hashtagCount = len(animMatch.group(2))
                animIndexStr = ''.join(['0' for _ in range(hashtagCount)])
                firstFramePath = re.sub(animPattern, fr'\1.{animIndexStr}.\3', previewPath)
                altFirstFramePath = re.sub(animPattern, fr'\1.{animIndexStr[:-1] + "1"}.\3', previewPath)
                return os.path.exists(firstFramePath) or os.path.exists(altFirstFramePath[:-1] + '1')
                
            return os.path.exists(previewPath)
        except:
            return False

    def stringToFilter(self, filterString : str) -> dict:
        return self.dbManager.stringToFilter(filterString)