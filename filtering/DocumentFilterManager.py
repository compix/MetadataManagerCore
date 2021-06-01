from MetadataManagerCore import Keys
from MetadataManagerCore.Event import Event
from MetadataManagerCore.filtering.DocumentFilter import DocumentFilter
from typing import List
from MetadataManagerCore.filtering.DocumentFilter import DocumentFilter
from MetadataManagerCore.mongodb_manager import MongoDBManager
import os
import re
from MetadataManagerCore.animation import anim_util

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

    def insertFilter(self, documentFilter: DocumentFilter, idx: int):
        # Do not add duplicates:
        customFilter = self.getFilterFromLabel(documentFilter.uniqueFilterLabel)
        if customFilter:
            self.customFilters.remove(customFilter)

        self.customFilters.insert(idx, documentFilter)

        self.onFilterListUpdateEvent()

    def applyFilters(self, document, filters : List[DocumentFilter]):
        if filters == None:
            return True

        for docFilter in filters:
            if not docFilter.apply(document):
                return False
                
        return True

    def yieldFilteredDocuments(self, collectionName : str, mongodbFilter : dict = {}, distinctionText : str = '', filters : List[DocumentFilter] = None):
        if filters == None:
            filters = []

        for filter in filters:
            filter.preApply()
            
        for document in self.dbManager.getFilteredDocuments(collectionName, mongodbFilter, distinctionText):
            if self.applyFilters(document, filters):
                yield document

        for filter in filters:
            filter.postApply()

    def hasPreviewFilter(self, document):
        try:
            previewPath = document.get(Keys.preview)

            if '#' in previewPath:
                return anim_util.hasExistingFrameFilenames(previewPath)
                
            return os.path.exists(previewPath)
        except:
            return False

    def stringToFilter(self, filterString : str) -> dict:
        return self.dbManager.stringToFilter(filterString)