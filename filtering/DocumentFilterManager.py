from MetadataManagerCore import Keys
from MetadataManagerCore.Event import Event
from MetadataManagerCore.filtering.DocumentFilter import DocumentFilter
from typing import List
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

        self.collectionToFiltersDict = dict()

    def getFilterFromLabel(self, uniqueFilterLabel : str, collectionNames: List[str] = None):
        customFilters = self.getFilters(collectionNames)

        for customFilter in customFilters:
            if customFilter.uniqueFilterLabel == uniqueFilterLabel:
                return customFilter
        
        return None

    def getFilters(self, collectionNames: List[str] = None) -> List[DocumentFilter]:
        if collectionNames == None:
            collectionNames = []

        filters = [f for f in self.customFilters]
        for collectionName in collectionNames:
            filters += self.collectionToFiltersDict.get(collectionName, [])
        
        return filters

    def clearFilters(self):
        self.customFilters.clear()
        self.collectionToFiltersDict.clear()

        self.onFilterListUpdateEvent()

    def addFilter(self, documentFilter : DocumentFilter, collectionName: str = None):
        if collectionName:
            documentFilter.collectionName = collectionName
            customFilters = self.collectionToFiltersDict.setdefault(collectionName, [])
            existingFilter = next((cf for cf in customFilters if cf.uniqueFilterLabel == documentFilter.uniqueFilterLabel), None)
            if existingFilter:
                customFilters.remove(existingFilter)

            customFilters.append(documentFilter)
        else:
            # Do not add duplicates:
            customFilter = self.getFilterFromLabel(documentFilter.uniqueFilterLabel)
            if customFilter:
                self.customFilters.remove(customFilter)

            self.customFilters.append(documentFilter)

        self.onFilterListUpdateEvent()

    def insertFilter(self, documentFilter: DocumentFilter, idx: int, collectionName: str = None):
        if collectionName:
            self.collectionToFiltersDict.setdefault(collectionName, []).insert(idx, documentFilter)
        else:
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

        filters = filters + self.collectionToFiltersDict.get(collectionName, [])

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