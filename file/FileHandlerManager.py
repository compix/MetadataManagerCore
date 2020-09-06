from typing import Dict
from MetadataManagerCore.file.FileHandler import FileHandler
import logging
from MetadataManagerCore.file.FileHandlerRegistry import FILE_HANDLER_CLASSES

logger = logging.getLogger(__name__)

class FileHandlerManager(object):
    def __init__(self) -> None:
        super().__init__()

        self.fileHandlerClassNameToClassMap = dict()

        for fileHandlerClass in FILE_HANDLER_CLASSES:
            self.registerFileHandlerClass(fileHandlerClass)

    def registerFileHandlerClass(self, fileHandlerClass):
        if fileHandlerClass.__name__ in self.fileHandlerClassNameToClassMap:
            logger.warning(f'The file handler class {fileHandlerClass.__name__} is already regsitered.')
            return

        self.fileHandlerClassNameToClassMap[fileHandlerClass.__name__] = fileHandlerClass

    def constructFileHandlerFromDict(self, infoDict: dict):
        className = infoDict.get('class')
        fileHandlerClass : FileHandler = self.fileHandlerClassNameToClassMap.get(className)

        if fileHandlerClass:
            fileHandler = fileHandlerClass()
            fileHandler.setupFromDict(infoDict)
            return fileHandler
    
        return None

    def getAllHandlerClassNames(self):
        return self.fileHandlerClassNameToClassMap.keys()
