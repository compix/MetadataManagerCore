from typing import Set
from MetadataManagerCore.file.FileHandler import FileHandler

FILE_HANDLER_CLASSES : Set[FileHandler] = set()

def registerFileHandlerClass(handlerCls):
    FILE_HANDLER_CLASSES.add(handlerCls)
    
    return handlerCls