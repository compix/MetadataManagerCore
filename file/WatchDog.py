from abc import ABCMeta, abstractmethod
from typing import List
from MetadataManagerCore.Event import Event
import os
import logging

logger = logging.getLogger(__name__)

class WatchDog(object,metaclass=ABCMeta):
    def __init__(self, watchedFolder: str, watchedExtensions: List[str] = None, recursive=False) -> None:
        super().__init__()

        self.watchedFolder = watchedFolder
        self.watchedExtensions = watchedExtensions
        self.recursive = recursive

        self.fileModifiedEvent = Event()
        self.fileCreatedEvent = Event()

    @abstractmethod
    def run(self):
        ...

    @property
    @abstractmethod
    def running(self):
        ...

    @abstractmethod
    def stop(self):
        ...

    @abstractmethod
    def processFiles(self, fileHandler):
        ...

    @abstractmethod
    def copyFile(self, srcFilename, destFilename):
        ...

    @abstractmethod
    def renameFile(self, srcFilename, destFilename):
        ...

    def copyFiles(self, destPath):
        def copy(filename):
            self.copyFile(filename, os.path.join(destPath, os.path.basename(filename)))

        self.processFiles(copy)

    def moveFiles(self, destDirectory):
        def move(filename):
            self.moveFile(filename, os.path.join(destDirectory, os.path.basename(filename)))

        self.processFiles(move)

    def moveFile(self, srcFilename, destFilename):
        self.renameFile(srcFilename, os.path.join(destFilename, os.path.basename(srcFilename)))

    def onFileModified(self, filename):
        logger.info(f'File modified: {filename}')
        self.fileModifiedEvent(filename)

    def onFileCreated(self, filename):
        logger.info(f'File created: {filename}')
        self.fileCreatedEvent(filename)

    def checkExtension(self, filename):
        if self.watchedExtensions:
            _, ext = os.path.splitext(filename)
            if ext in self.watchedExtensions:
                return True
        else:
            return True

        return False