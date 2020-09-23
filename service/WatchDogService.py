from MetadataManagerCore.file.WatchDogFileHandler import WatchDogFileHandler
from MetadataManagerCore.file.FileHandlerManager import FileHandlerManager
from MetadataManagerCore.file.FileSystemWatchDog import FileSystemWatchDog
from MetadataManagerCore.file.WatchDog import WatchDog
from typing import List
from MetadataManagerCore.service.Service import Service, ServiceStatus
from MetadataManagerCore.ftp.SFTPWatchDog import SFTPWatchDog

class WatchDogService(Service):
    def __init__(self) -> None:
        super().__init__()

        self.watchedFolder : str = None
        self.watchedFileExtensions : str = None
        self.watchDog : WatchDog = None
        self.recursive : bool = False
        self.processExistingFiles = False
        self.fileHandler = None
        self.fileHandlerManager = None
        self.existingFileHandler = None
        self.fileCreatedHandler = None
        self.fileModifiedHandler = None

    def setup(self, watchedFolder: str, watchedFileExtensions: List[str], recursive = False):
        self.watchedFolder = watchedFolder
        self.watchedFileExtensions = watchedFileExtensions
        self.watchDog : WatchDog = None
        self.recursive = recursive

    def setFileHandlers(self, existingFileHandler: WatchDogFileHandler = None, 
                              fileCreatedHandler:  WatchDogFileHandler = None, 
                              fileModifiedHandler: WatchDogFileHandler = None):
        self.existingFileHandler = existingFileHandler
        self.fileCreatedHandler = fileCreatedHandler
        self.fileModifiedHandler = fileModifiedHandler

        if self.existingFileHandler:
            self.existingFileHandler.watchDog = self.watchDog

        self.setupEvents()

    def initSFTPWatchDog(self, host: str, username: str, password: str, pollingIntervalInSeconds: float):
        self.watchDog = SFTPWatchDog(self.watchedFolder, host, username, password, self.watchedFileExtensions, self.recursive)
        self.watchDog.setPollingIntervalInSeconds(pollingIntervalInSeconds)
        self.watchDog.onConnectionEstablished.subscribe(self.onSFTPWatchDogConnectionEstablished)

    def initWatchDog(self):
        self.watchDog = FileSystemWatchDog(self.watchedFolder, self.watchedFileExtensions, self.recursive)

    def setupEvents(self):
        if self.fileCreatedHandler:
            self.watchDog.fileCreatedEvent.subscribe(self.fileCreatedHandler)

        if self.fileModifiedHandler:
            self.watchDog.fileModifiedEvent.subscribe(self.fileModifiedHandler)

    def onStatusChanged(self, status: ServiceStatus):
        if status != ServiceStatus.Running:
            if self.watchDog and self.watchDog.running:
                self.watchDog.stop()

    def _run(self):
        if self.watchDog:
            if self.existingFileHandler and isinstance(self.watchDog, FileSystemWatchDog):
                self.watchDog.processFiles(self.existingFileHandler)

            self.watchDog.run()

    def onSFTPWatchDogConnectionEstablished(self):
        if self.existingFileHandler:
            self.watchDog.processFiles(self.existingFileHandler)

    def asDict(self):
        isSFTP = isinstance(self.watchDog, SFTPWatchDog)
        host = self.watchDog.host if isSFTP else None
        username = self.watchDog.username if isSFTP else None
        password = self.watchDog.password if isSFTP else None
        pollingIntervalInSeconds = self.watchDog.pollingIntervalInSeconds if isSFTP else None
        serviceDict = WatchDogService.constructDict(self.watchedFolder, self.watchedFileExtensions, 
                                             self.recursive, isSFTP, host, username, 
                                             password, pollingIntervalInSeconds)

        # Save file handler info:
        if self.existingFileHandler:
            serviceDict['existingFileHandler'] = self.existingFileHandler.asDict()

        if self.fileCreatedHandler:
            serviceDict['fileCreatedHandler'] = self.fileCreatedHandler.asDict()
            
        if self.fileModifiedHandler:
            serviceDict['fileModifiedHandler'] = self.fileModifiedHandler.asDict()

        return serviceDict

    @staticmethod
    def constructDict(watchedFolder: str, watchedFileExtensions: List[str], recursive: bool, 
                      isSFTP: bool, sftpHost: str, sftpUsername: str, sftpPassword: str, 
                      sftpPollingIntervalInSeconds: float, 
                      existingFileHandlerDict: dict = None, fileCreatedHandlerDict: dict = None, fileModifiedHandlerDict: dict = None):
        base = {
            'watchedFolder': watchedFolder,
            'watchedFileExtensions': watchedFileExtensions,
            'isSFTP': isSFTP,
            'recursive': recursive
        }

        if isSFTP:
            base = {
                **base,
                'host': sftpHost,
                'username': sftpUsername,
                'password': sftpPassword,
                'pollingIntervalInSeconds': sftpPollingIntervalInSeconds if sftpPollingIntervalInSeconds else 15.0
            }

        if existingFileHandlerDict:
            base['existingFileHandler'] = existingFileHandlerDict

        if fileCreatedHandlerDict:
            base['fileCreatedHandler'] = fileCreatedHandlerDict
            
        if fileModifiedHandlerDict:
            base['fileModifiedHandler'] = fileModifiedHandlerDict

        return base

    def setupFromDict(self, theDict: dict):
        self.setup(theDict.get('watchedFolder'), theDict.get('watchedFileExtensions'), theDict.get('recursive'))
        if theDict.get('isSFTP'):
            self.initSFTPWatchDog(theDict.get('host'), theDict.get('username'), theDict.get('password'), theDict.get('pollingIntervalInSeconds'))
        else:
            self.initWatchDog()

        # Load and setup file handlers:
        existingFileHandlerDict = theDict.get('existingFileHandler')
        fileCreatedHandlerDict = theDict.get('fileCreatedHandler')
        fileModifiedHandlerDict = theDict.get('fileModifiedHandler')

        self.fileHandlerManager = self.serviceRegistry.fileHandlerManager
        existingFileHandler = self.fileHandlerManager.constructFileHandlerFromDict(existingFileHandlerDict) if existingFileHandlerDict else None
        fileCreatedHandler = self.fileHandlerManager.constructFileHandlerFromDict(fileCreatedHandlerDict) if fileCreatedHandlerDict else None
        fileModifiedHandler = self.fileHandlerManager.constructFileHandlerFromDict(fileModifiedHandlerDict) if fileModifiedHandlerDict else None

        self.setFileHandlers(existingFileHandler, fileCreatedHandler, fileModifiedHandler)