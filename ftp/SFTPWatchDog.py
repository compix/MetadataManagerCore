from MetadataManagerCore.file.WatchDog import WatchDog
import os
from typing import List

from MetadataManagerCore.Event import Event
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class SFTPWatchDog(WatchDog):
    """Checks a specified remote folder on an SFTP server for new files. Requires the pysftp module.
    Example:
    myHostname = "127.0.0.1"
    myUsername = "tester"
    myPassword = "password"

    sftpWatchDog = SFTPWatchDog('/Test_Dir', myHostname, myUsername, myPassword)
    sftpWatchDog.setPollingIntervalInSeconds(5.0)
    sftpWatchDog.run() # Blocking, you may want to run this on a different thread.
    """
    def __init__(self, remoteFolder : str, host : str, username : str, password : str, watchedExtensions: List[str] = None, recursive = False) -> None:
        """Creates the SFTP watch dog.

        Args:
            remoteFolder (str): The remote folder to watch.
            serializationFilename (str): The absolute path to the filename used for serialization of watch dog info.
            host (str): The SFTP server host.
            username (str): SFTP server username.
            password (str): SFTP server password.
        """
        if remoteFolder.startswith('.'):
            remoteFolder = remoteFolder[1:]

        super().__init__(remoteFolder, watchedExtensions, recursive)

        self.host = host
        self.username = username
        self.password = password
        self.sftp = None

        self.pollingIntervalInSeconds = 1.0

        self._running = False

        self.lastTime = datetime.now()

        self.knownFiles = dict()

        self.onConnectionEstablished = Event()

    def setPollingIntervalInSeconds(self, interval : float):
        self.pollingIntervalInSeconds = interval

    def processFile(self, filename):
        sftpAttribute = self.sftp.stat(filename)
        if not sftpAttribute:
            return

        dateModified = datetime.fromtimestamp(sftpAttribute.st_mtime)

        fileInfoDict = self.knownFiles.get(filename)
        if fileInfoDict:
            curDateModified = fileInfoDict['dateModified']
            if dateModified - curDateModified > timedelta(microseconds=1):
                fileInfoDict['dateModified'] = dateModified
                self.knownFiles[filename] = fileInfoDict
                self.onFileModified(filename)
        else:
            self.onFileCreated(filename)
            self.knownFiles[filename] = {
                'dateModified': dateModified
            }

    def run(self):
        import pysftp

        self._running = True

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        logging.getLogger("paramiko").setLevel(logging.WARNING)

        with pysftp.Connection(host=self.host, username=self.username, password=self.password, cnopts=cnopts) as self.sftp:
            print("Connection successfully established ...")
            self.onConnectionEstablished()
            
            # First add the files that are already present in the directory:
            def initFile(filename):
                sftpAttribute = self.sftp.stat(filename)
                if sftpAttribute:
                    dateModified = datetime.fromtimestamp(sftpAttribute.st_mtime)
                    self.knownFiles[filename] = {'dateModified': dateModified}

            self.processFiles(initFile)

            while self._running:
                now = datetime.now()
                if now - self.lastTime > timedelta(seconds=self.pollingIntervalInSeconds):
                    self.lastTime = now
                    self.processFiles(self.processFile)

    def processFiles(self, fileHandler):
        def checkedFileHandler(filename):
            if self.checkExtension(filename):
                fileHandler(filename)

        self.sftp.walktree(self.watchedFolder, checkedFileHandler, lambda _: {}, lambda _: {}, recurse=self.recursive)

    def copyFile(self, remoteFilename, localFilename):
        self.sftp.get(remoteFilename, localFilename)

    def renameFile(self, srcRemoteFilename, destRemoteFilename):
        self.sftp.rename(srcRemoteFilename, destRemoteFilename)

    def stop(self):
        self._running = False

    @property
    def running(self):
        return self._running