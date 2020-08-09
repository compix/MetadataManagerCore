from enum import Enum
import os

from MetadataManagerCore.Event import Event
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

class SFTPWatchDog(object):
    """Checks a specified remote folder on an SFTP server for new files. Requires the pysftp module.
    Example:
    myHostname = "127.0.0.1"
    myUsername = "tester"
    myPassword = "password"

    sftpWatchDog = SFTPWatchDog('/Test_Dir', myHostname, myUsername, myPassword)
    sftpWatchDog.setPollingIntervalInSeconds(5.0)
    sftpWatchDog.run() # Blocking, you may want to run this on a different thread.
    """
    def __init__(self, remoteFolder : str, host : str, username : str, password : str) -> None:
        """Creates the SFTP watch dog.

        Args:
            remoteFolder (str): The remote folder to watch.
            serializationFilename (str): The absolute path to the filename used for serialization of watch dog info.
            host (str): The SFTP server host.
            username (str): SFTP server username.
            password (str): SFTP server password.
        """
        super().__init__()

        self.remoteFolder = remoteFolder
        self.host = host
        self.username = username
        self.password = password

        self.pollingInvervalInSeconds = 1.0

        self.running = False

        self.lastTime = datetime.now()

        self.knownFiles = dict()

        self.fileModifiedEvent = Event()
        self.fileAddedEvent = Event()

    def setPollingIntervalInSeconds(self, interval : float):
        self.pollingInvervalInSeconds = interval

    def addOrUpdateFile(self, filename, dateModified : datetime):
        fileInfoDict = self.knownFiles.get(filename)
        if fileInfoDict:
            curDateModified = fileInfoDict['dateModified']
            if dateModified - curDateModified > timedelta(microseconds=1):
                fileInfoDict['dateModified'] = dateModified
                self.knownFiles[filename] = fileInfoDict
                self.fileModifiedEvent(filename)
                print(f'On File Modified: {filename}')
        else:
            self.fileAddedEvent(filename)
            self.knownFiles[filename] = {
                'dateModified': dateModified
            }
            print(f'On File Added: {filename}')

    def run(self):
        import pysftp

        self.running = True

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        with pysftp.Connection(host=self.host, username=self.username, password=self.password, cnopts=cnopts) as sftp:
            print("Connection successfully established ...")

            sftp.cwd(self.remoteFolder)

            # First add the files that are already present in the directory:
            sftpAttributes = sftp.listdir_attr()
            for sftpAttribute in sftpAttributes:
                dateModified = datetime.fromtimestamp(sftpAttribute.st_mtime)
                if sftpAttribute.filename:
                    self.knownFiles[sftpAttribute.filename] = {
                        'dateModified': dateModified
                    }

            while self.running:
                now = datetime.now()
                if now - self.lastTime > timedelta(seconds=self.pollingInvervalInSeconds):
                    self.lastTime = now
                    sftpAttributes = sftp.listdir_attr()
                    for sftpAttribute in sftpAttributes:
                        dateModified = datetime.fromtimestamp(sftpAttribute.st_mtime)
                        self.addOrUpdateFile(sftpAttribute.filename, dateModified)