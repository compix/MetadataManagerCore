from typing import Any
from MetadataManagerCore.mongodb_manager import MongoDBManager
import socket
from datetime import datetime, timedelta
import time
import os

class HostProcess(object):
    # If the host doesn't update it's heartbeat time within this time interval it will be considered dead.
    dyingTimeInSeconds = 5.0

    def __init__(self, dbManager: MongoDBManager) -> None:
        super().__init__()

        self.dbManager = dbManager
        self.hostname = socket.gethostname()

        self.heartbeatIntervalInSeconds = 1.0
        self.isRunning = True
        self.lastUpdateTime = None

        self.signalHeartbeat()

    def runHeartbeat(self):
        while self.isRunning:
            if datetime.utcnow() - self.lastUpdateTime > timedelta(seconds=self.heartbeatIntervalInSeconds):
                self.signalHeartbeat()

            time.sleep(self.heartbeatIntervalInSeconds)
            
    def signalHeartbeat(self):
        HostProcess.updateMongoDBEntry(self.dbManager, self.hostname, self.pid, 'heartbeat_time', datetime.utcnow())
        self.lastUpdateTime = datetime.utcnow()

    @property
    def lastHeartbeatTime(self):
        processInfo = self.findDBEntry()
        if processInfo:
            lastTime = processInfo.get('heartbeat_time')
            if lastTime:
                return lastTime

        return None

    def findDBEntry(self):
        return self.dbManager.hostProcessesCollection.find_one({'hostname': self.hostname, 'pid': self.pid})

    @property
    def pid(self) -> int:
        return os.getpid()

    def shutdown(self):
        self.isRunning = False

        # Remove db entry:
        HostProcess.delete(self.dbManager, self.hostname, self.pid)

    @staticmethod
    def updateMongoDBEntry(dbManager: MongoDBManager, hostname: str, pid: int, key: str, value: Any):
        dbManager.hostProcessesCollection.update_one({'hostname': hostname, 'pid': pid}, {'$set': {key: value}}, upsert=True)

    @staticmethod
    def delete(dbManager: MongoDBManager, hostname: str, pid: int):
        dbManager.hostProcessesCollection.delete_one({'hostname': hostname, 'pid': pid})