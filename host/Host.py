from typing import Any
from MetadataManagerCore.host.HostStatus import HostStatus
from MetadataManagerCore.mongodb_manager import MongoDBManager
import socket
from datetime import datetime, timedelta
import time

class Host(object):
    # If the host doesn't update it's heartbeat time within this time interval it will be considered dead.
    dyingTimeInSeconds = 5.0

    def __init__(self, dbManager: MongoDBManager) -> None:
        super().__init__()

        self.dbManager = dbManager
        self.hostname = socket.gethostname()

        self.heartbeatIntervalInSeconds = 1.0
        self.isRunning = True
        self.lastUpdateTime = None

        lastHeartbeatTime = self.lastHeartbeatTime
        if not lastHeartbeatTime or (datetime.now() - lastHeartbeatTime > timedelta(seconds=Host.dyingTimeInSeconds)):
            Host.updateMongoDBEntry(self.dbManager, self.hostname, 'instances', 1)
        else:
            Host.incInstanceCount(self.dbManager, self.hostname, 1)
            
        self.signalHeartbeat()

    def runHeartbeat(self):
        while self.isRunning:
            if datetime.now() - self.lastUpdateTime > timedelta(seconds=self.heartbeatIntervalInSeconds):
                self.signalHeartbeat()

            time.sleep(self.heartbeatIntervalInSeconds)
            
    def signalHeartbeat(self):
        Host.updateMongoDBEntry(self.dbManager, self.hostname, 'heartbeat_time', datetime.now())
        self.lastUpdateTime = datetime.now()

    @property
    def lastHeartbeatTime(self):
        hostInfo = self.dbManager.hostsCollection.find_one({'_id': self.hostname})
        if hostInfo:
            lastTime = hostInfo.get('heartbeat_time')
            if lastTime:
                return lastTime

        return None

    def shutdown(self):
        self.isRunning = False
        if self.status != HostStatus.Dead:
            Host.incInstanceCount(self.dbManager, self.hostname, -1)

    @property
    def instanceCount(self):
        hostInfo = self.dbManager.hostsCollection.find_one({'_id': self.hostname})

        if hostInfo:
            return hostInfo.get('instances')

        return 0

    @property
    def status(self) -> HostStatus:
        hostInfo = self.dbManager.hostsCollection.find_one({'_id': self.hostname})
        if hostInfo:
            status = hostInfo.get('status')
            if status:
                return HostStatus(status)

        return None

    @staticmethod
    def updateMongoDBEntry(dbManager: MongoDBManager, hostname: str, key: str, value: Any):
        dbManager.hostsCollection.update_one({'_id': hostname}, {'$set': {key: value}}, upsert=True)
        
    @staticmethod
    def incInstanceCount(dbManager: MongoDBManager, hostname: str, amount: int):
        dbManager.hostsCollection.update_one({'_id': hostname}, {'$inc': {'instances': amount}}, upsert=True)