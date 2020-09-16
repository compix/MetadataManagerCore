from MetadataManagerCore.host.Host import Host
from MetadataManagerCore.host.HostStatus import HostStatus
from typing import Any, Dict
from MetadataManagerCore.mongodb_manager import MongoDBManager
import time
from datetime import datetime, timedelta
from MetadataManagerCore.Event import Event
from concurrent.futures import ThreadPoolExecutor

class HostController(object):
    def __init__(self, dbManager: MongoDBManager) -> None:
        super().__init__()

        self.dbManager = dbManager
        self.isRunning = True

        # Arguments are:
        self._onHostStatusChangedEvent = Event()

        self.threadPoolExecutor = ThreadPoolExecutor(max_workers=1)

        self._hostStatusInfo: Dict[str,HostStatus] = dict()
        self.thisHost = Host(dbManager)

    @property
    def hostStatusInfo(self) -> Dict[str,HostStatus]:
        """
        Returns:
            Dict[str,HostStatus]: Key: Hostname
        """
        return self._hostStatusInfo

    @property
    def isHostAlreadyRunning(self) -> bool:
        return self.thisHost.status == HostStatus.Online

    @property
    def onHostStatusChangedEvent(self):
        """ Event arguments: (hostname: str, prevStatus: HostStatus, newStatus: HostStatus)
        """
        return self._onHostStatusChangedEvent

    def run(self):
        # Run this host:
        self.threadPoolExecutor.submit(self.thisHost.runHeartbeat)

        # Initialize status:
        hosts = self.getHosts()
        if hosts:
            for hostInfo in hosts:
                hostname = hostInfo.get('_id')
                hostStatus = hostInfo.get('status')
                if hostStatus:
                    self._hostStatusInfo[hostname] = HostStatus(hostStatus)
                else:
                    self._hostStatusInfo[hostname] = None

        self.updateHostStatus(self.thisHost.hostname, self.hostStatusInfo.get(self.thisHost.hostname), HostStatus.Online)

        if self.thisHost.instanceCount <= 0:
            Host.updateMongoDBEntry(self.dbManager, self.thisHost.hostname, 'instances', 1)

        # Check for host status changes and heartbeat time:
        while self.isRunning:
            hosts = self.getHosts()

            if hosts:
                for hostInfo in hosts:
                    hostname = hostInfo.get('_id')
                    heartbeatTime = hostInfo.get('heartbeat_time')
                    hostStatus = hostInfo.get('status')
                    hostStatus = HostStatus(hostStatus) if hostStatus else None

                    cachedStatus = self._hostStatusInfo.get(hostname)

                    if cachedStatus != hostStatus:
                        # Status was changed by another host controller:
                        self._hostStatusInfo[hostname] = HostStatus(hostStatus)
                        self.onHostStatusChangedEvent(hostname, cachedStatus, hostStatus)
                    else:
                        if datetime.now() - heartbeatTime > timedelta(seconds=Host.dyingTimeInSeconds):
                            if hostStatus == HostStatus.Online:
                                # Host died:
                                self.updateHostStatus(hostname, hostStatus, HostStatus.Dead)

            time.sleep(1.0)

        self.thisHost.shutdown()
        self.threadPoolExecutor.shutdown(True)

        # Set the status to Offline only if all instances are closed:
        if self.thisHost.instanceCount == 0:
            self.updateHostStatus(self.thisHost.hostname, HostStatus.Online, HostStatus.Offline)

    def getHosts(self) -> dict:
        return self.dbManager.hostsCollection.find({})

    def updateHostStatus(self, hostname: str, prevStatus: HostStatus, newStatus: HostStatus):
        self._hostStatusInfo[hostname] = newStatus
        Host.updateMongoDBEntry(self.dbManager, hostname, 'status', str(newStatus.value))
        if newStatus == HostStatus.Dead:
            Host.updateMongoDBEntry(self.dbManager, hostname, 'instances', 0)

        self.onHostStatusChangedEvent(hostname, prevStatus, newStatus)

    def shutdown(self):
        self.isRunning = False