from MetadataManagerCore.host.HostProcess import HostProcess
from MetadataManagerCore.host.HostProcessStatus import HostProcessStatus
from typing import Any, Dict
from MetadataManagerCore.mongodb_manager import MongoDBManager
import time
from datetime import datetime, timedelta
from MetadataManagerCore.Event import Event
from concurrent.futures import ThreadPoolExecutor
import logging

class HostProcessInfo(object):
    def __init__(self, hostname: str, pid: int) -> None:
        super().__init__()

        self.hostname = hostname
        self.pid = pid
        self.dirty = False

class HostProcessController(object):
    def __init__(self, dbManager: MongoDBManager) -> None:
        super().__init__()

        self.dbManager = dbManager
        self.isRunning = True
        self.logger = logging.getLogger(__name__)

        self.dbManager.hostProcessesCollection.create_index("heartbeat_time", expireAfterSeconds=HostProcess.dyingTimeInSeconds)

        # Arguments are:
        self._onHostProcessAddedEvent = Event()
        self._onHostProcessRemovedEvent = Event()

        self.threadPoolExecutor = ThreadPoolExecutor(max_workers=1)

        self._hostProcessInfos: Dict[str,HostProcessInfo] = dict()
        self.thisHost = HostProcess(dbManager)

    @property
    def hostProcessInfos(self) -> Dict[str,HostProcessInfo]:
        """
        Returns:
            Dict[str,HostProcessInfo]: Key: host process id
        """
        return self._hostProcessInfos

    @property
    def onHostProcessAddedEvent(self):
        """ Event arguments: (hostname: str, pid: int)
        """
        return self._onHostProcessAddedEvent

    @property
    def onHostProcessRemovedEvent(self):
        """ Event arguments: (hostname: str, pid: int)
        """
        return self._onHostProcessRemovedEvent

    @staticmethod
    def getHostProcessId(hostname: str, pid: int):
        return f'{hostname}_{pid}'

    def run(self):
        self.logger.info('Running controller.')

        # Run this host:
        self.threadPoolExecutor.submit(self.thisHost.runHeartbeat)

        # Initialize info of available host processes:
        hostProcesses = self.getHostProcesses()
        if hostProcesses:
            for processInfo in hostProcesses:
                hostname = processInfo.get('hostname')
                pid = processInfo.get('pid')

                if hostname != None and pid != None:
                    hostProcessId = HostProcessController.getHostProcessId(hostname, pid)

                    self._hostProcessInfos[hostProcessId] = HostProcessInfo(hostname, pid)

        # Check for host status changes:
        while self.isRunning:
            hostProcesses = self.getHostProcesses()

            if hostProcesses:
                for cachedProcessInfo in self._hostProcessInfos.values():
                    cachedProcessInfo.dirty = True

                for processInfo in hostProcesses:
                    try:
                        self.checkProcessInfo(processInfo)
                    except Exception as e:
                        self.logger.error(f'Host process check failed with exception: {str(e)}')

                removedHostProcessInfos = []
                for cachedProcessInfo in self._hostProcessInfos.values():
                    if cachedProcessInfo.dirty:
                        removedHostProcessInfos.append(cachedProcessInfo)

                for removedHostProcessInfo in removedHostProcessInfos:
                    self.removeHostProcessInfo(removedHostProcessInfo.hostname, removedHostProcessInfo.pid)

            time.sleep(1.0)

        self.thisHost.shutdown()
        self.threadPoolExecutor.shutdown(True)

        self.logger.info('Shut down.')

    def checkProcessInfo(self, processInfo):
        hostname = processInfo.get('hostname')
        pid = processInfo.get('pid')
        heartbeatTime = processInfo.get('heartbeat_time')

        if hostname != None and pid != None:
            hostProcessId = HostProcessController.getHostProcessId(hostname, pid)

            cachedProcessInfo = self._hostProcessInfos.get(hostProcessId)
            if cachedProcessInfo:
                cachedProcessInfo.dirty = False
            else:
                # New host process was added:
                self._hostProcessInfos[hostProcessId] = HostProcessInfo(hostname, pid)
                self._onHostProcessAddedEvent(hostname, pid)

            # Check for heartbeat time:
            if heartbeatTime and (datetime.utcnow() - heartbeatTime > timedelta(seconds=HostProcess.dyingTimeInSeconds)):
                HostProcess.delete(self.dbManager, hostname, pid)
                self.removeHostProcessInfo(hostname, pid)

    def getHostProcesses(self) -> dict:
        return self.dbManager.hostProcessesCollection.find({})

    def shutdown(self):
        self.logger.info('Shutting down...')
        self.isRunning = False

    def removeHostProcessInfo(self, hostname: str, pid: int):
        self._hostProcessInfos.pop(HostProcessController.getHostProcessId(hostname, pid))
        self.onHostProcessRemovedEvent(hostname, pid)