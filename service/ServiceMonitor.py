from MetadataManagerCore.service.ServiceSerializationInfo import ServiceSerializationInfo
from typing import Dict, List
from MetadataManagerCore.Event import Event
from MetadataManagerCore.service.Service import ServiceStatus
from concurrent.futures.thread import ThreadPoolExecutor
from MetadataManagerCore.mongodb_manager import MongoDBManager
import time
from MetadataManagerCore.service.ServiceInfo import ServiceInfo

class ServiceProcessInfo(object):
    def __init__(self, processDict: dict):
        super().__init__()

        self.processDict = processDict if processDict else dict()
        self.dirty = False

    @property
    def status(self) -> ServiceStatus:
        return ServiceStatus(self.processDict.get('status'))

    @property
    def id(self) -> str:
        return self.processDict.get('_id')

    @property
    def serviceName(self) -> str:
        return self.processDict.get('name')

    @property
    def hostname(self) -> str:
        return self.processDict.get('hostname')

    @property
    def pid(self) -> int:
        return self.processDict.get('pid')

    def get(self, key: str):
        return self.processDict.get(key)

class ServiceMonitor(object):
    def __init__(self, serviceInfoDict: dict, dbManager: MongoDBManager, threadPoolExecutor: ThreadPoolExecutor) -> None:
        super().__init__()

        self.serviceName = serviceInfoDict.get('name')
        self.dbManager = dbManager
        self.isRunning = True
        self.checkIntervalInSeconds = 1.0
        self.lastStatus = None
        self.lastServiceInfo = ServiceInfo(serviceInfoDict)

        self.lastServiceProcessInfos: Dict[str, ServiceProcessInfo] = dict()

        self._onServiceProcessStatusChangedEvent = Event()
        self._onServiceProcessAddedEvent = Event()
        self._onServiceProcessRemovedEvent = Event()
        self._onServiceInfoChanged = Event()
        self._onServiceProcessChanged = Event()

        threadPoolExecutor.submit(self.run)

    @property
    def serviceDescription(self):
        return self.lastServiceInfo.description

    @property
    def serviceActive(self):
        return self.lastServiceInfo.active

    @property
    def serviceInfo(self):
        return self.lastServiceInfo

    @property
    def onServiceProcessStatusChangedEvent(self):
        """Event args: (serviceProcessId: str, prevStatus: ServiceStatus, newStatus: ServiceStatus)
        """
        return self._onServiceProcessStatusChangedEvent

    @property
    def onServiceProcessAddedEvent(self):
        """Event args: (serviceProcessId: str)
        """
        return self._onServiceProcessAddedEvent

    @property
    def onServiceProcessRemovedEvent(self):
        """Event args: (serviceProcessId: str)
        """
        return self._onServiceProcessRemovedEvent
        
    @property
    def onServiceInfoChanged(self):
        """Event args: (prevInfo: ServiceInfo, newInfo: ServiceInfo)
        """
        return self._onServiceInfoChanged

    @property
    def onServiceProcessChanged(self):
        """Event args: (processInfo: ServiceProcessInfo)
        """
        return self._onServiceProcessChanged

    def findProcessInfos(self):
        return self.dbManager.serviceProcessCollection.find({'name': self.serviceName})
    
    def findServiceInfo(self):
        return ServiceInfo(self.dbManager.serviceCollection.find_one({'_id': self.serviceName}))
    
    def shutdown(self):
        self.isRunning = False

    def run(self):
        self.lastServiceInfo = self.findServiceInfo()
        serviceProcessInfoDicts = self.findProcessInfos()

        if serviceProcessInfoDicts:
            for processInfoDict in serviceProcessInfoDicts:
                id = processInfoDict.get('_id')
                self.lastServiceProcessInfos[id] = ServiceProcessInfo(processInfoDict)

        while self.isRunning:
            time.sleep(self.checkIntervalInSeconds)

            curServiceInfo = self.findServiceInfo()

            if curServiceInfo.serviceDict != self.lastServiceInfo.serviceDict:
                lastServiceInfo = self.lastServiceInfo
                self.lastServiceInfo = curServiceInfo
                self._onServiceInfoChanged(lastServiceInfo, curServiceInfo)

            # Check for service process changes:
            # Set last info dicts to dirty:
            for processInfo in self.lastServiceProcessInfos.values():
                processInfo.dirty = True

            curServiceProcessInfoDicts = self.findProcessInfos()
            addedServiceProcessInfos: List[ServiceProcessInfo] = []
            if curServiceProcessInfoDicts:
                for curProcessInfoDict in curServiceProcessInfoDicts:
                    curProcessInfo = ServiceProcessInfo(curProcessInfoDict)
                    if self.lastServiceProcessInfos:
                        id = curProcessInfo.id
                        lastServiceProcessInfo = self.lastServiceProcessInfos.get(id)
                        if lastServiceProcessInfo:
                            lastServiceProcessInfo.dirty = False
                            lastStatus = lastServiceProcessInfo.status
                            curStatus = curProcessInfo.status

                            if lastStatus != curStatus:
                                lastStatus = ServiceStatus(lastStatus) if lastStatus else None
                                curStatus = ServiceStatus(curStatus) if curStatus else None
                                self.lastServiceProcessInfos[id] = curProcessInfo
                                self.onServiceProcessStatusChangedEvent(id, lastStatus, curStatus)
                                self._onServiceProcessChanged(curProcessInfo)
                        else:
                            addedServiceProcessInfos.append(curProcessInfo)
                    else:
                        addedServiceProcessInfos.append(curProcessInfo)

            removedServiceProcessIds: List[str] = []
            for processInfo in self.lastServiceProcessInfos.values():
                if processInfo.dirty:
                    removedServiceProcessIds.append(processInfo.id)

            for removedProcessId in removedServiceProcessIds:
                removedProcessInfo = self.lastServiceProcessInfos.pop(removedProcessId)
                self.onServiceProcessRemovedEvent(removedProcessId)

                self._onServiceProcessChanged(removedProcessInfo)

            for addedProcessInfo in addedServiceProcessInfos:
                id = addedProcessInfo.id
                self.lastServiceProcessInfos[id] = addedProcessInfo
                self.onServiceProcessAddedEvent(id)

                self._onServiceProcessChanged(addedProcessInfo)

    @property
    def serviceProcessInfos(self) -> List[ServiceProcessInfo]:
        return self.lastServiceProcessInfos.values()

    def getServiceHealthStatusString(self):
        if self.lastServiceProcessInfos:
            serviceProcessCount = len(self.lastServiceProcessInfos.keys())
            if serviceProcessCount > 0:
                runningProcessCount = sum(1 for p in self.lastServiceProcessInfos.values() if p.status == ServiceStatus.Running)

                if runningProcessCount == serviceProcessCount:
                    return 'Healthy'
                elif runningProcessCount == 0:
                    return 'Failing'
                else:
                    return f'{runningProcessCount}/{serviceProcessCount} running'

        return 'Not Running'