from typing import Dict, List
from MetadataManagerCore.Event import Event
from MetadataManagerCore.service.Service import ServiceStatus
from concurrent.futures.thread import ThreadPoolExecutor
from MetadataManagerCore.mongodb_manager import MongoDBManager
import time

class ProcessInfoDictWrapper(object):
    def __init__(self, processInfoDict: dict) -> None:
        super().__init__()

        self.processInfoDict = processInfoDict
        self.dirty = False

class ServiceMonitor(object):
    def __init__(self, serviceName: str, dbManager: MongoDBManager, threadPoolExecutor: ThreadPoolExecutor) -> None:
        super().__init__()

        self.serviceName = serviceName
        self.dbManager = dbManager
        self.isRunning = True
        self.checkIntervalInSeconds = 1.0
        self.lastStatus = None
        self.lastServiceInfoDict = None

        self.lastServiceProcessInfos: Dict[str, ProcessInfoDictWrapper] = dict()

        self._onServiceProcessStatusChangedEvent = Event()
        self._onServiceProcessAddedEvent = Event()
        self._onServiceProcessRemovedEvent = Event()
        self._onServiceInfoDictChanged = Event()

        threadPoolExecutor.submit(self.run)

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
    def onServiceInfoDictChanged(self):
        """Event args: (prevInfoDict: dict, newInfoDict: dict)
        """
        return self._onServiceInfoDictChanged

    def findProcessInfos(self):
        return self.dbManager.serviceProcessCollection.find({'name': self.serviceName})
    
    def findServiceInfo(self):
        self.dbManager.serviceCollection.find_one({'_id': self.serviceName})
    
    def shutdown(self):
        self.isRunning = False

    def run(self):
        self.lastServiceInfoDict = self.findServiceInfo()
        serviceProcessInfoDicts = self.findProcessInfos()

        if serviceProcessInfoDicts:
            for processInfoDict in serviceProcessInfoDicts:
                id = processInfoDict.get('_id')
                self.lastServiceProcessInfos[id] = ProcessInfoDictWrapper(processInfoDict)

        while self.isRunning:
            time.sleep(self.checkIntervalInSeconds)

            curServiceInfoDict = self.findServiceInfo()

            if curServiceInfoDict != self.lastServiceInfoDict:
                self._onServiceInfoDictChanged(self.lastServiceInfoDict, curServiceInfoDict)
                self.lastServiceInfoDict = curServiceInfoDict

            # Check for service process changes:
            # Set last info dicts to dirty:
            for processInfo in self.lastServiceProcessInfos.values():
                processInfo.dirty = True

            curServiceProcessInfoDicts = self.findProcessInfos()
            addedServiceProcessInfoDicts: List[dict] = []
            if curServiceProcessInfoDicts:
                for curProcessInfoDict in curServiceProcessInfoDicts:
                    if self.lastServiceProcessInfos:
                        id = curProcessInfoDict.get('_id')
                        lastServiceProcessInfo = self.lastServiceProcessInfos.get(id)
                        if lastServiceProcessInfo:
                            lastServiceProcessInfo.dirty = False
                            lastStatus = lastServiceProcessInfo.processInfoDict.get('status')
                            curStatus = curProcessInfoDict.get('status')

                            if lastStatus != curStatus:
                                lastStatus = ServiceStatus(lastStatus) if lastStatus else None
                                curStatus = ServiceStatus(curStatus) if curStatus else None
                                self.onServiceProcessStatusChangedEvent(id, lastStatus, curStatus)
                        else:
                            addedServiceProcessInfoDicts.append(curProcessInfoDict)
                    else:
                        addedServiceProcessInfoDicts.append(curProcessInfoDict)

            removedServiceProcessIds: List[str] = []
            for processInfo in self.lastServiceProcessInfos.values():
                if processInfo.dirty:
                    removedServiceProcessIds.append(processInfo.get('_id'))

            for removedProcessId in removedServiceProcessIds:
                self.lastServiceProcessInfos.pop(removedProcessId)
                self.onServiceProcessRemovedEvent(removedProcessId)

            for addedProcessInfoDict in addedServiceProcessInfoDicts:
                id = addedProcessInfoDict.get('_id')
                self.lastServiceProcessInfos[id] = ProcessInfoDictWrapper(addedProcessInfoDict)
                self.onServiceProcessAddedEvent(id)



            




