from MetadataManagerCore.service.ServiceMonitor import ServiceMonitor
import os
import socket
from MetadataManagerCore.service.ServiceTargetRestriction import ServiceTargetRestriction
from MetadataManagerCore.service.ServiceProcessController import ServiceProcessController
from MetadataManagerCore.host.HostProcessController import HostProcessController
from MetadataManagerCore.Event import Event
from typing import Any, List
from MetadataManagerCore.service.Service import Service, ServiceStatus
from MetadataManagerCore.mongodb_manager import MongoDBManager
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime
from MetadataManagerCore.service.ServiceSerializationInfo import ServiceSerializationInfo
from MetadataManagerCore.service.ServiceInfo import ServiceInfo
import time

logger = logging.getLogger(__name__)
        
class ServiceManager(object):
    def __init__(self, dbManager: MongoDBManager, hostProcessController: HostProcessController, serviceRegistry) -> None:
        super().__init__()

        dbManager.serviceProcessCollection.create_index("heartbeat_time", expireAfterSeconds=ServiceProcessController.dyingTimeInSeconds)

        self.isRunning = True
        self.dbManager = dbManager
        self.hostProcessController = hostProcessController
        self.serviceRegistry = serviceRegistry
        self.serviceClasses = set()
        self.serviceControllers: List[ServiceProcessController] = []
        self.serviceMonitors: List[ServiceMonitor] = []
        self.threadPoolExecutor = ThreadPoolExecutor()
        self.onServiceActiveStatusChanged = Event()
        self.onServiceProcessChanged = Event()
        self.onServiceAdded = Event()

    @staticmethod
    def createBaseServiceInfoDictionary(serviceClassName: str, serviceName: str, serviceDescription: str, initialStatus: ServiceStatus):
        return {
            'name': serviceName,
            'description': serviceDescription,
            'active': initialStatus == ServiceStatus.Running,
            'className': serviceClassName,
            'serviceInfoDict': None
        }

    @staticmethod
    def setServiceInfoDict(baseInfoDict: dict, serviceInfoDict: dict):
        baseInfoDict['serviceInfoDict'] = serviceInfoDict

    def registerServiceClass(self, serviceClass):
        self.serviceClasses.add(serviceClass)

    def monitorServices(self):
        class MonitorInfo(object):
            def __init__(self, serviceDict):
                super().__init__()

                self.serviceDict = serviceDict
                self.dirty = False

            @property
            def id(self):
                return self.serviceDict.get('_id')

        def findMonitorInfos():
            lastServiceDicts = self.dbManager.serviceCollection.find({})
            return {infoDict.get('_id'):MonitorInfo(infoDict) for infoDict in lastServiceDicts} if lastServiceDicts else dict()

        lastMonitorInfos = findMonitorInfos()

        while self.isRunning:
            time.sleep(1.0)
            
            try:
                for i in lastMonitorInfos.values():
                    i.dirty = True

                monitorInfos = findMonitorInfos()

                for info in monitorInfos.values():
                    lastInfo = lastMonitorInfos.get(info.id)
                    if not lastInfo:
                        # New service was added:
                        lastMonitorInfos[info.id] = info
                        self.addServiceFromDict(info.serviceDict)
                    else:
                        lastInfo.dirty = False
                
                removedMonitorInfos = [i for i in lastMonitorInfos.values() if i.dirty]

                for removedInfo in removedMonitorInfos:
                    lastMonitorInfos.pop(removedInfo.id)
                    # TODO: Remove the service (controller + monitor).
                    pass
            except Exception as e:
                logger.error(f'Failed service monitoring with exception: {str(e)}')
            
    def shutdown(self):
        self.isRunning = False

        for serviceController in self.serviceControllers:
            serviceController.shutdown()

        for serviceMonitor in self.serviceMonitors:
            serviceMonitor.shutdown()

        logger.info('Waiting for shutdown completion...')
        self.threadPoolExecutor.shutdown(wait=True)
        logger.info('All services were shut down.')
        
    def save(self, settings, dbManager: MongoDBManager):
        """
        Serializes the state in settings and/or in the database.

        input:
            - settings: Must support settings.setValue(key: str, value)
            - dbManager: MongoDBManager
        """
        pass

    def load(self, settings, dbManager: MongoDBManager):
        """
        Loads the state from settings and/or the database.

        input:
            - settings: Must support settings.value(str)
            - dbManager: MongoDBManager
        """

        serviceInfos = dbManager.serviceCollection.find({})
        if serviceInfos:
            for serviceInfoDict in serviceInfos:
                self.addServiceFromDict(serviceInfoDict)

        self.threadPoolExecutor.submit(self.monitorServices)

    def getServiceClassFromClassName(self, className: str):
        returnServiceClass = None
        for serviceClass in self.serviceClasses:
            if serviceClass.__name__ == className:
                returnServiceClass = serviceClass
                break

        return returnServiceClass

    @staticmethod
    def getServiceProcessId(serviceInfo: ServiceSerializationInfo, serviceClass: Any):
        serviceTargetRestriction: ServiceTargetRestriction = serviceClass.getServiceTargetRestriction()

        hostname = socket.gethostname()
        pid = os.getpid()

        if serviceTargetRestriction == ServiceTargetRestriction.Unrestricted:
            serviceProcessId = f'{serviceInfo.name}_{hostname}_{pid}'
        elif serviceTargetRestriction == ServiceTargetRestriction.SingleHost:
            serviceProcessId = f'{serviceInfo.name}_{hostname}'
        elif serviceTargetRestriction == ServiceTargetRestriction.SingleHostProcess:
            serviceProcessId = serviceInfo.name
        else:
            logger.error(f'Unhandled ServiceTargetRestriction: {serviceTargetRestriction}')
            return None

        return serviceProcessId

    def insertServiceStatus(self, serviceInfo: ServiceSerializationInfo, serviceClass: Any):
        """Returns the service process id on success. If this operation fails because the service process with this id is already running None is returned.
        """
        hostname = socket.gethostname()
        pid = os.getpid()

        serviceProcessId = ServiceManager.getServiceProcessId(serviceInfo, serviceClass)

        try:
            self.dbManager.serviceProcessCollection.insert_one({
                '_id': serviceProcessId,
                'name': serviceInfo.name,
                'status': str(ServiceStatus.Created.value), 
                'heartbeat_time': datetime.utcnow(),
                'hostname': hostname,
                'pid': pid
                })
        except:
            return None

        return serviceProcessId

    def setServiceActive(self, serviceName: str, active: bool):
        self.updateServiceValue(serviceName, 'active', active)

    def updateServiceValue(self, serviceName: str, key: str, value: Any):
        self.dbManager.serviceCollection.update_one({'_id': serviceName}, {'$set': {key: value}}, upsert=True)

    def addServiceFromDict(self, serviceInfoDict: dict, newService = False):
        """Request to add a service from dict. This operation might actually not create a service.
        Args:
            serviceInfoDict (dict): Dictionary with information describing the service.
        """
        serInfo = ServiceSerializationInfo(self.serviceRegistry)
        serInfo.setupFromDict(serviceInfoDict)
        serviceClass = self.getServiceClassFromClassName(serInfo.className)

        if serviceClass:
            # First check if the service is active:
            if serInfo.active:
                # Try to create a service status and insert in DB. If this operation fails the service is locked to host/host process.
                serviceProcessId = self.insertServiceStatus(serInfo, serviceClass)
                if serviceProcessId != None:
                    serviceController = ServiceProcessController(self.dbManager, self.serviceRegistry, serviceProcessId, serInfo, serviceClass, self.threadPoolExecutor)
                    self.serviceControllers.append(serviceController)
        else:
            logger.error(f'Could not find service class for service with class name {serInfo.className}.')

        # Add ServiceMonitor
        serviceProcessId = ServiceManager.getServiceProcessId(serInfo, serviceClass)
        serviceMonitor = ServiceMonitor(serviceInfoDict, self.dbManager, self.threadPoolExecutor)
        self.serviceMonitors.append(serviceMonitor)

        self.onServiceAdded(ServiceInfo(serviceInfoDict))

        serviceMonitor.onServiceInfoChanged.subscribe(self.onServiceInfoChanged)
        serviceMonitor.onServiceProcessChanged.subscribe(self.onServiceProcessChanged)

    def onServiceInfoChanged(self, prevInfo: ServiceInfo, newInfo: ServiceInfo):
        if prevInfo.active != newInfo.active:
            self.onServiceActiveStatusChanged(newInfo)