from MetadataManagerCore.Event import Event
from MetadataManagerCore.mongodb_manager import MongoDBManager
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any
from MetadataManagerCore.service.Service import Service, ServiceStatus
import logging
from MetadataManagerCore.service.ServiceSerializationInfo import ServiceSerializationInfo
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class ServiceProcessController(object):
    dyingTimeInSeconds = 5.0

    def __init__(self, dbManager: MongoDBManager, serviceRegistry, serviceProcessId: str, serviceSerializationInfo: ServiceSerializationInfo, serviceClass: Any, threadPoolExecutor: ThreadPoolExecutor) -> None:
        super().__init__()

        self.dbManager = dbManager
        self.serviceRegistry = serviceRegistry
        self.serviceProcessId = serviceProcessId
        self.isRunning = True
        self.heartbeatUpdateIntervalInSeconds = 1.0
        self._onServiceProcessFailedEvent = Event()
        self.threadPoolExecutor = threadPoolExecutor
        self.serviceClass = serviceClass
        self.service, self.successfulServiceCreation = serviceSerializationInfo.constructService(self.serviceClass)
        self.statusSerializationFailed = False
        self.saveService()

        self.service.statusChangedEvent.subscribe(self.onServiceProcessStatusChanged)

    def submitServiceRunner(self):
        self.threadPoolExecutor.submit(self.runHeartbeatUpdate)

        if self.successfulServiceCreation:
            self.threadPoolExecutor.submit(self.runService)
        else:
            self.service.status = ServiceStatus.Failed
            self.updateServiceProcessValue('status', str(ServiceStatus.Failed.value))

    def updateServiceProcessValue(self, key: str, value: Any):
        self.dbManager.serviceProcessCollection.update_one({'_id': self.serviceProcessId}, {'$set': {key: value}}, upsert=False)

    def updateServiceValue(self, key: str, value: Any):
        self.dbManager.serviceCollection.update_one({'_id': self.service.name}, {'$set': {key: value}}, upsert=True)

    def shutdown(self):
        logger.info(f'Shutting down service {self.service.name} ...')
        self.isRunning = False
        self.service.status = ServiceStatus.ShuttingDown
    
    def onServiceProcessStatusChanged(self, serviceStatus: ServiceStatus):
        if self.statusSerializationFailed:
            return

        try:
            self.updateServiceProcessValue('status', str(serviceStatus.value))
        except Exception as e:
            logger.error(f'Status update failed with exception: {str(e)}')
            self.statusSerializationFailed = True
            self.service.status = ServiceStatus.Failed
            self.isRunning = False
            return

        if serviceStatus == ServiceStatus.Starting:
            self.saveService()
        elif serviceStatus == ServiceStatus.Disabled:
            self.updateServiceValue('active', False)
        elif serviceStatus == ServiceStatus.Failed:
            self.isRunning = False

    def runService(self):
        try:
            self.service.run()            
        except Exception as e:
            logger.error(f'Service {self.service.name} failed with exception: {str(e)}')
            self.service.status = ServiceStatus.Failed

    def runHeartbeatUpdate(self):
        while self.isRunning:
            try:
                self.updateServiceProcessValue('heartbeat_time', datetime.utcnow())
            except Exception as e:
                logger.error(f'Heartbeat update failed with exception: {str(e)}')
                self.service.status = ServiceStatus.Failed
                self.isRunning = False
                break

            time.sleep(self.heartbeatUpdateIntervalInSeconds)

        # Remove service process
        try:
            self.dbManager.serviceProcessCollection.delete_one({'_id': self.serviceProcessId})
        except Exception as e:
            logger.error(f'Deleting service process with id {self.serviceProcessId} failed with exception: {str(e)}')

    def saveService(self):
        serInfo = ServiceSerializationInfo(self.serviceRegistry)
        serInfo.setupFromService(self.service)

        self.dbManager.serviceCollection.replace_one({'_id': serInfo.name}, serInfo.asDict(), upsert=True)