from MetadataManagerCore.Event import Event
from typing import List
from MetadataManagerCore.service.Service import Service, ServiceStatus
from MetadataManagerCore.mongodb_manager import MongoDBManager
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

class ServiceSerializationInfo(object):
    def __init__(self, serviceRegistry) -> None:
        super().__init__()

        self.serviceRegistry = serviceRegistry
        self.name = None
        self.description = None
        self.status = None
        self.active = None
        self.className = None
        self.module = None
        self.serviceInfoDict = None

    def setupFromService(self, service: Service):
        self.name = service.name
        self.description = service.description
        self.status = service.status
        self.active = service.active
        self.className = type(service).__name__
        self.serviceInfoDict = service.asDict()

    def setupFromDict(self, theDict: dict):
        self.name = theDict['name']
        self.description = theDict['description']
        self.status = ServiceStatus(theDict['status'])
        self.active = theDict['active']
        self.className = theDict['className']
        self.serviceInfoDict = theDict['serviceInfoDict']

    def asDict(self):
        return {
            'name': self.name,
            'description': self.description,
            'status': str(self.status.value),
            'active': self.active,
            'className': self.className,
            'serviceInfoDict': self.serviceInfoDict
        }

    def constructService(self, serviceClass) -> Service:
        service : Service = serviceClass()
        service.serviceRegistry = self.serviceRegistry
        service.name = self.name
        service.description = self.description
        service._status = self.status
        service.active = self.active
        successful = True
        try:
            service.setupFromDict(self.serviceInfoDict if self.serviceInfoDict != None else dict())
        except Exception as e:
            logger.error(f'Info creation of service {service.name} failed: {str(e)}')
            successful = False

        return service, successful
        
class ServiceManager(object):
    def __init__(self, dbManager: MongoDBManager, serviceRegistry) -> None:
        super().__init__()

        self.dbManager = dbManager
        self.serviceRegistry = serviceRegistry
        self.serviceClasses = set()
        self.services :List[Service] = []
        self.threadPoolExecutor = ThreadPoolExecutor(max_workers=10)
        self.serviceStatusChangedEvent = Event()

    @staticmethod
    def createBaseServiceInfoDictionary(serviceClassName: str, serviceName: str, serviceDescription: str, initialStatus: ServiceStatus):
        return {
            'name': serviceName,
            'description': serviceDescription,
            'status': ServiceStatus.Created,
            'active': initialStatus == ServiceStatus.Running,
            'className': serviceClassName,
            'serviceInfoDict': None
        }

    @staticmethod
    def setServiceInfoDict(baseInfoDict: dict, serviceInfoDict: dict):
        baseInfoDict['serviceInfoDict'] = serviceInfoDict

    def registerServiceClass(self, serviceClass):
        self.serviceClasses.add(serviceClass)

    def onServiceStatusChanged(self, service: Service, serviceStatus: ServiceStatus):
        self.serviceStatusChangedEvent(service, serviceStatus)
        if serviceStatus == ServiceStatus.Starting:
            self.saveService(service)
            self.threadPoolExecutor.submit(self.runService, service)
        elif serviceStatus == ServiceStatus.Disabled:
            self.saveService(service)
        else:
            self.saveServiceStatus(service)

    def addService(self, service: Service, initialStatus = ServiceStatus.Running):
        if not type(service) in self.serviceClasses:
            logger.error(f'Unknown service class: {type(service)}')
            return

        self.services.append(service)
        service.statusChangedEvent.subscribe(lambda serviceStatus: self.onServiceStatusChanged(service, serviceStatus))

        if initialStatus == ServiceStatus.Running:
            service.active = True
            # The status change will trigger an event that will run the service
            service.status = ServiceStatus.Starting
        else:
            service.status = initialStatus

            if initialStatus == ServiceStatus.Disabled:
                service.active = False

            if initialStatus in [ServiceStatus.ShuttingDown]:
                logger.error(f'Invalid initial status: {initialStatus}')

            self.saveService(service)

    def runService(self, service: Service):
        try:
            service.run()            
        except Exception as e:
            logger.error(f'Service {service.name} failed with exception: {str(e)}')
            service.status = ServiceStatus.Failed

    def saveService(self, service: Service):
        serInfo = ServiceSerializationInfo(self.serviceRegistry)
        serInfo.setupFromService(service)

        self.dbManager.stateCollection.update_one({'_id': 'service_manager'}, [{'$set': {'services': {service.name: serInfo.asDict()}}}], upsert=True)

    def saveServiceStatus(self, service: Service):
        self.dbManager.stateCollection.update_one({'_id': 'service_manager'}, [{'$set': {'services': {service.name: {'status': service.statusAsString}}}}], upsert=True)

    def removeService(self, service: Service):
        self.services.remove(service)

    def removeServiceByName(self, serviceName: str):
        self.services = [service for service in self.services if not service.name == serviceName]

    def shutdown(self):
        for service in self.services:
            logger.info(f'Shutting down service {service.name} ...')
            service.status = ServiceStatus.ShuttingDown

        logger.info('Waiting for shutdown completion...')
        self.threadPoolExecutor.shutdown(wait=True)
        logger.info('All services were shut down.')

        for service in self.services:
            service._status = ServiceStatus.Offline
            self.saveServiceStatus(service)
        
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
        state = dbManager.stateCollection.find_one({'_id': "service_manager"})

        if state:
            for serviceInfoDict in state['services'].values():
                self.addServiceFromDict(serviceInfoDict)

    def getServiceClassFromClassName(self, className: str):
        returnServiceClass = None
        for serviceClass in self.serviceClasses:
            if serviceClass.__name__ == className:
                returnServiceClass = serviceClass
                break

        return returnServiceClass

    def addServiceFromDict(self, serviceInfoDict: dict):
        serInfo = ServiceSerializationInfo(self.serviceRegistry)
        serInfo.setupFromDict(serviceInfoDict)
        serviceClass = self.getServiceClassFromClassName(serInfo.className)
        if serviceClass:
            service, successful = serInfo.constructService(serviceClass)

            if not successful:
                self.addService(service, initialStatus=ServiceStatus.Failed)
                return

            # Check service status correctness. It must be either Offline or Disabled eitherwise the application wasn't shut down correctly.
            if not service.status in [ServiceStatus.Offline, ServiceStatus.Disabled]:
                service._status = ServiceStatus.Created

            initialStatus = ServiceStatus.Running if service.active else ServiceStatus.Disabled
            self.addService(service, initialStatus=initialStatus)
        else:
            logger.error(f'Could not find service class for service with class name {serInfo.className}.')