import logging
from MetadataManagerCore.service.Service import Service

logger = logging.getLogger(__name__)

class ServiceSerializationInfo(object):
    def __init__(self, serviceRegistry) -> None:
        super().__init__()

        self.serviceRegistry = serviceRegistry
        self.name = None
        self.description = None
        self.active = None
        self.className = None
        self.module = None
        self.serviceInfoDict = None

    def setupFromService(self, service: Service):
        self.name = service.name
        self.description = service.description
        self.active = service.active
        self.className = type(service).__name__
        self.serviceInfoDict = service.asDict()

    def setupFromDict(self, theDict: dict):
        self.name = theDict['name']
        self.description = theDict['description']
        self.active = theDict['active']
        self.className = theDict['className']
        self.serviceInfoDict = theDict['serviceInfoDict']

    def asDict(self):
        return {
            'name': self.name,
            'description': self.description,
            'active': self.active,
            'className': self.className,
            'serviceInfoDict': self.serviceInfoDict
        }

    def constructService(self, serviceClass) -> Service:
        service : Service = serviceClass()
        service.serviceRegistry = self.serviceRegistry
        service.name = self.name
        service.description = self.description
        service.active = self.active
        successful = True
        try:
            service.setupFromDict(self.serviceInfoDict if self.serviceInfoDict != None else dict())
        except Exception as e:
            logger.error(f'Info creation of service {service.name} failed: {str(e)}')
            successful = False

        return service, successful