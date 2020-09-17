from MetadataManagerCore.service.ServiceTargetRestriction import ServiceTargetRestriction
from abc import ABCMeta, abstractmethod
from enum import Enum
from MetadataManagerCore.Event import Event
import logging

class ServiceStatus(Enum):
    Created      = 'Created'
    Idle         = 'Idle' # Only services which have a fixed runtime (no "endless" while loop) change to this state.
    Starting     = 'Starting'
    Running      = 'Running'
    Disabling    = 'Disabling'
    Disabled     = 'Disabled'
    Failed       = 'Failed'
    ShuttingDown = 'ShuttingDown'
    Offline      = 'Offline'

logger = logging.getLogger(__name__)

class Service(object,metaclass=ABCMeta):
    """Services must have a default constructor. Don't forget to register the service class.
    """
    def __init__(self) -> None:
        super().__init__()
        
        self.name = None
        self.description = None
        self._status = ServiceStatus.Created
        self.active = None
        self.statusChangedEvent = Event()
        self.serviceRegistry = None

        # Hostname and pid will only be set if the service is allowed to run on only one host process
        self.hostname = None
        self.pid = None
    
    def run(self):
        self.status = ServiceStatus.Running
        self._run()

        if self.status == ServiceStatus.Disabling:
            self.status = ServiceStatus.Disabled
        elif self.status == ServiceStatus.ShuttingDown:
            self.status = ServiceStatus.Offline
        else:
            self.status = ServiceStatus.Idle

    def setActive(self, active: bool):
        self.active = active
        if self.active and self.status in [ServiceStatus.Disabled, ServiceStatus.Idle, ServiceStatus.Failed]:
            self.status = ServiceStatus.Starting
        elif not self.active and self.status in [ServiceStatus.Running, ServiceStatus.Idle, ServiceStatus.Failed]:
            if self.status == ServiceStatus.Running:
                self.status = ServiceStatus.Disabling
            else:
                self.status = ServiceStatus.Disabled

    @abstractmethod
    def _run(self):
        ...

    @classmethod
    @property
    def serviceTargetRestriction() -> ServiceTargetRestriction:
        return ServiceTargetRestriction.SingleHostProcess

    @property
    def supportsConsoleMode(self):
        return False

    def onStatusChanged(self, status):
        pass

    @property
    def status(self) -> ServiceStatus:
        return self._status

    @status.setter
    def status(self, status):
        prevStatus = self._status
        self._status = status

        if status != prevStatus:
            logger.info(f'Status changed of service {self.name} to {self.status}.')
            self.onStatusChanged(status)
            self.statusChangedEvent(status)

    def asDict(self):
        return {}

    def setupFromDict(self, theDict: dict):
        pass
    
    @property
    def statusAsString(self):
        return str(self.status.value)