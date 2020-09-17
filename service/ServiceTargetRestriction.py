from enum import Enum

class ServiceTargetRestriction(Enum):
    Unrestricted = 'Unrestricted'
    SingleHost = 'SingleHost'
    SingleHostProcess = 'SingleHostProcess'