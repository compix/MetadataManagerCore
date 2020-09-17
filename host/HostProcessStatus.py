from enum import Enum

class HostProcessStatus(Enum):
    Online = 'Online'
    Offline = 'Offline'
    Dead = 'Dead' # If the host didn't shutdown properly or is frozen.