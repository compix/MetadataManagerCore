from enum import Enum

class HostStatus(Enum):
    Online = 'Online'
    Offline = 'Offline'
    Dead = 'Dead' # If the host didn't shutdown properly or is frozen.