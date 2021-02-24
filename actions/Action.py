from MetadataManagerCore.Event import Event
from MetadataManagerCore.actions.ActionType import ActionType
import math

class Action(object):
    def __init__(self) -> None:
        super().__init__()

        self.linkedCollections = []
        self.progressUpdateEvent = Event()
        self.currentProgress = 0

    def execute(self):
        pass

    def updateProgress(self, progress: float, progressMessage: str = None):
        """Updates the progress of this action.

        Args:
            progress (float): Expected value in [0,1].
        """
        self.currentProgress = max(min(progress, 1.0), 0.0)
        self.progressUpdateEvent(progress, progressMessage)
        
    @property
    def actionType(self):
        return ActionType.GeneralAction

    @property
    def id(self):
        return self.__class__.__name__

    @property
    def displayName(self):
        return self.id

    @property
    def filterTags(self):
        return []

    @property
    def category(self):
        return "Default"

    @property
    def runsOnMainThread(self):
        return False