from MetadataManagerCore.task_processor.Task import Task
from MetadataManagerCore.actions.ActionManager import ActionManager
import logging

logger = logging.getLogger(__name__)

class ActionTask(Task):
    def __init__(self, actionManager : ActionManager):
        super().__init__()

        self.actionManager = actionManager

    def execute(self, dataDict: dict):
        actionId = dataDict.get('actionId')
        if actionId:
            action = self.actionManager.getActionById(actionId)
            if action:
                action.execute()
            else:
                raise RuntimeError(f'Unknown actionId: {actionId}')
        else:
            raise RuntimeError(f"The data dictionary is missing an actionId entry.")