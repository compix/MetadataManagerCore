from MetadataManagerCore.task_processor.TaskPicker import TaskPicker
from MetadataManagerCore.actions.ActionManager import ActionManager
from MetadataManagerCore.filtering.DocumentFilterManager import DocumentFilterManager
from MetadataManagerCore.task_processor.ActionTask import ActionTask
from MetadataManagerCore.task_processor.DocumentActionTask import DocumentActionTask

class ActionTaskPicker(TaskPicker):
    """
    Handles Action and DocumentAction task types.
    """
    def __init__(self, actionManager : ActionManager, documentFilterManager : DocumentFilterManager):
        super().__init__()

        self.actionManager = actionManager
        self.documentFilterManager = documentFilterManager

    def pickTask(self, taskType: str):
        """
        Returns an instance of a Task for the given taskType.
        """
        if taskType == 'Action':
            return ActionTask(self.actionManager)
        elif taskType == 'DocumentAction':
            return DocumentActionTask(self.actionManager, self.documentFilterManager)

        return None