from MetadataManagerCore.actions.Action import Action
from MetadataManagerCore.actions.ActionType import ActionType

class DocumentAction(Action):
    def __init__(self):
        super().__init__()

    @property
    def id(self):
        return self.__class__.__name__
        
    def execute(self, document):
        pass

    @property
    def actionType(self):
        return ActionType.DocumentAction