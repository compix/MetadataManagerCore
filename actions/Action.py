from MetadataManagerCore.actions.ActionType import ActionType

class Action(object):
    def execute(self):
        pass

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