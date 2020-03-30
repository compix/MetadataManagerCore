from MetadataManagerCore.mongodb_manager import MongoDBManager
from MetadataManagerCore.actions.ActionManager import ActionManager
from MetadataManagerCore.environment.EnvironmentManager import EnvironmentManager
from MetadataManagerCore import Keys
import json

class StateManager(object):
    def __init__(self, dbManager: MongoDBManager):
        self.dbManager = dbManager
        self.actionManager = ActionManager()
        self.environmentManager = EnvironmentManager()

    def saveState(self):
        self.actionManager.save(self.dbManager)
        self.environmentManager.save(self.dbManager)

    def loadState(self):
        self.actionManager.load(self.dbManager)
        self.environmentManager.load(self.dbManager)