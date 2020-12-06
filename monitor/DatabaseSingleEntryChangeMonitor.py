from abc import abstractmethod
from typing import Callable
from MetadataManagerCore.monitor.ChangeMonitor import ChangeMonitor
from MetadataManagerCore.mongodb_manager import MongoDBManager

class DatabaseSingleEntryChangeMonitor(ChangeMonitor):
    def __init__(self, dbManager: MongoDBManager, collection: str, id: str) -> None:
        super().__init__()

        self.dbManager = dbManager
        self.collection = collection
        self.id = id

    def checkForChanges(self):
        curState = self.getCurrentState()
        state = self.dbManager.db[self.collection].find_one({'_id': self.id})
        del state['_id']

        if curState != state:
            self.onStateChanged()

    @abstractmethod
    def getCurrentState(self) -> dict:
        pass