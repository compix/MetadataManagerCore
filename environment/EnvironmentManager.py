import typing
from MetadataManagerCore.environment.Environment import Environment
from MetadataManagerCore.mongodb_manager import MongoDBManager
from MetadataManagerCore import Keys
from MetadataManagerCore.Event import Event
import logging

logger = logging.getLogger(__name__)

class EnvironmentManager(object):
    def __init__(self):
        self.dbManager = None
        self.onStateChanged = Event()
        self.changeConsumer = None

    def getEnvironmentFromId(self, envId: str):
        if not self.dbManager:
            return None

        envDict = self.dbManager.db[Keys.ENVIRONMENT_COLLECTION].find_one({"_id": envId})
        if envDict:
            env = Environment(envId)
            env.load(envDict)
            return env

        return None

    def getEnvironmentNames(self):
        if not self.dbManager:
            return []

        names = []
        with self.dbManager.db[Keys.ENVIRONMENT_COLLECTION].find({}, {"display_name":1}, no_cursor_timeout=True) as cursor:
            for doc in cursor:
                names.append(doc.get('display_name'))

        return names

    def getEnvironmentFromName(self, environmentName: str):
        envId = EnvironmentManager.getIdFromEnvironmentName(environmentName)
        return self.getEnvironmentFromId(envId)

    def hasEnvironmentId(self, envId: str):
        if not self.dbManager:
            return False

        return self.dbManager.db[Keys.ENVIRONMENT_COLLECTION].count_documents({"_id": envId}) > 0

    def save(self, settings, dbManager):
        """
        Serializes the state in settings and/or in the database.

        input:
            - settings: Must support settings.setValue(key: str, value)
            - dbManager: MongoDBManager
        """
        pass

    def delete(self, envId: str):
        self.dbManager.db[Keys.ENVIRONMENT_COLLECTION].delete_one({"_id": envId})

    def _migrate_archived(self):
        if not self.dbManager:
            return

        prevState = self.dbManager.db[Keys.STATE_COLLECTION].find_one({"_id": Keys.ARCHIVED_ENVIRONMENTS_ID})
        if prevState == None:
            return

        environmentsDict = prevState.get("environments")

        if environmentsDict != None:
            for envId, envState in environmentsDict.items():
                self.dbManager.db[Keys.ARCHIVED_ENV_COLLECTION].replace_one({"_id": envId}, envState, upsert=True)

        self.dbManager.db[Keys.STATE_COLLECTION].delete_one({"_id": Keys.ARCHIVED_ENVIRONMENTS_ID})

    def _migrate(self):
        if not self.dbManager:
            return

        prevState = self.dbManager.db[Keys.STATE_COLLECTION].find_one({"_id": Keys.ENVIRONMENT_MANAGER_ID})
        if prevState == None:
            return

        environmentsDict = prevState.get("environments")

        if environmentsDict != None:
            for envId, envState in environmentsDict.items():
                self.dbManager.db[Keys.ENVIRONMENT_COLLECTION].replace_one({"_id": envId}, envState, upsert=True)

        self.dbManager.db[Keys.STATE_COLLECTION].delete_one({"_id": Keys.ENVIRONMENT_MANAGER_ID})

    def migrate(self):
        self._migrate()
        self._migrate_archived()

    def upsert(self, env: Environment):
        if not self.dbManager:
            return

        self.dbManager.db[Keys.ENVIRONMENT_COLLECTION].replace_one({"_id": env.uniqueEnvironmentId}, env.getStateDict(), upsert=True)

    def load(self, settings, dbManager):
        """
        Loads the state from settings and/or the database.

        input:
            - settings: Must support settings.value(str)
            - dbManager: MongoDBManager
        """
        self.dbManager = dbManager

    def isValidEnvironmentId(self, id):
        return id != None and id != ""

    @staticmethod
    def getIdFromEnvironmentName(environmentName: str):
        return environmentName.replace(" ","").replace("\n","").replace("\t","").replace("\r","") if isinstance(environmentName, str) else None

    def getEnvironments(self) -> typing.List[Environment]:
        return [env for env in self.yieldEnvironments()]

    def yieldEnvironments(self) -> typing.Iterator[Environment]:
        if self.dbManager:
            with self.dbManager.db[Keys.ENVIRONMENT_COLLECTION].find({}) as cursor:
                for envDict in cursor:
                    env = Environment(envDict.get('_id'))
                    env.load(envDict)
                    yield env

    def archive(self, dbManager : MongoDBManager, envId: str):
        env = self.getEnvironmentFromId(envId)
        if not env:
            return
        
        envState = env.getStateDict()
        dbManager.db[Keys.ARCHIVED_ENV_COLLECTION].replace_one({"_id": envId}, envState, upsert=True)

        self.delete(envId)

    def shutdown(self):
        pass