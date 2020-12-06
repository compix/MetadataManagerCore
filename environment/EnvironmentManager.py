from MetadataManagerCore.monitor.DatabaseSingleEntryChangeMonitor import DatabaseSingleEntryChangeMonitor
from MetadataManagerCore.environment.Environment import Environment
from MetadataManagerCore.mongodb_manager import MongoDBManager
from MetadataManagerCore import Keys
from typing import List
from MetadataManagerCore.Event import Event

class EnvironmentManagerChangeMonitor(DatabaseSingleEntryChangeMonitor):
    def __init__(self, dbManager: MongoDBManager, collection: str, id: str, envManager) -> None:
        super().__init__(dbManager, collection, id)

        self.envManager = envManager

    def getCurrentState(self) -> dict:
        return self.envManager.getCurrentState()

    def onStateChanged(self):
        self.envManager.updateState()
        self.envManager.onStateChanged()

class EnvironmentManager(object):
    def __init__(self):
        self.environments : List[Environment] = []

        self.dbManager = None
        self.changeMonitor = None
        self.onStateChanged = Event()

    def addEnvironment(self, env: Environment):
        self.environments.append(env)

    def getEnvironmentFromId(self, environmentId):
        for env in self.environments:
            if env.uniqueEnvironmentId == environmentId:
                return env

        return None

    def getEnvironmentNames(self):
        return [env.displayName for env in self.environments]

    def getEnvironmentFromName(self, environmentName):
        envId = self.getIdFromEnvironmentName(environmentName)
        return self.getEnvironmentFromId(envId)

    def hasEnvironmentId(self, environmentId):
        return self.getEnvironmentFromId(environmentId) != None

    def save(self, settings, dbManager):
        """
        Serializes the state in settings and/or in the database.

        input:
            - settings: Must support settings.setValue(key: str, value)
            - dbManager: MongoDBManager
        """
        pass

    def getCurrentState(self) -> dict:
        return {
            "environments": {env.uniqueEnvironmentId:env.getStateDict() for env in self.environments}
        }

    def saveToDatabase(self):
        if self.dbManager:
            self.dbManager.db[Keys.STATE_COLLECTION].replace_one({"_id": Keys.ENVIRONMENT_MANAGER_ID}, self.getCurrentState(), upsert=True)

    def load(self, settings, dbManager):
        """
        Loads the state from settings and/or the database.

        input:
            - settings: Must support settings.value(str)
            - dbManager: MongoDBManager
        """
        self.dbManager = dbManager
        self.loadFromDatabase()

        self.changeMonitor = EnvironmentManagerChangeMonitor(self.dbManager, Keys.STATE_COLLECTION, Keys.ENVIRONMENT_MANAGER_ID, self)
        self.changeMonitor.checkIntervalInSeconds = 5.0
        self.changeMonitor.runAsync()

    def loadFromDatabase(self):
        if self.dbManager:
            state = self.dbManager.db[Keys.STATE_COLLECTION].find_one({"_id": Keys.ENVIRONMENT_MANAGER_ID})
            if state != None:
                environmentsDict = state.get("environments")

                if environmentsDict != None:
                    for envId, envState in environmentsDict.items():
                        env = next((env for env in self.environments if env.uniqueEnvironmentId == envId), None)
                        if not env:
                            env = Environment(envId)
                            self.addEnvironment(env)

                        env.load(envState)

    def isValidEnvironmentId(self, id):
        return id != None and id != ""

    def getIdFromEnvironmentName(self, environmentName):
        return environmentName.replace(" ","").replace("\n","").replace("\t","").replace("\r","") if isinstance(environmentName, str) else None

    def archive(self, dbManager : MongoDBManager, environment : Environment):
        self.environments = [env for env in self.environments if env.uniqueEnvironmentId != environment.uniqueEnvironmentId]
        self.saveToDatabase()

        envState = environment.getStateDict()
        state = dbManager.db[Keys.STATE_COLLECTION].find_one({"_id": Keys.ARCHIVED_ENVIRONMENTS_ID})
        archivedEnvs = None
        if state != None:
            archivedEnvs = state.get("environments")

        if archivedEnvs == None:
            archivedEnvs = dict()      

        archivedEnvs[environment.uniqueEnvironmentId] = envState
        dbManager.db[Keys.STATE_COLLECTION].replace_one({"_id": Keys.ARCHIVED_ENVIRONMENTS_ID}, {"environments": archivedEnvs}, upsert=True)

    def updateState(self):
        self.loadFromDatabase()

    def checkForChanges(self):
        if self.changeMonitor:
            self.changeMonitor.checkForChanges()

    def shutdown(self):
        if self.changeMonitor:
            self.changeMonitor.stopAndJoin()