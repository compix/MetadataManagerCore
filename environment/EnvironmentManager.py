from MetadataManagerCore.environment.Environment import Environment
from MetadataManagerCore.mongodb_manager import MongoDBManager
from MetadataManagerCore import Keys
import json
from typing import List

class EnvironmentManager(object):
    def __init__(self):
        self.environments : List[Environment] = []
        self.archived_environments : List[Environment] = []
        self.stateDict = dict()

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
        environmentsDict = dict()
        for env in self.environments:
            envState = env.getStateDict()
            environmentsDict[env.uniqueEnvironmentId] = envState

        dbManager.db[Keys.STATE_COLLECTION].replace_one({"_id": Keys.ENVIRONMENT_MANAGER_ID}, {"environments": environmentsDict}, upsert=True)

    def load(self, settings, dbManager):
        """
        Loads the state from settings and/or the database.

        input:
            - settings: Must support settings.value(str)
            - dbManager: MongoDBManager
        """
        state = dbManager.db[Keys.STATE_COLLECTION].find_one({"_id": Keys.ENVIRONMENT_MANAGER_ID})

        if state != None:
            environmentsDict = state.get("environments")

            if environmentsDict != None:
                for envId, envState in environmentsDict.items():
                    env = Environment(envId)
                    env.load(envState)
                    self.addEnvironment(env)

    def isValidEnvironmentId(self, id):
        return id != None and id != ""

    def getIdFromEnvironmentName(self, environmentName):
        return environmentName.replace(" ","").replace("\n","").replace("\t","").replace("\r","") if isinstance(environmentName, str) else None

    def archive(self, dbManager : MongoDBManager, environment : Environment):
        self.environments = [env for env in self.environments if env.uniqueEnvironmentId != environment.uniqueEnvironmentId]
        self.save(dbManager)

        envState = environment.getStateDict()
        state = dbManager.db[Keys.STATE_COLLECTION].find_one({"_id": Keys.ARCHIVED_ENVIRONMENTS_ID})
        archivedEnvs = None
        if state != None:
            archivedEnvs = state.get("environments")

        if archivedEnvs == None:
            archivedEnvs = dict()      

        archivedEnvs[environment.uniqueEnvironmentId] = envState
        dbManager.db[Keys.STATE_COLLECTION].replace_one({"_id": Keys.ARCHIVED_ENVIRONMENTS_ID}, {"environments": archivedEnvs}, upsert=True)