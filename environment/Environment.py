from MetadataManagerCore import Keys
import json
import re

class Environment(object):
    """
    An environment encapsulates user defined settings, data and processes.
    """
    def __init__(self, uniqueEnvironmentId):
        self.uniqueEnvironmentId = uniqueEnvironmentId
        self.settingsDict = dict()
        self.displayName = uniqueEnvironmentId

    def setDisplayName(self, displayName):
        self.displayName = displayName

    def getStateDict(self):
        #settingsAsJson = json.dumps(self.settingsDict)
        return {"settings": self.settingsDict, "display_name": self.displayName}

    #def save(self, dbManager):
        #dbManager.db[Keys.STATE_COLLECTION].replace_one({"_id": self.uniqueEnvironmentId}, {"settings": settingsAsJson, "display_name": self.displayName}, upsert=True)

    def load(self, stateDict):
        #envState = dbManager.db[Keys.STATE_COLLECTION].find_one({"_id": self.uniqueEnvironmentId})

        if stateDict != None:
            self.settingsDict = stateDict.get('settings')
           # settingsAsJson = stateDict.get('settings')
            #if settingsAsJson != None:
                #self.settingsDict = json.loads(settingsAsJson)

            self.setDisplayName(stateDict.get("display_name"))

    def evaluateSettingsValue(self, value):
        p = re.compile(r'\${(.*)}')
        match = p.match(value)
        if match:
            key = match.group(1)
            evaluatedValue = self.settingsDict.get(key)
            if evaluatedValue == None:
                raise f"Failed evaluating {value}. Unknown token found: {key}"

            evaluatedValue = value.replace(match.group(), evaluatedValue)
            return self.evaluateSettingsValue(evaluatedValue)
        else:
            return value

    def getEvaluatedSettings(self):
        """
        Evaluates special tokens, e.g. ${KEY}, and returns the evaluated settings as dictionary.
        """
        evaluatedSettingsDict = dict()
        for key, val in self.settingsDict.items():
            evaluatedValue = self.evaluateSettingsValue(val)
            evaluatedSettingsDict[key] = evaluatedValue

        return evaluatedSettingsDict

    @property
    def settings(self):
        """
        Returns the settings dictionary.
        """
        return self.settingsDict

