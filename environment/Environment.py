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
        return {"settings": self.settingsDict, "display_name": self.displayName}

    def load(self, stateDict):
        if stateDict != None:
            self.settingsDict = stateDict.get('settings')
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

