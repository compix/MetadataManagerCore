from MetadataManagerCore import Keys
import json
import re
from enum import Enum

class EnvironmentTarget(Enum):
    Production = 'production'
    Preview = 'preview'

class Environment(object):
    """
    An environment encapsulates user defined settings, data and processes.
    """
    def __init__(self, uniqueEnvironmentId):
        self.uniqueEnvironmentId = uniqueEnvironmentId
        self.settingsDict = dict()
        self.displayName = uniqueEnvironmentId
        self.autoExportPath = ''

    def setDisplayName(self, displayName):
        self.displayName = displayName

    def getStateDict(self):
        return {"settings": self.settingsDict, "display_name": self.displayName, "auto_export_path": self.autoExportPath}

    def load(self, stateDict):
        if stateDict != None:
            self.settingsDict = stateDict.get('settings')
            self.setDisplayName(stateDict.get("display_name"))
            self.autoExportPath = stateDict.get('auto_export_path')

    def evaluateSettingsValue(self, value, depth=0):
        if depth == 1000:
            raise RuntimeError(f'Max recursion depth reached in settings value evaluation of {value}')

        if not isinstance(value, str):
            return value
            
        p = re.compile(r'\${(.*?)}')
        match = p.match(value)
        if match:
            key = match.group(1)
            evaluatedValue = self.settingsDict.get(key)
            if evaluatedValue == None:
                raise RuntimeError(f"Failed evaluating {value}. Unknown token found: {key}")

            evaluatedValue = value.replace(match.group(), evaluatedValue)
            return self.evaluateSettingsValue(evaluatedValue, depth=depth + 1)
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

    @property
    def target(self) -> EnvironmentTarget:
        settingsDict = self.getEvaluatedSettings()
        curTarget = settingsDict.get('target')
        if curTarget:
            try:
                return EnvironmentTarget(curTarget)
            except:
                return EnvironmentTarget.Preview
        
        return None
