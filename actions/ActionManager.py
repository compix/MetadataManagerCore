from MetadataManagerCore.actions.DocumentAction import DocumentAction
from MetadataManagerCore.actions.Action import Action
from MetadataManagerCore.actions.ActionType import ActionType
from MetadataManagerCore.Event import Event
from MetadataManagerCore import Keys
import json

class ActionManager(object):
    def __init__(self):
        self.actions = []
        self.collectionToActionsMap = dict()

        self.m_linkActionToCollectionEvent = Event()
        self.m_unlinkActionFromCollectionEvent = Event()
        self.m_registerActionEvent = Event()

    @property
    def registerActionEvent(self) -> Event:
        """
        Expected argument: action
        """
        return self.m_registerActionEvent
        
    @property
    def linkActionToCollectionEvent(self) -> Event:
        """
        Expected arguments: actionId, collectionName
        """
        return self.m_linkActionToCollectionEvent

    @property
    def unlinkActionFromCollectionEvent(self) -> Event:
        """
        Expected arguments: actionId, collectionName
        """
        return self.m_unlinkActionFromCollectionEvent

    def getAllCategories(self):
        categories = []
        for a in self.actions:
            if not a.category in categories:
                categories.append(a.category)

        return categories

    def getActionIdsOfCategory(self, category):
        return [a.id for a in self.actions if a.category == category]
    
    def registerAction(self, action):
        for a in self.actions:
            if a.id == action.id:
                print(f"Warning: Action {action.id} is already registered.")
                return
                
        self.actions.append(action)

        for collectionName, actionIds in self.collectionToActionsMap.items():
            if action.id in actionIds:
                action.linkedCollections.append(collectionName)

        self.m_registerActionEvent(action)

    def save(self, dbManager):
        collectionToActionMapAsJson = json.dumps(self.collectionToActionsMap)
        dbManager.db[Keys.STATE_COLLECTION].replace_one({"_id":Keys.ACTION_MANAGER_ID}, {"collectionToActionMapAsJson": collectionToActionMapAsJson}, upsert=True)

    def load(self, dbManager):
        actionManagerState = dbManager.db[Keys.STATE_COLLECTION].find_one({"_id":Keys.ACTION_MANAGER_ID})

        if actionManagerState != None:
            collectionToActionMapAsJson = actionManagerState.get('collectionToActionMapAsJson')
            if collectionToActionMapAsJson != None:
                self.collectionToActionsMap = json.loads(collectionToActionMapAsJson)

    def unregisterAction(self, action):
        if action != None:
            self.unregisterActionId(action.id)

    def unregisterActionId(self, actionId):
        self.actions = [a for a in self.actions if a.id != actionId]

    def applyFilter(self, actions, filterString):
        filteredActions = []

        for a in actions:
            if filterString in a.id or len([t for t in a.filterTags if filterString in t]) > 0:
                filteredActions.append(a)

        return filteredActions

    def getActionsFiltered(self, filterString):
        return self.applyFilter(self.actions, filterString)

    def getActionById(self, actionId) -> Action:
        actionsWithId = [a for a in self.actions if a.id == actionId]
        return actionsWithId[0] if len(actionsWithId) > 0 else None

    def linkActionToCollection(self, actionId, collectionName):
        action = self.getActionById(actionId)
        if collectionName in action.linkedCollections:
            return

        if collectionName in self.collectionToActionsMap.keys():
            self.collectionToActionsMap[collectionName].append(actionId)
        else:
            self.collectionToActionsMap[collectionName] = [actionId]

        action.linkedCollections.append(collectionName)

        self.m_linkActionToCollectionEvent(actionId, collectionName)

    def unlinkActionFromCollection(self, actionId, collectionName):
        self.collectionToActionsMap[collectionName].remove(actionId)

        action = self.getActionById(actionId)
        if action != None:
            self.getActionById(actionId).linkedCollections.remove(collectionName)

        self.m_unlinkActionFromCollectionEvent(actionId, collectionName)
        
    def getCollectionActionIds(self, collectionName):
        actionIds = self.collectionToActionsMap.get(collectionName)
        return actionIds if actionIds != None else []

    def isActionRegisteredForCollection(self, actionId, collectionName):
        return actionId in self.getCollectionActionIds(collectionName)

    def getCollectionActionsFiltered(self, collectionName, filterString):
        actions = self.getCollectionActionIds(collectionName)
        return self.applyFilter(actions, filterString)

    def isValidActionId(self, actionId):
        return self.isActionIdRegistered(actionId)

    def isActionIdRegistered(self, actionId):
        return actionId in [action.id for action in self.actions]

    def getGeneralActions(self):
        return [action for action in self.actions if action.actionType == ActionType.GeneralAction]

    def getDocumentActions(self):
        return [action for action in self.actions if action.actionType == ActionType.DocumentAction]