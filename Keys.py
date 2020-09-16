# This document contains global keys/definitions

notDefinedValue = "N.D."

systemIDKey = "s_id"

systemVersionKey = "s_version"

# s_id is not unique but s_id + s_version is which happens to be defined as _id = s_id + s_version
systemKeys = ["_id", systemIDKey, systemVersionKey]

collectionsMD = "collectionsMD"
hiddenCollections = set([collectionsMD])

COLLECTION_MD_DISPLAY_NAME_IDX = 0
COLLECTION_MD_KEY_IDX = 1

OLD_VERSIONS_COLLECTION_SUFFIX = "_old_versions"

STATE_COLLECTION = "state"
HOSTS_COLLECTION = "hosts"
ACTION_MANAGER_ID = "action_manager"
ENVIRONMENT_MANAGER_ID = "environment_manager"
ARCHIVED_ENVIRONMENTS_ID = "archived_environments"
