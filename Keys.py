# This document contains global keys/definitions

notDefinedValue = "N.D."

systemIDKey = "s_id"

systemVersionKey = "s_version"

collection = 's_collection'

preview = "preview"

# s_id is not unique but s_id + s_version is which happens to be defined as _id = s_id + s_version
systemKeys = ["_id", systemIDKey, systemVersionKey, preview]

collectionsMD = "collectionsMD"

COLLECTION_MD_DISPLAY_NAME_IDX = 0
COLLECTION_MD_KEY_IDX = 1

OLD_VERSIONS_COLLECTION_SUFFIX = "_old_versions"

STATE_COLLECTION = "state"
HOST_PROCESSES_COLLECTION = "host_processes"
SERVICE_COLLECTION = "services"
SERVICE_PROCESS_COLLECTION = "service_processes"
ACTION_MANAGER_ID = "action_manager"
ENVIRONMENT_MANAGER_ID = "environment_manager"
ARCHIVED_ENVIRONMENTS_ID = "archived_environments"
DEADLINE_SERVICE_ID = "deadline_service"
TAGS = 's_tags'

RABBITMQ_ENVIRONMENT_EXCHANGE = "env_exchange"

hiddenCollections = set([collectionsMD, STATE_COLLECTION, HOST_PROCESSES_COLLECTION, SERVICE_COLLECTION, SERVICE_PROCESS_COLLECTION])