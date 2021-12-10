from enum import Enum

class DataRetrievalType(Enum):
    UseDocumentFilter = 'UseDocumentFilter' # Uses the provided document filtering info to determine documents as data
    UseSubmittedData = 'UseSubmittedData'