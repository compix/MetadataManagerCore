from abc import ABCMeta, abstractmethod

class FileHandler(object,metaclass=ABCMeta):
    """Don't forget to register the handler class. Handlers must have a default constructor.
    """
    def __init__(self) -> None:
        super().__init__()
        
        self.name = type(self).__name__
        self.description = ""
    
    def setupFromDict(self, infoDict: dict):
        self.name = infoDict.get('name')
        self.description = infoDict.get('description')

    def asDict(self):
        return {'name': self.name, 'description': self.description, 'class': type(self).__name__}

    @abstractmethod
    def __call__(self, filename):
        ...