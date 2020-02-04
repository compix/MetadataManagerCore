class DocumentAction(object):
    def __init__(self):
        self.linkedCollections = []

    def execute(self, document):
        pass

    @property
    def id(self):
        return self.__class__.__name__

    @property
    def filterTags(self):
        return []

    @property
    def category(self):
        return "Default"