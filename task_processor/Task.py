class Task(object):
    def __init__(self):
        super().__init__()

    def execute(self, dataDict: dict):
        raise NotImplementedError()