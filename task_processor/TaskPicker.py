class TaskPicker(object):
    def __init__(self):
        super().__init__()

    def pickTask(self, taskType: str):
        """
        Returns an instance of a Task for the given taskType.
        """
        raise NotImplementedError()