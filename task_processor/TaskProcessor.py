from MetadataManagerCore.communication.socket_util import JsonSocket
from MetadataManagerCore.task_processor.Task import Task
from MetadataManagerCore.task_processor.TaskPicker import TaskPicker
import socket
import logging
import json
import traceback

class TaskProcessor(JsonSocket):
    def __init__(self, socketTimeout=None):
        super().__init__(socketTimeout)
        self.logger = logging.getLogger(__name__)
        self.taskPickers = []

    def addTaskPicker(self, taskPicker : TaskPicker):
        self.taskPickers.append(taskPicker)

    def removeTaskPicker(self, taskPicker : TaskPicker):
        self.taskPickers.remove(taskPicker)

    def handleClientSocket(self, clientSocket : socket.socket, address, dataDictionary : dict):
        self.logger.info(f"Handling client socket with address {address} and data {str(dataDictionary)}")
        try:
            self.processTask(dataDictionary)
        except Exception as e:
            self.logger.error(f"Failed to process the task from {address}. Reason: {str(e)}")

    def processTaskFromJsonFile(self, jsonFilePath : str):
        self.logger.info(f"Processing task request from json file: {jsonFilePath}")
        try:
            with open(jsonFilePath, mode='r') as f:
                taskDataDictionary = json.load(f)
                self.processTask(taskDataDictionary)
        except Exception as e:
            self.logger.error(f"Failed to process the task from {jsonFilePath}. Reason: {str(e)}")
            self.logger.error(traceback.format_exc())

    def processTask(self, taskDataDictionary: dict):
        taskType = taskDataDictionary.get('taskType')
        if taskType:
            handledTask = False
            for taskPicker in self.taskPickers:
                task = taskPicker.pickTask(taskType)
                if task:
                    handledTask = True
                    try:
                        task.execute(taskDataDictionary)
                    except Exception as e:
                        raise RuntimeError(f"{taskType}: {str(e)}")

                    # Don't handle multiple times if there are multiple task pickers that return a valid task for the given type.
                    break

            if not handledTask:
                raise RuntimeError(f"Unknown taskType: {taskType}")
        else:
            raise RuntimeError(f"No taskType was specified.")
        
    def save(self, settings, dbManager):
        """
        Serializes the state in settings and/or in the database.

        input:
            - settings: Must support settings.setValue(key: str, value)
            - dbManager: MongoDBManager
        """
        settings.setValue('task_processor_socket_timeout', self.timeout)

    def load(self, settings, dbManager):
        """
        Loads the state from settings and/or the database.

        input:
            - settings: Must support settings.value(str)
            - dbManager: MongoDBManager
        """
        timeout = settings.value('task_processor_socket_timeout')
        if timeout:
            self.timeout = timeout

            if self.sock:
                self.sock.settimeout(self.timeout)