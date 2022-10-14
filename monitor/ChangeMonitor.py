import time
import threading
from abc import ABC, abstractmethod
import logging

logger = logging.get(__name__)

class ChangeMonitor(ABC):
    def __init__(self) -> None:
        super().__init__()

        self.checkIntervalInSeconds = 1.0
        self.isRunning = False
        self.thread: threading.Thread = None

    @abstractmethod
    def checkForChanges(self):
        ...

    def runAsync(self):
        self.isRunning = True
        self.thread = threading.Thread(target=self.run_)
        self.thread.start()

    def stop(self):
        self.isRunning = False

    def run_(self):
        while self.isRunning:
            self.checkForChanges()
            time.sleep(self.checkIntervalInSeconds)

    def join(self):
        if self.thread:
            self.thread.join()

    def stopAndJoin(self):
        self.stop()
        self.join()

    @abstractmethod
    def onStateChanged(self):
        ...