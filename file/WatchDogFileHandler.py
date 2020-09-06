from MetadataManagerCore.file.FileHandler import FileHandler
from MetadataManagerCore.file.WatchDog import WatchDog

class WatchDogFileHandler(FileHandler):
    def __init__(self) -> None:
        super().__init__()

        self.watchDog : WatchDog = None