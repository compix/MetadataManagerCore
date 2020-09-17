from MetadataManagerCore.file.WatchDog import WatchDog
from typing import List
from MetadataManagerCore.Event import Event
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import os
import shutil

class WatchDogFileSystemEventHandler(FileSystemEventHandler):
    def __init__(self, watchDog) -> None:
        self.watchDog = watchDog

    def on_created(self, event):
        if self.watchDog.checkExtension(event.src_path):
            self.watchDog.onFileCreated(event.src_path)

    def on_modified(self, event):
        if self.watchDog.checkExtension(event.src_path):
            self.watchDog.onFileModified(event.src_path)

class FileSystemWatchDog(WatchDog):
    def __init__(self, watchedFolder: str, watchedExtensions: List[str] = None, recursive=False) -> None:
        super().__init__(watchedFolder, watchedExtensions, recursive)

        self.observer = None

    def run(self):
        self.observer = Observer()
        self.observer.schedule(WatchDogFileSystemEventHandler(self), self.watchedFolder, recursive=self.recursive)
        self.observer.start()
        self.observer.join()

    @property
    def running(self):
        return self.observer and self.observer.is_alive()

    def stop(self):
        if self.observer:
            self.observer.stop()

    def processFiles(self, fileHandler):
        for root, _, filenames in os.walk(self.watchedFolder):
            for filename in filenames:
                if self.checkExtension(filename):
                    fileHandler(os.path.join(root, filename))

            if not self.recursive:
                return

    def copyFile(self, srcFilename, destFilename):
        shutil.copy(srcFilename, destFilename)

    def renameFile(self, srcFilename, destFilename):
        os.rename(srcFilename, destFilename)
