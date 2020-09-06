
from MetadataManagerCore.file.FileHandler import FileHandler

class PrintFileHandler(FileHandler):
    def __call__(self, filename) -> None:
        print(filename)
    