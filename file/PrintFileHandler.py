
from MetadataManagerCore.file.FileHandlerRegistry import registerFileHandlerClass
from MetadataManagerCore.file.FileHandler import FileHandler

@registerFileHandlerClass
class PrintFileHandler(FileHandler):
    def __call__(self, filename) -> None:
        print(filename)
    