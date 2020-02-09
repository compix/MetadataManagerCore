
import json
import os
import shutil
import distutils
from distutils import dir_util

VERSION_MD_FILE_NAME = "version.info"

def createFileVersion(srcFilePath, destFolder, maxVersionCount):
    """
    Creates a new version of the given src file given by the absolute srcFilePath in the given absolute path of the destination folder
    where the maximum number of file versions is indicated by maxVersionCount. If maxVersionCount is None the number of versions is unlimited.

    Implementation details: A metadata file "version.info" is created to store information about the newest version in the destFolder.

    return: Newly created version number.
    """

    global VERSION_MD_FILE_NAME

    if not os.path.exists(srcFilePath):
        raise Exception(f"The given source file path {srcFilePath} does not exist.")

    if not os.path.exists(destFolder):
        raise Exception(f"The given destination folder path {destFolder} does not exist.")

    if maxVersionCount != None and maxVersionCount <= 0:
        raise Exception("Please specify a positive number for maxVersionCount or None for unlimited versions.")

    versionMDFilePath = os.path.join(destFolder, VERSION_MD_FILE_NAME)

    # Determine version number
    curVersion = -1
    if os.path.exists(versionMDFilePath):
        with open(versionMDFilePath, "r") as f:
            versionInfoDict = json.load(f)
            try:
                curVersion = versionInfoDict["version"]
            except:
                curVersion = -1

    newVersion = curVersion + 1

    if maxVersionCount != None:
        newVersion = newVersion % maxVersionCount

    # Copy version to destination folder
    destVersionFolder = os.path.join(destFolder, f"version{newVersion}")

    if not os.path.exists(destVersionFolder):
        os.mkdir(destVersionFolder)

    if os.path.isdir(srcFilePath):
        distutils.dir_util.copy_tree(srcFilePath, destVersionFolder)
    else:
        shutil.copy2(srcFilePath, destVersionFolder)

    # Update version metadata file
    versionInfoDict = {"version": newVersion}
    with open(versionMDFilePath, "w+") as f:
        json.dump(versionInfoDict, f)

    return newVersion

def getVersionFolder(baseVersioningFolder, versionNumber=None):
    """
    If versionNumber is None the newest version is returned if available. If the specified version is not available None is returned.
    """

    if not os.path.exists(baseVersioningFolder):
        raise Exception(f"The given base versioning folder path {baseVersioningFolder} does not exist.")

    versionMDFilePath = os.path.join(baseVersioningFolder, VERSION_MD_FILE_NAME)

    if not os.path.exists(versionMDFilePath):
        return None

    if versionNumber == None:
        with open(versionMDFilePath, "r") as f:
            versionInfoDict = json.load(f)
            try:
                versionNumber = versionInfoDict["version"]
            except:
                return None

    versionFolder = os.path.join(baseVersioningFolder, f"version{versionNumber}")

    if os.path.exists(versionFolder):
        return versionFolder
    else:
        return None
