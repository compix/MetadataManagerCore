import sys
import os
import subprocess
import re
from MetadataManagerCore.Event import Event
import threading
import tempfile
import distutils
from distutils import dir_util
import shutil

class DeadlineServiceInfo(object):
    def __init__(self):
        super().__init__()

        self.deadlineInstallPath = ""
        self.webserviceHost = ""
        self.webservicePort = 8082
        self.deadlineStandalonePythonPackagePath = ""
        self.deadlineRepositoryLocation = r"C:\DeadlineRepository10"

    def initWebservice(self, deadlineStandalonePythonPackagePath, hostName, port=8082):
        self.deadlineStandalonePythonPackagePath = deadlineStandalonePythonPackagePath
        self.webserviceHost = hostName
        self.webservicePort = port

    def setDeadlineCmdPath(self, path):
        self.deadlineCmdPath = path

    @property
    def deadlineCmdPath(self):
        return os.path.join(self.deadlineInstallPath, r"bin\deadlinecommand.exe")

    @property
    def deadlineCustomPluginsPath(self):
        return os.path.join(self.deadlineRepositoryLocation, r"custom\plugins")

def threadLocked(func):
    def wrapper(*args, **kwargs):
        args[0].lock.acquire()
        ret = func(*args, **kwargs)
        args[0].lock.release()
        return ret
    return wrapper

class DeadlineService(object):
    def __init__(self, info: DeadlineServiceInfo):
        super().__init__()
        self.lock = threading.Lock()

        self.updateInfo(info)
        self.deadlineConnection = None
        self.webserviceConnectionEstablished = False

        self.messageUpdateEvent = Event()

    def printMsg(self, msg):
        self.messageUpdateEvent(msg)

    def printCmdLineFallback(self):
        self.printMsg(f"\n\nTrying command line fallback with deadline command at path {self.info.deadlineCmdPath}...")

    def runDeadlineCmd(self, cmd):
        output = None
        if os.path.exists(self.info.deadlineCmdPath):
            try:
                p = subprocess.Popen(f"\"{os.path.normpath(self.info.deadlineCmdPath)}\" {cmd}", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                output = p.stdout.read()
                output = output.decode("windows-1252",'backslashreplace')
                self.printMsg(output)
            except Exception as e:
                self.printMsg(str(e))
        else:
            self.printMsg(f"Could not find deadlineCmdPath at {self.info.deadlineCmdPath}")

        return output

    @threadLocked
    def updateInfo(self, info: DeadlineServiceInfo):
        self.info = info

        if self.info != None:
            self.webserviceConnectionEstablished = False

            # Get deadline version and check if the connection can be established:
            try:
                if os.path.exists(self.info.deadlineStandalonePythonPackagePath):
                    if self.info.deadlineStandalonePythonPackagePath not in sys.path:
                        sys.path.append(os.path.abspath(os.path.join(self.info.deadlineStandalonePythonPackagePath, os.pardir)))
                        sys.path.append(self.info.deadlineStandalonePythonPackagePath)
                else:
                    raise Exception(f"Could not find deadline standalone python package path {self.info.deadlineStandalonePythonPackagePath}")

                from Deadline.DeadlineConnect import DeadlineCon
                
                self.deadlineConnection = DeadlineCon(self.info.webserviceHost, self.info.webservicePort)

                deadlineVersion = self.deadlineConnection.Repository.GetDeadlineVersion()
                self.printMsg(f"Established connection with deadline webservice on host {self.info.webserviceHost} via port {self.info.webservicePort}.")
                self.printMsg(f"Deadline version: {deadlineVersion}")
                self.webserviceConnectionEstablished = True
            except Exception as e:
                self.printMsg(str(e))
                self.printCmdLineFallback()
                self.runDeadlineCmd("About")
        else:
            self.info = DeadlineServiceInfo()

    """
    Returns the submitted job if the submission was successful otherwise an exception is thrown.
    If returnJobIdOnly is false the job is returned as a dictionary, otherwise only the job id is returned.
    """
    @threadLocked
    def submitJobFiles(self, jobInfoFilename, pluginInfoFilename, auxiliaryFilenames=[], quiet=False, returnJobIdOnly=False):
        if not quiet:
            self.printMsg(f"Submitting job {jobInfoFilename} with plugin info {pluginInfoFilename}...")

        if self.webserviceConnectionEstablished:
            try:
                job = self.deadlineConnection.Jobs.SubmitJobFiles(jobInfoFilename, pluginInfoFilename, aux=auxiliaryFilenames, idOnly=returnJobIdOnly)
                if job != None and not quiet:
                    self.printMsg(f"Successfully submitted job with id {job['_id']}")
                return job['_id']
            except Exception as e:
                self.printMsg(str(e))

                if not quiet:
                    self.printCmdLineFallback()
        
        auxFilesStr = " ".join([f"\"{os.path.normpath(auxFile)}\"" for auxFile in auxiliaryFilenames])
        cmd = f"\"{os.path.normpath(jobInfoFilename)}\" \"{os.path.normpath(pluginInfoFilename)}\" {auxFilesStr}"

        """
        argsFile = tempfile.mkstemp(suffix="_deadline_args.txt")[1]

        with open(argsFile, mode="w+") as f:
            f.write("-SubmitMultipleJobs\n-job\n")
            f.write(f"{os.path.normpath(jobInfoFilename)}\n")
            f.write(f"{os.path.normpath(pluginInfoFilename)}\n")

            for auxFile in auxiliaryFilenames:
                f.write(f"\n{os.path.normpath(auxFile)}")
        

        cmd = f"\"{os.path.normpath(argsFile)}\""
        """

        cmdOutput = self.runDeadlineCmd(cmd)
        
        #os.remove(argsFile)

        if isinstance(cmdOutput, str):
            errorMatch = re.search('Error:(.*)', cmdOutput)

            if not errorMatch:
                jobIDMatch = re.search('JobID=(.*)\n', cmdOutput)

                if jobIDMatch != None:
                    jobId = jobIDMatch.group(1).replace('\r','')
                    return jobId if returnJobIdOnly else {'_id':jobId}

        return None

    """
    Returns the submitted job as dictionary if the submission was successful otherwise an exception is thrown.
    """
    def submitJob(self, jobInfoDict, pluginInfoDict, auxiliaryFilenames=[], quiet=False, returnJobIdOnly=False):
        jobInfoFilename = tempfile.mkstemp(suffix=".txt")[1]
        pluginInfoFilename = tempfile.mkstemp(suffix=".txt")[1]

        with open(jobInfoFilename, mode='w+') as f:
            for key, val in jobInfoDict.items():
                f.write(f"{str(key)}={str(val)}\n")

        with open(pluginInfoFilename, mode='w+') as f:
            f.write("\n")
            for key, val in pluginInfoDict.items():
                f.write(f"{str(key)}={str(val)}\n")         

        ret = self.submitJobFiles(jobInfoFilename, pluginInfoFilename, auxiliaryFilenames=auxiliaryFilenames, quiet=quiet, returnJobIdOnly=returnJobIdOnly)

        os.remove(jobInfoFilename)
        os.remove(pluginInfoFilename)

        return ret

    def verifyDeadlineRepository(self):
        if not os.path.isdir(self.info.deadlineRepositoryLocation):
            self.printMsg(f"Could not find the deadline location {self.info.deadlineRepositoryLocation}. Please specify the deadline repository location.")
            return False

        if not os.path.isdir(self.info.deadlineCustomPluginsPath):
            self.printMsg(f"Could not find the custom plugins location {self.info.deadlineCustomPluginsPath}.")
            return False

        return True

    def installDeadlinePlugin(self, pluginLocation):
        if not self.verifyDeadlineRepository():
            return False

        if os.path.isdir(pluginLocation):
            try:
                distutils.dir_util.copy_tree(pluginLocation, os.path.join(self.info.deadlineCustomPluginsPath, os.path.basename(pluginLocation)))
                return True
            except Exception as e:
                self.printMsg(str(e))
                return False
        else:
            self.printMsg(f"Could not find the given plugin location: {pluginLocation}")
        
        return False

    """
    Installs a plugin that can be identified by the given name in the third_party_integrations/deadline folder.
    """
    def installKnownDeadlinePlugin(self, pluginName):
        curLocation = os.path.abspath(os.path.dirname(__file__))
        return self.installDeadlinePlugin(os.path.join(curLocation, pluginName))