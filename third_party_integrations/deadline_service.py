
import sys
import os
import subprocess
import re
from MetadataManagerCore.Event import Event
import threading

class DeadlineServiceInfo(object):
    def __init__(self):
        super().__init__()

        self.deadlineInstallPath = ""
        self.webserviceHost = ""
        self.webservicePort = 8082
        self.deadlineStandalonePythonPackagePath = ""

    def initWebservice(self, deadlineStandalonePythonPackagePath, hostName, port=8082):
        self.deadlineStandalonePythonPackagePath = deadlineStandalonePythonPackagePath
        self.webserviceHost = hostName
        self.webservicePort = port

    def setDeadlineCmdPath(self, path):
        self.deadlineCmdPath = path

    @property
    def deadlineCmdPath(self):
        return os.path.join(self.deadlineInstallPath, r"bin\deadlinecommand.exe")

class DeadlineService(object):
    def __init__(self, info: DeadlineServiceInfo):
        super().__init__()
        self.lock = threading.Lock()

        self.updateInfo(info)
        self.deadlineConnection = None
        self.webserviceConnectionEstablished = False
        self.msgLog = []

        self.messageUpdateEvent = Event()

    def printMsg(self, msg):
        self.msgLog.append(msg)
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

    def updateInfo(self, info: DeadlineServiceInfo):
        self.lock.acquire()

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

        self.lock.release()

    """
    Returns the submitted job info as dictionary if the submission was successful otherwise an exception is thrown.
    """
    def submitJob(self, jobInfoFilename, pluginInfoFilename, auxiliaryFilenames=[]):
        self.printMsg(f"Submitting job {jobInfoFilename}...")

        if self.webserviceConnectionEstablished:
            try:
                job = self.deadlineConnection.Jobs.SubmitJobFiles(jobInfoFilename, pluginInfoFilename, aux=auxiliaryFilenames)
                if job != None:
                    self.printMsg(f"Successfully submitted job with id {job['_id']}")
                return job
            except Exception as e:
                self.printMsg(str(e))

        self.printCmdLineFallback()
        auxFilesStr = " ".join([f"\"{os.path.normpath(auxFile)}\"" for auxFile in auxiliaryFilenames])
        cmd = f"\"{os.path.normpath(jobInfoFilename)}\" \"{os.path.normpath(pluginInfoFilename)}\" {auxFilesStr}"
        cmdOutput = self.runDeadlineCmd(cmd)

        if isinstance(cmdOutput, str):
            errorMatch = re.search('Error:(.*)\n', cmdOutput)

            if not errorMatch:
                #resultMatch = re.search('Result=(.*)\n', cmdOutput)
                jobIDMatch = re.search('JobID=(.*)\n', cmdOutput)

                if jobIDMatch != None:
                    return {'_id':jobIDMatch.group(1)}

        return None
        