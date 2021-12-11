from typing import List
from MetadataManagerCore import Keys
import sys
import os
import subprocess
import re
import threading
import tempfile
import distutils
from distutils import dir_util
import shutil
import logging
import requests
import json
from MetadataManagerCore.mongodb_manager import MongoDBManager
from MetadataManagerCore.Event import Event

class DeadlineServiceInfo(object):
    def __init__(self):
        super().__init__()

        self.deadlineInstallPath = ""
        self.webserviceHost = ""
        self.webservicePort = 8082
        self.deadlineRepositoryLocation = r"C:\DeadlineRepository10"

    def initWebservice(self, hostName, port=8082):
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

    @property
    def customJobInfoDirectory(self):
        return os.path.join(self.deadlineRepositoryLocation, 'custom', 'job_info')

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
        self.webserviceConnectionEstablished = False
        self.dbManager: MongoDBManager  = None

        self.logger = logging.getLogger(__name__)

        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

        self.onConnected = Event()

    def printMsg(self, msg):
        self.logger.info(msg)

    def printCmdLineFallback(self):
        self.printMsg(f"\n\nTrying command line fallback with deadline command at path {self.info.deadlineCmdPath}...")

    def runDeadlineCmd(self, cmd, quiet=False):
        output = None
        if os.path.exists(self.info.deadlineCmdPath):
            try:
                p = subprocess.Popen(f"\"{os.path.normpath(self.info.deadlineCmdPath)}\" {cmd}", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                output = p.stdout.read()
                output = output.decode("windows-1252",'backslashreplace')
                if not quiet:
                    self.printMsg(output)
            except Exception as e:
                self.printMsg(str(e))
        else:
            self.printMsg(f"Could not find deadlineCmdPath at {self.info.deadlineCmdPath}")

        return output

    @threadLocked
    def updateInfo(self, info: DeadlineServiceInfo):
        self.info = info

    def fullRequestRoute(self, relRoute: str):
        return f'http://{self.info.webserviceHost}:{self.info.webservicePort}/{relRoute}'

    def requestGET(self, route: str):
        return self.checkResponse(requests.get(self.fullRequestRoute(route)))

    def requestPOST(self, route: str, body):
        return self.checkResponse(requests.post(self.fullRequestRoute(route), body))

    def requestSubmitJob(self, jobInfo: dict, pluginInfo: dict, auxFiles: List[str] = None, returnJobIdOnly = False):
        # Copy the auxFiles to repository to make them accessible in a network
        auxFileDir = None
        auxFilesInRepo = []
        try:
            if auxFiles and len(auxFiles) > 0:
                auxFilesDir = os.path.join(self.info.deadlineRepositoryLocation, 'temp_auxfiles')
                os.makedirs(auxFilesDir, exist_ok=True)
                auxFileDir = tempfile.mkdtemp(dir=auxFilesDir)
                for auxFile in auxFiles:
                    copiedAuxFilename = shutil.copy(auxFile, os.path.join(auxFileDir, os.path.basename(auxFile)))
                    auxFilesInRepo.append(copiedAuxFilename)

            body = '{"JobInfo":'+json.dumps(jobInfo)+',"PluginInfo":'+json.dumps(pluginInfo)+',"AuxFiles":'+json.dumps(auxFilesInRepo)
            if returnJobIdOnly:
                body += ',"IdOnly":true'
            body += '}'

            r = self.requestPOST('api/jobs', body)
        except:
            raise
        finally:
            if auxFileDir:
                shutil.rmtree(auxFileDir)

        return r.json()

    def checkResponse(self, r):
        if not r.ok:
            raise RuntimeError(f'Failed to connect to the deadline webservice. Please make sure the webservice is running on the specified address: {self.info.webserviceHost}:{self.info.webservicePort}. Status Code: {r.status_code}')
        
        return r

    @threadLocked
    def connect(self):
        """
        Tries to connect to the deadline service.
        """
        if self.info != None:
            self.webserviceConnectionEstablished = False

            # Get deadline version and check if the connection can be established:
            try:
                r = self.requestGET('')

                self.printMsg(f"Established connection with deadline webservice on host {self.info.webserviceHost} via port {self.info.webservicePort}.")
                self.printMsg(r.text)
                self.webserviceConnectionEstablished = True
                self.onConnected()
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
    def submitJobFiles(self, jobInfoFilename, pluginInfoFilename, auxiliaryFilenames=None, quiet=False, returnJobIdOnly=False):
        if auxiliaryFilenames == None:
            auxiliaryFilenames = []
            
        if not quiet:
            self.printMsg(f"Submitting job {jobInfoFilename} with plugin info {pluginInfoFilename}...")

        auxFilesStr = " ".join([f"\"{os.path.normpath(auxFile)}\"" for auxFile in auxiliaryFilenames])
        cmd = f"\"{os.path.normpath(jobInfoFilename)}\" \"{os.path.normpath(pluginInfoFilename)}\" {auxFilesStr}"

        cmdOutput = self.runDeadlineCmd(cmd)
        
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
    def submitJob(self, jobInfoDict, pluginInfoDict, auxiliaryFilenames=None, quiet=False, returnJobIdOnly=False):
        if self.webserviceConnectionEstablished:
            try:
                job = self.requestSubmitJob(jobInfoDict, pluginInfoDict, auxiliaryFilenames, returnJobIdOnly=returnJobIdOnly)

                if not quiet:
                    jobId = job['_id']
                    self.printMsg(f"Successfully submitted job with id {jobId}")

                return job['_id'] if returnJobIdOnly else job
            except Exception as e:
                self.printMsg(str(e))

                if not quiet:
                    self.printCmdLineFallback()


        jobInfoFilenameHandle, jobInfoFilename = tempfile.mkstemp(suffix=".txt")
        if not quiet:
            self.printMsg(f"Created temp job info file: {jobInfoFilename}")

        with open(jobInfoFilename, mode='w+') as f:
            for key, val in jobInfoDict.items():
                f.write(f"{str(key)}={str(val)}\n")

        os.close(jobInfoFilenameHandle)

        pluginInfoFilenameHandle, pluginInfoFilename = tempfile.mkstemp(suffix=".txt")
        if not quiet:
            self.printMsg(f"Created temp plugin info file: {pluginInfoFilename}")

        with open(pluginInfoFilename, mode='w+') as f:
            f.write("\n")
            for key, val in pluginInfoDict.items():
                f.write(f"{str(key)}={str(val)}\n")

        os.close(pluginInfoFilenameHandle)

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
        return self.installDeadlinePlugin(os.path.join(curLocation, "plugins", pluginName))

    def getJobNames(self, quiet=False):
        if not quiet:
            self.printMsg(f"Retrieving jobs...")

        if self.webserviceConnectionEstablished:
            try:
                r = self.requestGET('api/jobs')
                jobs = r.json()

                return [job['Props']['Name'] for job in jobs]
            except Exception as e:
                self.printMsg(str(e))

                if not quiet:
                    self.printCmdLineFallback()

        cmd = '-getjobs'
        cmdOutput = self.runDeadlineCmd(cmd, quiet=True)

        if isinstance(cmdOutput, str):
            errorMatch = re.search('Error:(.*)', cmdOutput)
            
            if not errorMatch:
                jobNames = re.findall('JobName=(.*)\n', cmdOutput)

                if jobNames != None:
                    return [jobName.strip() for jobName in jobNames]
            elif not quiet:
                self.printMsg(cmdOutput)

        return None

    def getPoolNames(self, quiet=False):
        if not quiet:
            self.printMsg(f"Retrieving jobs...")

        if self.webserviceConnectionEstablished:
            try:
                r = self.requestGET('api/pools')
                poolNames = r.json()

                return poolNames
            except Exception as e:
                self.printMsg(str(e))

                if not quiet:
                    self.printCmdLineFallback()

        cmd = '-pools'
        cmdOutput = self.runDeadlineCmd(cmd, quiet=True)

        if isinstance(cmdOutput, str):
            errorMatch = re.search('Error:(.*)', cmdOutput)
            
            if not errorMatch:
                poolNames = cmdOutput.replace('\r\n', '\n').split('\n')
                return [poolName for poolName in poolNames if poolName.strip() != '']

            elif not quiet:
                self.printMsg(cmdOutput)

        return None

    def saveToDB(self):
        deadlineStandaloneInfo = {
            'host': self.info.webserviceHost,
            'port': self.info.webservicePort,
            'deadlineRepositoryLocation': self.info.deadlineRepositoryLocation
        }

        self.dbManager.db[Keys.STATE_COLLECTION].replace_one({"_id": Keys.DEADLINE_SERVICE_ID}, deadlineStandaloneInfo, upsert=True)

    def loadFromDB(self):
        state = self.dbManager.db[Keys.STATE_COLLECTION].find_one({"_id": Keys.DEADLINE_SERVICE_ID})
        if state != None:
            if not self.info:
                self.updateInfo(DeadlineServiceInfo())

            deadlineRepositoryLocation = state.get('deadlineRepositoryLocation')
            if deadlineRepositoryLocation:
                self.info.deadlineRepositoryLocation = deadlineRepositoryLocation

            self.info.initWebservice(state.get('host'), state.get('port'))

    def save(self, settings, dbManager: MongoDBManager):
        """
        Serializes the state in settings and/or in the database.

        input:
            - settings: Must support settings.setValue(key: str, value)
            - dbManager: MongoDBManager
        """
        settings.setValue("deadline_service", self.info.__dict__)

    def load(self, settings, dbManager: MongoDBManager):
        """
        Loads the state from settings and/or the database.

        input:
            - settings: Must support settings.value(str)
            - dbManager: MongoDBManager
        """
        self.dbManager = dbManager
        infoDict = settings.value("deadline_service")

        if infoDict != None:
            info = DeadlineServiceInfo()
            info.__dict__ = infoDict
            self.updateInfo(info)

        self.loadFromDB()