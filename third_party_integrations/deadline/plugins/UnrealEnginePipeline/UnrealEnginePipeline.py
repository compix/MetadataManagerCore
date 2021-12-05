from System import *
from System.Diagnostics import *
from System.IO import *
from Deadline.Plugins import *
from Deadline.Scripting import *

def GetDeadlinePlugin():
    return UnrealEnginePipelinePlugin()

def CleanupDeadlinePlugin(deadlinePlugin):
    deadlinePlugin.Cleanup()

class UnrealEnginePipelinePlugin(DeadlinePlugin):
    def __init__(self):
        self.InitializeProcessCallback += self.InitializeProcess
        self.RenderExecutableCallback += self.RenderExecutable
        self.RenderArgumentCallback += self.RenderArgument
        
    def Cleanup(self):
        for stdoutHandler in self.StdoutHandlers:
            del stdoutHandler.HandleCallback
        
        del self.InitializeProcessCallback
        del self.RenderExecutableCallback
        del self.RenderArgumentCallback
    
    def InitializeProcess(self):
        self.SingleFramesOnly = False
        self.PopupHandling = False
        self.StdoutHandling = True
        
        # Set the stdout handlers.
        self.AddStdoutHandlerCallback(".*Warning:.*").HandleCallback += self.HandleStdoutWarning
        self.AddStdoutHandlerCallback(".* Error:(.*)").HandleCallback += self.HandleStdoutError
        self.AddStdoutHandlerCallback("WARNING:.*").HandleCallback += self.HandleStdoutWarning
        self.AddStdoutHandlerCallback("ERROR:(.*)").HandleCallback += self.HandleStdoutError
        self.AddStdoutHandlerCallback(".*PROGRESS: ([0-9]+(\.?[0-9]+)?).*").HandleCallback += self.HandleProgress

        self.LogInfo("Successfully Executed Initialization Process.")
    
    ## Callback for when a line of stdout contains a WARNING message.
    def HandleStdoutWarning(self):
        self.LogWarning(self.GetRegexMatch(0))

    ## Callback for when a line of stdout contains an ERROR message.
    def HandleStdoutError(self):
        self.FailRender("Detected an error: " + self.GetRegexMatch(1))

    def HandleProgress(self):
        try:
            progress = float(self.GetRegexMatch(1)) * 100.0
            self.SetProgress(progress)
        except:
            pass

    def RenderExecutable(self):
        version = self.GetIntegerPluginInfoEntryWithDefault("Version", 4)
        UEExeList = self.GetConfigEntry("UE_" + str(version)+ "_EditorExecutable")
        UEExe = ""
        
        UEExe = FileUtils.SearchFileList(UEExeList)
        if(UEExe == ""):
            self.FailRender("Unreal Engine " + str(version)+ " editor executable was not found in the semicolon separated list \"" + UEExeList + "\". The path to the editor executable can be configured from the Plugin Configuration in the Deadline Monitor.")
    
        return UEExe
        
    def RenderArgument(self):
        arguments = []
    
        projectFile =  self.GetPluginInfoEntry("ProjectFile")
        projectFile = RepositoryUtils.CheckPathMapping(projectFile)
        if not projectFile.endswith('.uproject'):
            projectFile += '.uproject'
        
        arguments.append('"{0}"'.format(projectFile))
        
        map = self.GetPluginInfoEntryWithDefault("Map", "")
        if map:
            arguments.append('"{0}"'.format(map))

        arguments.append('-NoLoadingScreen')

        if self.GetBooleanPluginInfoEntryWithDefault("VSyncEnabled", False):
            arguments.append('-VSync')
        else:
            arguments.append('-NoVSync')

        if self.GetBooleanPluginInfoEntryWithDefault("DisableTextureStreaming", False):
            arguments.append("-NoTextureStreaming")

        hideMessages = self.GetBooleanPluginInfoEntryWithDefault("HideMessages", True)
        if hideMessages:
            arguments.append('-NoScreenMessages')

        arguments.append('-stdout')
        arguments.append('-FullStdOutLogOutput')

        executePythonScript = self.GetBooleanPluginInfoEntryWithDefault("ExecutePythonScript", False)
        auxiliaryFilenames = self.GetAuxiliaryFilenames()
        if executePythonScript:
            # Note: Script is not executed with -game flag
            if len(auxiliaryFilenames) != 2:
                self.FailRender('Missing auxiliary filenames.')
                return ""

            script = auxiliaryFilenames[0]
            arguments.append('-ExecutePythonScript="{0}"'.format(script))
        else:
            # Render
            if len(auxiliaryFilenames) != 1:
                self.FailRender('Missing auxiliary filenames.')
                return ""

            arguments.append('-game')
            arguments.append('-MoviePipelineLocalExecutorClass={0}'.format('/Script/MovieRenderPipelineCore.MoviePipelinePythonHostExecutor'))
            arguments.append('-ExecutorPythonClass={0}'.format('/Engine/PythonTypes.MD_MoviePipelineExecutor'))
            arguments.append('-MoviePipelineConfig="{0}"'.format(self.GetPluginInfoEntry('MoviePipelineQueueSettings')))

        if self.GetBooleanPluginInfoEntryWithDefault("OverrideResolution", False):
            xResolution = self.GetIntegerPluginInfoEntryWithDefault("ResX", 1280)
            yResolution = self.GetIntegerPluginInfoEntryWithDefault("ResY", 720)       
            arguments.append('-ResX={0}'.format(xResolution))
            arguments.append('-ResY={0}'.format(yResolution))
            arguments.append('-windowed')

        if auxiliaryFilenames:
            arguments.append('-MD_DESC_FILE="{0}"'.format(auxiliaryFilenames[-1]))

        return " ".join(arguments)
