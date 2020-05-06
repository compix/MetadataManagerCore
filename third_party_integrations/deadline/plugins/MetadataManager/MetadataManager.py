from Deadline.Plugins import *
import os

######################################################################
## This is the function that Deadline calls to get an instance of the
## main DeadlinePlugin class.
######################################################################
def GetDeadlinePlugin():
    return MetadataManagerPlugin()

######################################################################
## This is the function that Deadline calls when the plugin is no
## longer in use so that it can get cleaned up.
######################################################################
def CleanupDeadlinePlugin( deadlinePlugin ):
    deadlinePlugin.Cleanup()

class MetadataManagerPlugin(DeadlinePlugin):
    def __init__( self ):
        self.InitializeProcessCallback += self.InitializeProcess
        self.RenderExecutableCallback += self.RenderExecutable
        self.RenderArgumentCallback += self.RenderArgument

    ## Clean up the plugin.
    def Cleanup():
        # Clean up stdout handler callbacks.
        for stdoutHandler in self.StdoutHandlers:
            del stdoutHandler.HandleCallback

        del self.InitializeProcessCallback
        del self.RenderExecutableCallback
        del self.RenderArgumentCallback

    ## Called by Deadline to initialize the plugin.
    def InitializeProcess( self ):
        self.LogInfo("Initializing MetadataManagerPlugin...")

        # Set the plugin specific settings.
        self.SingleFramesOnly = False
        self.PluginType = PluginType.Simple

        # Set the ManagedProcess specific settings.
        self.UseProcessTree = True
        self.StdoutHandling = True

        # Set the stdout handlers.
        self.AddStdoutHandlerCallback(".*WARNING:.*").HandleCallback += self.HandleStdoutWarning
        self.AddStdoutHandlerCallback(".*ERROR:(.*)").HandleCallback += self.HandleStdoutError

        self.LogInfo("Successfully Executed Initialization Process.")

    ## Callback for when a line of stdout contains a WARNING message.
    def HandleStdoutWarning( self ):
        self.LogWarning( self.GetRegexMatch(0) )

    ## Callback for when a line of stdout contains an ERROR message.
    def HandleStdoutError( self ):
        self.FailRender( "Detected an error: " + self.GetRegexMatch(1) )

    ## Callback to get the executable used for rendering.
    def RenderExecutable( self ):
        isPythonJob = self.GetPluginInfoEntryWithDefault("as_python", 'False') == 'True'
        return self.GetConfigEntry("PythonPath") if isPythonJob else self.GetConfigEntry("MetadataManagerExe")

    ## Callback to get the arguments that will be passed to the executable.
    def RenderArgument( self ):
        auxiliaryFilenames = self.GetAuxiliaryFilenames()

        if len(auxiliaryFilenames) == 0:
            self.FailRender("Missing required auxiliary files.")
            return ""

        taskFile = auxiliaryFilenames[0]
        self.LogInfo("Task File: " + taskFile)

        if not os.path.exists(taskFile):
            self.FailRender("Could not find task file path: " + taskFile)
            return ""

        isPythonJob = self.GetPluginInfoEntryWithDefault("as_python", 'False') == 'True'
        arguments = ('"' + self.GetConfigEntry("MetadataManagerPythonFile") + '" ') if isPythonJob else ""
        arguments += self.GetPluginInfoEntryWithDefault("args", '')
        arguments += ' -mode "Console" -task "' + taskFile + '"'
        return arguments