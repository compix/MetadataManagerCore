from Deadline.Plugins import *
import os

######################################################################
## This is the function that Deadline calls to get an instance of the
## main DeadlinePlugin class.
######################################################################
def GetDeadlinePlugin():
    return MaxPipelinePlugin()

######################################################################
## This is the function that Deadline calls when the plugin is no
## longer in use so that it can get cleaned up.
######################################################################
def CleanupDeadlinePlugin( deadlinePlugin ):
    deadlinePlugin.Cleanup()

class MaxPipelinePlugin (DeadlinePlugin):
    def __init__( self ):
        self.InitializeProcessCallback += self.InitializeProcess
        self.RenderExecutableCallback += self.RenderExecutable
        self.RenderArgumentCallback += self.RenderArgument
        self.EndJobCallback += self.EndJob

    ## Clean up the plugin.
    def Cleanup(self):
        # Clean up stdout handler callbacks.
        for stdoutHandler in self.StdoutHandlers:
            del stdoutHandler.HandleCallback

        del self.InitializeProcessCallback
        del self.RenderExecutableCallback
        del self.RenderArgumentCallback
        del self.EndJobCallback

    ## Called by Deadline to initialize the plugin.
    def InitializeProcess( self ):
        self.LogInfo("Initializing 3ds Max Pipeline Plugin...")

        # Set the plugin specific settings.
        self.SingleFramesOnly = False
        self.PluginType = PluginType.Simple

        # Set the ManagedProcess specific settings.
        self.UseProcessTree = True
        self.StdoutHandling = True

        # Set the stdout handlers.
        self.AddStdoutHandlerCallback("WARNING:.*").HandleCallback += self.HandleStdoutWarning
        self.AddStdoutHandlerCallback("ERROR:(.*)").HandleCallback += self.HandleStdoutError

        self.LogInfo("Successfully Executed Initialization Process.")
    
    def EndJob(self):
        job = self.GetJob()
        outputDir = job.GetJobExtraInfoKeyValue("OutputDirectory0")
        outputFile = job.GetJobInfoKeyValue("OutputFilename0")
        if outputFile and outputDir and not os.path.exists(os.path.join(outputDir, outputFile)):
            self.FailRender( "Expected output file " + os.path.join(outputDir, outputFile) + " was not generated." )

        super(MaxPipelinePlugin, self).EndJob()

    ## Callback for when a line of stdout contains a WARNING message.
    def HandleStdoutWarning( self ):
        self.LogWarning( self.GetRegexMatch(0) )

    ## Callback for when a line of stdout contains an ERROR message.
    def HandleStdoutError( self ):
        self.FailRender( "Detected an error: " + self.GetRegexMatch(1) )

    ## Callback to get the executable used for rendering.
    def RenderExecutable( self ):
        version = self.GetPluginInfoEntry("3dsMaxVersion")
        return self.GetConfigEntry( "3dsMaxExe" + version)

    ## Callback to get the arguments that will be passed to the executable.
    def RenderArgument( self ):
        auxiliaryFilenames = self.GetAuxiliaryFilenames()

        if len(auxiliaryFilenames) == 0:
            self.FailRender("Missing required auxiliary files.")
            return ""

        maxScript = auxiliaryFilenames[0]
        self.LogInfo("Max Script Path: " + maxScript)

        if not os.path.exists(maxScript):
            self.FailRender("Could not find max script path: " + maxScript)
            return ""
            
        arguments = self.GetPluginInfoEntryWithDefault("3dsMaxArgs", " -ma -silent")
        arguments += " -u MAXScript " + maxScript
        return arguments