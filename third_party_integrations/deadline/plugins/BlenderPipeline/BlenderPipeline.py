from Deadline.Plugins import *
from System.IO import *
from System import *
from System.Diagnostics import *
from Deadline.Scripting import *
import os

######################################################################
## This is the function that Deadline calls to get an instance of the
## main DeadlinePlugin class.
######################################################################
def GetDeadlinePlugin():
    return BlenderPipelinePlugin()

######################################################################
## This is the function that Deadline calls when the plugin is no
## longer in use so that it can get cleaned up.
######################################################################
def CleanupDeadlinePlugin( deadlinePlugin ):
    deadlinePlugin.Cleanup()

class BlenderPipelinePlugin (DeadlinePlugin):
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
        self.LogInfo("Initializing Blender Pipeline Plugin...")

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

        super(BlenderPipelinePlugin, self).EndJob()

    ## Callback for when a line of stdout contains a WARNING message.
    def HandleStdoutWarning( self ):
        self.LogWarning( self.GetRegexMatch(0) )

    ## Callback for when a line of stdout contains an ERROR message.
    def HandleStdoutError( self ):
        self.FailRender( "Detected an error: " + self.GetRegexMatch(1) )

    ## Callback to get the executable used for rendering.
    def RenderExecutable(self):
        version = self.GetPluginInfoEntry("BlenderVersion")
        build = self.GetPluginInfoEntryWithDefault( "Build", "None" ).lower()
        
        executable = ""
        executableList = self.GetConfigEntry( "Blender" + version )
        
        if(SystemUtils.IsRunningOnWindows()):
            if( build == "32bit" ):
                self.LogInfo( "Enforcing 32 bit build of Blender" )
                executable = FileUtils.SearchFileListFor32Bit( executableList )
                if( executable == "" ):
                    self.LogWarning( "32 bit Blender render executable was not found in the semicolon separated list \"" + executableList + "\". Checking for any executable that exists instead." )
            
            elif( build == "64bit" ):
                self.LogInfo( "Enforcing 64 bit build of Blender" )
                executable = FileUtils.SearchFileListFor64Bit( executableList )
                if( executable == "" ):
                    self.LogWarning( "64 bit Blender render executable was not found in the semicolon separated list \"" + executableList + "\". Checking for any executable that exists instead." )
            
        if( executable == "" ):
            self.LogInfo( "Not enforcing a build of Blender" )
            executable = FileUtils.SearchFileList( executableList )
            if executable == "":
                self.FailRender( "Blender render executable was not found in the semicolon separated list \"" + executableList + "\". The path to the render executable can be configured from the Plugin Configuration in the Deadline Monitor." )
        
        return executable

    ## Callback to get the arguments that will be passed to the executable.
    def RenderArgument( self ):
        auxiliaryFilenames = self.GetAuxiliaryFilenames()

        if len(auxiliaryFilenames) == 0:
            self.FailRender("Missing required auxiliary files.")
            return ""

        scriptFilename = auxiliaryFilenames[0]
        self.LogInfo("Blender Script Path: " + scriptFilename)

        if not os.path.exists(scriptFilename):
            self.FailRender("Could not find script path: " + scriptFilename)
            return ""
            
        extraArgs = self.GetPluginInfoEntryWithDefault("BlenderArgs", "")
        baseScene = self.GetPluginInfoEntryWithDefault("BaseScene", "")
        if baseScene:
            if not os.path.exists(baseScene):
                self.FailRender("Could not find base scene file at " + baseScene)
                return ''

            baseScene = '"' + baseScene.replace('\\', '/') + '"'

        return "-b " + baseScene +  " -P " + scriptFilename + ((" " + extraArgs) if extraArgs else "")
