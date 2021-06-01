import os

def idxToFrameString(idx, maxDigits):
    s = str(idx)

    if len(s) > maxDigits:
        raise f"Invalid input: The index {idx} exceeds the max digit count {maxDigits}"

    for _ in range(len(s), maxDigits):
        s = "0" + s
    
    return s

def extractFrameFilenames(filename: str, frameCount: int, startsAtZero=False):
    # Check for animation frame pattern:
    idxStart = filename.find("#")
    idxEnd = filename.rfind("#")

    frames = []
    
    if idxStart >= 0:
        maxDigits = (idxEnd + 1) - idxStart

        for i in range(frameCount):
            idx = i if startsAtZero else (i + 1)
            frameStr = idxToFrameString(idx, maxDigits)
            framePath = frameStr.join([filename[:idxStart],filename[idxEnd+1:]])

            frames.append(framePath)
            
    return frames

def yieldExistingFrameFilenames(filename: str):
    # Check for animation frame pattern:
    idxStart = filename.find("#")
    idxEnd = filename.rfind("#")

    if idxStart >= 0:
        maxDigits = (idxEnd + 1) - idxStart

        for i in range(0,10**maxDigits):
            frameStr = idxToFrameString(i, maxDigits)
            framePath = frameStr.join([filename[:idxStart],filename[idxEnd+1:]])

            if os.path.exists(framePath):
                yield framePath
            elif i > 0: # Support frames starting at 1
                break
 
def extractExistingFrameFilenames(filename: str):
    return [fn for fn in yieldExistingFrameFilenames(filename)]

def hasExistingFrameFilenames(filename: str):
    for _ in yieldExistingFrameFilenames(filename):
        return True

    return False