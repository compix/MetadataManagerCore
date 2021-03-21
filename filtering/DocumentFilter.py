from typing import Callable


class DocumentFilter(object):
    def __init__(self, filterFunction = None, uniqueFilterLabel : str = None, active : bool = False, hasStringArg : bool = False) -> None:
        super().__init__()

        self.filterFunction: Callable[[dict, str], bool] = filterFunction
        self.uniqueFilterLabel = uniqueFilterLabel
        self.active = active
        self.args = None
        self.hasStringArg = hasStringArg
        self.negate = False

    def setActive(self, active):
        self.active = active

    def setArgs(self, args):
        self.args = args

    def setNegateFilter(self, negate):
        self.negate = negate

    def apply(self, document):
        if self.active:
            filterFuncResult = self.filterFunction(document, self.args) if self.hasStringArg else self.filterFunction(document)
            return filterFuncResult if not self.negate else not filterFuncResult
        else:
            return True

    def copy(self):
        f = self.__class__()
        f.filterFunction = self.filterFunction
        f.setFromDict(self.asDict())
        return f

    def preApply(self):
        pass

    def postApply(self):
        pass

    def asDict(self):
        """Returns the filter document as a dictionary excluding the filter function.
        """
        return {
            'uniqueFilterLabel': self.uniqueFilterLabel,
            'active': self.active,
            'args': self.args,
            'hasStringArg': self.hasStringArg,
            'negate': self.negate
        }

    def setFromDict(self, theDict):
        self.uniqueFilterLabel = theDict['uniqueFilterLabel']
        self.active = theDict['active']
        self.args = theDict['args']
        self.hasStringArg = theDict['hasStringArg']
        self.negate = theDict['negate']