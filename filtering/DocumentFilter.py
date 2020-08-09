class DocumentFilter(object):
    def __init__(self, filterFunction, uniqueFilterLabel : str, active : bool = False, hasStringArg : bool = False) -> None:
        super().__init__()

        self.filterFunction = filterFunction
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