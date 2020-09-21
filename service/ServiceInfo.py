class ServiceInfo(object):
    def __init__(self, serviceDict: dict):
        super().__init__()

        self.serviceDict = serviceDict if serviceDict else dict()

    def get(self, key: str):
        return self.serviceDict.get(key)

    @property
    def active(self) -> bool:
        return self.get('active')

    @property
    def description(self) -> str:
        return self.get('description')

    @property
    def name(self) -> str:
        return self.get('name')

    @property
    def className(self) -> str:
        return self.get('className')

    @property
    def serviceInfoDict(self) -> dict:
        return self.get('serviceInfoDict')