from enum import Enum

class LinkType(Enum):
    FILE = 0
    METADATA = 1

class Link:
    def __init__(self):
        type = LinkType.METADATA
        path = ""

class TestCaseStatus(Enum):
    APPROVED = 0
    PENDING_APPROVAL = 1
    DISAPPROVED = 2

class TestCase:
    def __init__(self):
        status = TestCaseStatus.PENDING_APPROVAL

class User:
    def __init__(self):
        name = ""

class Comment:
    def __init__(self):
        poster = ""
        text = ""

class Metadata:
    def __init__(self):
        link = Link()
        version = 0
        tags = []
        comments = []
        data_dict = dict()

def add_metadata():
    raise NotImplementedError()