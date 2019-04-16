import os
import logging
from das_framework.driver import AbstractDASTakedown
class takedown(AbstractDASTakedown):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    def takedown(self):
        return True
    def removeWrittenData(self, reference):
        return True
