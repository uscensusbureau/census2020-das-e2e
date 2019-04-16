# stub writer

from driver import AbstractDASWriter
class writer(AbstractDASWriter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def write(self, mdf):
        return mdf
