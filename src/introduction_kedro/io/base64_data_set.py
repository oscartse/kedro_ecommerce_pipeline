from base64 import b64encode
from kedro.io.core import AbstractDataSet, DataSetError


class Base64DataSet(AbstractDataSet):
    def __init__(self, filepath): 
        self.filepath = filepath

    def _save(self, binary_data):
        with open(str(self.filepath), "w") as f:
            f.write(b64encode(binary_data).decode("utf-8"))

    def _load(self):
        raise DataSetError("Write Only DataSet")

    def _describe(self):
        return dict(filepath=self.filepath)
