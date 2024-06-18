from enum import Enum


class CompressionType(Enum):
    Uncompressed = ("", True, False)
    Zip = ("zip", True, False)
    GZip = ("gz", True, True)

    def __init__(self, type: str, compressible: bool, compressed: bool):
        self.type = type
        self.compressible = compressible
        self.compressed = compressed
