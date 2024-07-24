from enum import Enum


class CompressionType(Enum):
    Uncompressed = ("Uncompressed",)
    GZip = ("GZip",)
    Zip = "Zip"
