# coding=utf-8
"""Microsoft Azure Blobs Storage"""

# Generic classes
from pycosio.storage.azure_blob._system import _AzureBlobSystem
from pycosio.storage.azure_blob._base_blob import (
    AzureBlobRawIO, AzureBlobBufferedIO)

# Specific blob types classes
from pycosio.storage.azure_blob._append_blob import (
    AzureAppendBlobRawIO, AzureAppendBlobBufferedIO)
from pycosio.storage.azure_blob._block_blob import (
    AzureBlockBlobRawIO, AzureBlockBlobBufferedIO)
from pycosio.storage.azure_blob._page_blob import (
    AzurePageBlobRawIO, AzurePageBlobBufferedIO)

__all__ = ['AzureBlobRawIO', 'AzureBlobBufferedIO',
           'AzureAppendBlobRawIO', 'AzureAppendBlobBufferedIO',
           'AzureBlockBlobRawIO', 'AzureBlockBlobBufferedIO',
           'AzurePageBlobRawIO', 'AzurePageBlobBufferedIO']

# Makes cleaner namespace
for _name in __all__:
    locals()[_name].__module__ = __name__
del _name
