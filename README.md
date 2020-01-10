
:warning: **This project is cancelled and unmaintened**

Pycosio (Python Cloud Object Storage I/O)
=========================================

Pycosio brings standard Python I/O to cloud objects by providing:

* Abstract classes of Cloud objects with the complete ``io.RawIOBase`` and
  ``io.BufferedIOBase`` standard interfaces.
* Features equivalent to the standard library (``io``, ``os``, ``os.path``,
  ``shutil``) for seamlessly managing cloud objects and local files.

These functions are source agnostic and always provide the same interface for
all files from cloud storage or local file systems.

Buffered cloud objects also support the following features:

* Buffered asynchronous writing of any object size.
* Buffered asynchronous preloading in reading mode.
* Write or read lock depending on memory usage limitation.
* Maximization of bandwidth using parallels connections.

Supported Cloud storage
-----------------------

Pycosio is compatible with the following cloud objects storage services:

* Alibaba Cloud OSS
* Amazon Web Services S3
* Microsoft Azure Blobs Storage
* Microsoft Azure Files Storage
* OpenStack Swift

Pycosio can also access any publicly accessible file via HTTP/HTTPS
(Read only).
