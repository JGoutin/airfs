airfs: A Python library for cloud and remote file Systems
=========================================================

airfs brings standard Python I/O to various storage (like cloud objects storage, remote
file-systems, ...) by providing:

* Abstract classes of Cloud objects with the complete ``io.RawIOBase`` and
  ``io.BufferedIOBase`` standard interfaces.
* Features equivalent to the standard library (``io``, ``os``, ``os.path``, ``shutil``)
  for seamlessly managing storage objects and local files.

These functions are source agnostic and always provide the same interface for all files
from storage or local file systems.

Buffered storage objects also support the following features:

* Buffered asynchronous writing of any object size.
* Buffered asynchronous preloading in reading mode.
* Write or read lock depending on memory usage limitation.
* Maximization of bandwidth using parallels connections.

Example of code:

.. code-block:: python

    import airfs

    # Open an object on AWS S3 as text for reading
    with airfs.open('s3://my_bucket/my_object.txt', 'rt') as file:
        text = file.read()

    # Open an object on AWS S3 as binary for writing
    with airfs.open('s3://my_bucket/my_object.bin', 'wb') as file:
        file.write(b'binary_data')

    # Copy file from the local file system to OpenStack Swift
    airfs.copy(
        'my_file',
        'https://objects.my_cloud.com/v1/12345678912345/my_container/my_object'
        )

    # Get size of a file over internet
    airfs.getsize('https://www.example.org/any_object')
    >>> 956

Supported storage
-----------------

airfs is compatible with the following storage services:

* Alibaba Cloud OSS
* Amazon Web Services S3 (and compatible)
* GitHub (Read Only)
* Microsoft Azure Blobs Storage
* Microsoft Azure Files Storage
* OpenStack Swift / Object Store

airfs can also access any publicly accessible file via HTTP/HTTPS (Read only).

Limitations
-----------

All storage are not real file systems and may have the following limitations (
Depending on the selected storage):

- Storage objects may not be seekable in write mode.
- Storage objects may be written entirely at once.
- Storage objects may not be locked when accessed.
- Storage object attributes available may be more limited.
- Some file-system features like symbolic links may ne be present

.. toctree::
   :maxdepth: 2
   :caption: User Documentation

   getting_started
   api
   changes

.. toctree::
   :maxdepth: 2
   :caption: Links

   airfs on GitHub <https://github.com/JGoutin/airfs>
   airfs on PyPI <https://pypi.org/project/airfs>

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
