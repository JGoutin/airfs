airfs: A Python library for cloud and remote file Systems
=========================================================

airfs brings standard Python I/O to cloud objects by providing:

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
        'https://objects.my_cloud.com/v1/12345678912345/my_container/my_object')

    # Get size of a file over internet
    airfs.getsize('https://www.example.org/any_object')
    >>> 956

Supported Cloud storage
-----------------------

airfs is compatible with the following cloud objects storage services:

* Alibaba Cloud OSS
* Amazon Web Services S3
* Google Cloud Storage
* Microsoft Azure Blobs Storage
* Microsoft Azure Files Storage
* OpenStack Swift

airfs can also access any publicly accessible file via HTTP/HTTPS
(Read only).

Limitations
-----------

Cloud object storage is not file systems and has the following limitations:

- Cloud objects are not seekable in write mode.
- Cloud objects must be written entirely at once.
- Cloud objects are not locked when accessed.
- The cloud object attributes available are more limited.

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
