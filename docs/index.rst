Pycosio (Python Cloud Object Storage I/O)
=========================================

Pycosio brings standard Python I/O to cloud objects by providing:

* Abstract classes of Cloud objects with the complete ``io.RawIOBase`` and
  ``io.BufferedIOBase`` standard interfaces.
* Features equivalent to the standard library (``io``, ``os``, ``os.path``,
  ``shutil``) for seamlessly managing cloud objects and local files.

Theses functions are source agnostic and always provide the same interface for
all files from cloud storage or local file systems.

Buffered cloud objects also support following features:

* Buffered asynchronous writing of any object size.
* Buffered asynchronous preloading in read mode.
* Write or read lock depending on memory usage limitation.
* Maximization of bandwidth using parallels connections.

Example of code:

.. code-block:: python

    import pycosio

    # Open an object on AWS S3 as text for reading
    with pycosio.open('s3://my_bucket/my_object.txt', 'rt') as file:
        text = file.read()

    # Open an object on AWS S3 as binary for writing
    with pycosio.open('s3://my_bucket/my_object.bin', 'wb') as file:
        file.write(b'binary_data')

    # Copy file from local file system to OpenStack Swift
    pycosio.copy(
        'my_file',
        'https://objects.my_cloud.com/v1/12345678912345/my_container/my_object')

    # Get size of a file over internet
    pycosio.getsize('https://www.example.org/any_object')
    >>> 956

Supported Cloud storage
-----------------------

Pycosio is compatible with following cloud objects storage services:

* Alibaba Cloud OSS
* Amazon Web Services S3
* OpenStack Swift

Pycosio can also access any publicly accessible file via HTTP/HTTPS
(Read only).

Limitations
-----------

Cloud object storage are not file systems and have following limitations:

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

   Pycosio on GitHub <https://github.com/Accelize/pycosio>
   Pycosio on PyPI <https://pypi.org/project/pycosio>

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
