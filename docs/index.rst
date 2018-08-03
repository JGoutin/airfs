Pycosio (Python Cloud Object Storage I/O)
=========================================

Pycosio bring standard Python I/O to cloud objects by providing:

* Cloud objects classes with standards full featured ``io.RawIOBase`` and
  ``io.BufferedIOBase`` interfaces.
* Standard library functions equivalent to handle cloud objects and local files transparently:
  ``open``, ``copy``, ``getmtime``, ``getsize``, ``isfile``, ``listdir``, ``relpath``

Buffered cloud objects also support following features:

* Buffered asynchronous writing of object of any size.
* Buffered asynchronous preloading in read mode.
* Blocking write or read based on memory usage limitation.
* Bandwidth optimization using parallels connections.

Example of code:

.. code-block:: python

    import pycosio

    # Open an object on AWS S3 as text for reading
    with pycosio.open('s3://my_bucket/text.txt', 'rt') as file:
        text = file.read()

    # Open an object on AWS S3 as binary for writing
    with pycosio.open('s3://my_bucket/data.bin', 'wb') as file:
        file.write(b'binary_data')

    # Copy file from local file system to OpenStack Swift
    pycosio.copy('my_file', 'https://objects.mycloud.com/v1/12345678912345/my_container/my_file')

    # Get size of a file over internet
    pycosio.getsize('https://example.org/file')
    >>> 956

Supported Cloud storage
-----------------------

Pycosio is compatible with following cloud objects storage services:

* Amazon Web Services S3
* OpenStack Swift

Pycosio can also access to any file publicly available over HTTP/HTTPS (Read only)

Limitations
-----------

Due to their nature, objects storage are not seekable in write mode.
A cloud object must be writen from the beginning and in one time.

In read mode, there is no limitation and object support full random access.

Installation
------------

Supported Python versions: 2.7, 3.4, 3.5, 3.6, 3.7

Installation is performed using PIP:

.. code-block:: bash

    pip install pycosio

All mandatory dependencies are automatically installed.
You can also install these optional extras:

-  ``all``: Install all extras.
-  ``http``: HTTP/HTTPS files support.
-  ``s3``: AWS S3 support.
-  ``swift``: OpenStack Swift support.

Example for installing Pycosio with all dependencies:

.. code-block:: bash

    pip install pycosio[all]

Example for installing with support only for HTTP and OpenStack Swift:

.. code-block:: bash

    pip install pycosio[swift,http]

.. toctree::
   :maxdepth: 2
   :caption: User Documentation
   
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
