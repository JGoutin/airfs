Getting Started
===============

Installation
------------

Supported Python versions: 2.7, 3.4, 3.5, 3.6, 3.7

Installation is performed using PIP:

.. code-block:: bash

    pip install pycosio

All mandatory dependencies are automatically installed.
You can also install these optional extras:

* ``all``: Install all extras.
* ``oss``: Alibaba Cloud OSS support.
* ``s3``: AWS S3 support.
* ``swift``: OpenStack Swift support.

Example for installing Pycosio with all dependencies:

.. code-block:: bash

    pip install pycosio[all]

Example for installing with support only for HTTP and OpenStack Swift:

.. code-block:: bash

    pip install pycosio[swift,http]

Standard Python equivalents functions
-------------------------------------

Pycosio provides functions that use the same prototype as their equivalent of
the standard Python library. These functions are used exactly like the original
function but support URLs of cloud objects in addition to local paths.

Pycosio natively recognizes the URL/path with the following formats:

* Local path, or URL with the``file`` scheme:
  ``file:///home/user/my_file`` or ``/home/user/my_file``.
* Configured cloud storage domains (See below for storage configuration):
  ``https://objects.my_cloud.com/v1/12345678912345/my_container/my_object``.
* Cloud storage URL with specific schemes:
  ``s3://my_bucket/my_object``.
* Others classic HTTP/HTTPS URLs (That are not cloud domains):
  ``http://www.example.org/any_object`` or
  ``https://www.example.org/any_object``.

All available functions are in the ``pycosio`` namespace.

Examples of functions:

.. code-block:: python

    import pycosio

    # Open a cloud object in text mode
    with pycosio.open('https://my_cloud.com/object', 'rt') as file:
        text = file.read()

    # Copy a cloud object to local
    pycosio.copy(
        'https://my_cloud.com/object', '/home/user/local_object')

    # Get size of a cloud object
    pycosio.getsize('https://my_cloud.com/object')
    >>> 956

Cloud storage configuration
---------------------------

Like with file systems, Cloud storage need to be *mounted* to be used.

Some storage requires configuration before use (such as user access keys).
For the required parameter detail, see the targeted storage class or the
targeted storage documentation.

Storage that does not require configuration are automatically mounted.

All storage parameters must be defined in a ``storage_parameters`` dictionary.
This dictionary must be transmitted either to the ``pycosio.mount`` function
or when a file is opened using the ``pycosio.open`` function.

Once mounted, all functions can be used without the needs to pass
the ``storage_parameters`` dictionary.

``my_cloud`` storage is mounted as follows:

**With ``mount`` function:**

.. code-block:: python

    import pycosio

    # "storage_parameters" is the cloud storage configuration
    storage_parameters = dict(
        client_id='my_client_id', secret_id='my_secret_id')

    # Mount "my_cloud" storage with "mount" function
    pycosio.mount(
        storage='my_cloud', storage_parameters=storage_parameters)

    # _Storage files can now be used transparently
    with pycosio.open('https://my_cloud.com/object', 'rt') as file:
        file.read()

**On first cloud object open:**

.. code-block:: python

    import pycosio

    storage_parameters = dict(
        client_id='my_client_id', secret_id='my_secret_id')

    # The storage is mounted on first use by passing "storage_parameters"
    with pycosio.open('https://my_cloud.com/my_object', 'rt',
                      storage='my_cloud',
                      storage_parameters=storage_parameters) as file:
        file.read()

    # Next calls uses mounted storage transparently
    with pycosio.open(
            'https://my_cloud.com/my_other_object', 'rt') as file:
        file.read()
