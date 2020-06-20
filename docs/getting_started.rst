Getting Started
===============

Installation
------------

Supported Python versions: 3.5, 3.6, 3.7, 3.8

Python >= 3.6 is recommended to provide fully featured local file support.

Installation is performed using PIP:

.. code-block:: bash

    pip install airfs

All mandatory dependencies are automatically installed.
You can also install these optional extras:

* ``all``: Install all extras.
* ``azure_blob``: Microsoft Azure Blob Storage support.
* ``azure_file``: Microsoft Azure File Storage support.
* ``oss``: Alibaba Cloud OSS support.
* ``s3``: Amazon Web Services S3 support.
* ``swift``: OpenStack Swift support.

Example of installing airfs with all dependencies:

.. code-block:: bash

    pip install airfs[all]

Example for installing with support only for S3 and OpenStack Swift:

.. code-block:: bash

    pip install airfs[swift,s3]

Standard Python equivalents functions
-------------------------------------

airfs provides functions that use the same prototype as their equivalent of the standard
Python library. These functions are used exactly like the original function but support
URLs of storage objects in addition to local paths.

airfs natively recognizes the URL/path with the following formats:

* Local path, or URL with the``file`` scheme:
  ``file:///home/user/my_file`` or ``/home/user/my_file``.
* Configured storage domains (See below for storage configuration):
  ``https://objects.my_cloud.com/v1/12345678912345/my_container/my_object``.
* Cloud storage URL with specific schemes: ``s3://my_bucket/my_object``.
* Others classic HTTP/HTTPS URLs (That are not storage domains):
  ``http://www.example.org/any_object`` or ``https://www.example.org/any_object``.

All available functions are in the ``airfs`` namespace.

Examples of functions:

.. code-block:: python

    import airfs

    # Open a storage object in text mode
    with airfs.open('https://my_storage.com/object', 'rt') as file:
        text = file.read()

    # Copy a storage object to local
    airfs.copy(
        'https://my_storage.com/object',
        '/home/user/local_object'
        )

    # Get size of a storage object
    airfs.getsize('https://my_storage.com/object')
    >>> 956

Cloud storage configuration
---------------------------

Like with file systems, Cloud storage needs to be *mounted* to be used.

Some storage requires configuration before use (such as user access keys).
For the required parameter detail, see the targeted storage class or the targeted
storage documentation.

Storage that does not require configuration is automatically mounted.

All storage parameters must be defined in a ``storage_parameters`` dictionary.
This dictionary must be transmitted either to the ``airfs.mount`` function or when a
file is opened using the ``airfs.open`` function.

Once mounted, all functions can be used without the needs to pass the
``storage_parameters`` dictionary.

``my_storage`` storage is mounted as follows:

**With ``mount`` function:**

.. code-block:: python

    import airfs

    # "storage_parameters" is the storage configuration
    storage_parameters = dict(
        client_id='my_client_id',
        secret_id='my_secret_id'
        )

    # Mount "my_storage" storage with "mount" function
    airfs.mount(
        storage='my_storage',
        storage_parameters=storage_parameters
        )

    # _Storage files can now be used transparently
    with airfs.open('https://my_storage.com/object', 'rt') as file:
        file.read()

**On first storage object open:**

.. code-block:: python

    import airfs

    storage_parameters = dict(
        client_id='my_client_id', secret_id='my_secret_id')

    # The storage is mounted on first use by passing "storage_parameters"
    with airfs.open('https://my_storage.com/my_object', 'rt',
                      storage='my_storage',
                      storage_parameters=storage_parameters) as file:
        file.read()

    # Next calls use mounted storage transparently
    with airfs.open(
            'https://my_storage.com/my_other_object',
            'rt'
            ) as file:
        file.read()
