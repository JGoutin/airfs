airfs.storage.azure_file
========================

Microsoft Azure Storage File

Mount
-----

An Azure storage account can be mounted using the airfs ``mount`` function.

``storage_parameters`` await arguments to pass to the
``azure.storage.file.fileservice.FileService`` class from
``azure-storage-file`` Python library.

This example shows the mount of Azure Storage File with the minimal configuration:

.. code-block:: python

    import airfs

    # Mount Azure Storage File manually (Minimal configuration)
    airfs.mount(storage='azure_file', storage_parameters=dict(
            account_name='my_account_name', account_key='my_account_key'
        )
    )

    # Call of airfs on an Azure Storage file.
    with airfs.open(
            '//my_account.file.core.windows.net/my_share/my_file', 'rt') as file:
        text = file.read()

If using multiple Azure storage accounts simultaneously, the ``sas_token`` argument of
the ``FileService`` class is required to allow blob and files copies across different
accounts.

It is possible to mount Azure Storage Blob and Azure Storage File with a single
``airfs.mount`` call by using ``storage='azure'`` instead of ``storage='azure_file'``.

Limitation
~~~~~~~~~~

Only one configuration per Azure Storage account can be mounted simultaneously.

Preallocating files
-------------------

When flushing a file out of its current size, airfs first resize the file to allow the
flush of the new data.

In case of multiple flushes on a raw IO or when using a buffered IO, this is done with
extra requests to the Azure server. If The size to write is known before opening the
file, it is possible to avoid these extra requests by to preallocate the required size
in only one initial request.

The ``content_length`` argument allow preallocating a file to a defined size when
opening it in write mode:

.. code-block:: python

    # Open a new file and preallocate it with 1024 bytes.
    with airfs.open(
            '//my_account.file.core.windows.net/my_share/my_file', 'wb',
            content_length=1024
            ) as file:
        file.write(b'1')

    # Append on an existing file and pre-resize it to 2048 bytes.
    with airfs.open(
            '//my_account.file.core.windows.net/my_share/my_file', 'ab',
            content_length=2048
            ) as file:
        file.write(b'1')

The preallocation is done with padding of null characters (``b'\0'``).


Files objects classes
---------------------

.. automodule:: airfs.storage.azure_file
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
