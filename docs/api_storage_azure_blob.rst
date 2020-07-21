airfs.storage.azure_blob
========================

Microsoft Azure Storage Blob

.. versionadded:: 1.3.0

Mount
-----

An Azure storage account can be mounted using the airfs ``mount`` function.

``storage_parameters`` await arguments to pass to the
``azure.storage.blob.baseblobservice.BaseBlobService`` class from
``azure-storage-blob`` Python library.

This example shows the mount of Azure Storage Blob with the minimal configuration:

.. code-block:: python

    import airfs

    # Mount Azure Storage Blob manually (Minimal configuration)
    airfs.mount(storage='azure_blob', storage_parameters=dict(
            account_name='my_account_name',
            account_key='my_account_key'
        )
    )

    # Call of airfs on an Azure Storage blob.
    with airfs.open(
            'https://my_account.blob.core.windows.net/my_container/my_blob',
            'rt'
            ) as file:
        text = file.read()

If using multiple Azure storage accounts simultaneously, the ``sas_token`` argument of
the ``BaseBlobService`` class is required to allow blob and files copies across
different accounts.

It is possible to mount Azure Storage Blob and Azure Storage File with a single
``airfs.mount`` call by using ``storage='azure'`` instead of ``storage='azure_blob'``.

Limitation
~~~~~~~~~~

Only one configuration per Azure Storage account can be mounted simultaneously.

Azure blob type selection
-------------------------

It is possible to select the blob type for new files created using the ``blob_type``
argument.

Possible blob types are ``BlockBlob``, ``AppendBlob`` & ``PageBlob``.

The default blob type can be set when mounting the storage
(if not specified, the ``BlockBlob`` type is used by default):

.. code-block:: python

    import airfs

    airfs.mount(
        storage='azure_blob',
        storage_parameters=dict(
            account_name='my_account_name',
            account_key='my_account_key',
            blob_type='PageBlob',  # Using PageBlob by default for new files
        )
    )

It can also be selected for a specific file when opening it in write mode:

.. code-block:: python

    # Open a new file in write mode as PageBlob
    with airfs.open(
            'https://my_account.blob.core.windows.net/my_container/my_blob',
            'wb',
            blob_type='PageBlob'
            ) as file:
        file.write(b'0')

Page blobs specific features
----------------------------

The page blob supports the following specific features.

Preallocating pages
~~~~~~~~~~~~~~~~~~~

When flushing a page blob out of its current size, airfs first resize the blob to allow
the flush of the new data.

In case of multiple flushes on a raw IO or when using a buffered IO, this is done with
extra requests to the Azure server. If The size to write is known before opening the
file, it is possible to avoid these extra requests by to preallocate the required size
in only one initial request.

The ``content_length`` argument allow preallocating a Page blob to a defined size when
opening it in write mode:

.. code-block:: python

    # Open a new page blob and preallocate it with 1024 bytes.
    with airfs.open(
            'https://my_account.blob.core.windows.net/my_container/my_blob',
            'wb',
            blob_type='PageBlob',
            content_length=1024
            ) as file:
        file.write(b'1')

    # Append on an existing page blob and pre-resize it to 2048 bytes.
    with airfs.open(
            'https://my_account.blob.core.windows.net/my_container/my_blob',
            'ab',
            blob_type='PageBlob',
            content_length=2048
            ) as file:
        file.write(b'1')

The preallocation is done with padding of null characters (``b'\0'``).

End page padding handling
~~~~~~~~~~~~~~~~~~~~~~~~~

By default, airfs tries to handle page blobs like standard files by ignoring trailing
page padding of null characters:

* When opening a file in append mode (Seek to the end of file after ignoring trailing
  padding of the last page).
* When reading data (Read until a null character reaches).
* When using the ``seek()`` method with ``whence=os.SEEK_END`` (Ignore the trailing
  padding when determining the end of the file to use as the reference position)

This behaviour can be disabled using the ``ignore_padding=False`` argument when opening
the page blob.

Files objects classes
---------------------

.. automodule:: airfs.storage.azure_blob
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
