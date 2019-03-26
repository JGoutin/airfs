pycosio.storage.azure_blob
==========================

Microsoft Azure Storage Blob

Mount
-----

An Azure storage account can be mounted using the Pycosio ``mount`` function.

``storage_parameters`` await arguments to pass to the
``azure.storage.blob.baseblobservice.BaseBlobService`` class from
``azure-storage-blob`` Python library.

This example show the mount of Azure Storage Blob with the minimal
configuration:

.. code-block:: python

    import pycosio

    # Mount Azure Storage Blob manually (Minimal configuration)
    pycosio.mount(storage='azure_blob', storage_parameters=dict(
            account_name='my_account_name',
            account_key='my_account_key'
        )
    )

    # Call of pycosio on an Azure Storage blob.
    with pycosio.open(
            'https://my_account.blob.core.windows.net/my_container/my_blob',
            'rt') as file:
        text = file.read()

If using multiple Azure storage accounts simultaneously, the ``sas_token``
argument of the ``BaseBlobService`` class is required to allow blob and files
copies across different accounts.

It is possible to mount Azure Storage Blob and Azure Storage File with a single
``pycosio.mount`` call by using ``storage='azure'`` instead of
``storage='azure_blob'``.

Limitation
~~~~~~~~~~

Only one configuration per Azure Storage account can be mounted simultaneously.

Files objects classes
---------------------

.. automodule:: pycosio.storage.azure_blob
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
