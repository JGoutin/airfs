airfs.storage.oss
=================

Alibaba Cloud OSS

.. versionadded:: 1.0.0

Mount
-----

OSS can be mounted using the airfs ``mount`` function.

``storage_parameters`` await arguments to pass to
``oss2.Auth``, ``oss2.AnonymousAuth`` or ``oss2.StsAuth`` classes from
``oss2`` Python library (The class selection is done automatically based on parameters
found in ``storage_parameters``).

airfs also requires one extra argument, the ``endpoint``, which is basically the URL of
the OSS Alibaba region to use. (See ``endpoint`` argument of the ``oss2.Bucket`` class)

This example shows the mount of OSS with the minimal configuration:

.. code-block:: python

    import airfs

    # Mount OSS manually (Minimal configuration)
    airfs.mount(
        storage='oss',
        storage_parameters=dict(
            access_key_id='my_access_key_id',
            access_key_secret='my_access_key_secret',
            endpoint='http://oss-my_region.aliyuncs.com
        )
    )

    # Call of airfs on an OSS object.
    with airfs.open('oss://my_bucket/my_object', 'rt') as file:
        text = file.read()

Limitation
~~~~~~~~~~

Only one OSS configuration can be mounted simultaneously.

Files objects classes
---------------------

.. automodule:: airfs.storage.oss
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
