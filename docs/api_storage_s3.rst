airfs.storage.s3
================

Amazon Web Service S3

.. versionadded:: 1.0.0

Mount
-----

AWS S3 can be mounted using the airfs ``mount`` function.

``storage_parameters`` contain a sub-directory ``session`` that await arguments to pass
to ``boto3.session.Session(**session_parameters)`` from the ``boto3`` Python library.

It can also include a sub-directory ``client`` that is used to pass arguments to
``boto3.session.Session.client('s3', **client_parameters)``.

This example shows the mount of S3 with the minimal configuration:

.. code-block:: python

    import airfs

    # Mount S3 manually (Minimal configuration)
    airfs.mount(
        storage='s3',
        storage_parameters=dict(
            # "boto3.session.Session" arguments
            session=dict(
                aws_access_key_id='my_access_key',
                aws_secret_access_key='my_secret_key',
                region_name='my_region_name'
            )
        )
    )

    # Call of airfs on a S3 object.
    with airfs.open('s3://my_bucket/my_object', 'rt') as file:
        text = file.read()

Automatic mount
~~~~~~~~~~~~~~~

It is not required to mount S3 explicitly when using airfs on a host configured to
handle AWS S3 access (Through IAM policy, configuration files, environment variables,
...).

In this case, mounting is done transparently on the first call of a airfs function on
an S3 object and no configuration or extra steps are required:

.. code-block:: python

    import airfs

    # Call of airfs on a S3 object: Mounting is done transparently.
    with airfs.open('s3://my_bucket/my_bucket', 'rt') as file:
        text = file.read()

Limitation
~~~~~~~~~~

Only one S3 configuration can be mounted simultaneously.

Files objects classes
---------------------

.. automodule:: airfs.storage.s3
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
