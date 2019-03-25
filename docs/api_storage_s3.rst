pycosio.storage.s3
==================

Amazon Web Service S3

Mount
-----

AWS S3 can be mounted using the Pycosio ``mount`` function.

``storage_parameters`` contain a sub-directory ``session`` that await
arguments to pass to ``boto3.session.Session(**session_parameters)`` from the
``boto3`` Python library.

It can also include a sub-directory ``client`` that is used to pass arguments to
``boto3.session.Session.client('s3', **client_parameters)``.

This example show the mount of S3 with the minimal configuration:

.. code-block:: python

    import pycosio

    # Mount S3 manually (Minimal configuration)
    pycosio.mount(storage='s3', storage_parameters=dict(
            # "boto3.session.Session" arguments
            session=dict(
                aws_access_key_id='my_access_key',
                aws_secret_access_key='my_secret_key',
                region_name='my_region_name'
            )
        )
    )

    # Call of pycosio on a S3 object.
    with pycosio.open('s3://my_bucket/my_object', 'rt') as file:
        text = file.read()

Automatic mount
~~~~~~~~~~~~~~~

It is not required to mount S3 explicitly When using pycosio on a host
configured to handle AWS S3 access (Through IAM policy, configuration
files, environment variables, ...).

In this case, mounting is done transparently on the first call of a Pycosio
function on a S3 object and no configuration or extra steps are required:

.. code-block:: python

    import pycosio

    # Call of pycosio on a S3 object: Mounting is done transparently.
    with pycosio.open('s3://my_bucket/my_bucket', 'rt') as file:
        text = file.read()

Limitation
~~~~~~~~~~

Only one S3 configuration can be mounted simultaneously.

Files objects classes
---------------------

.. automodule:: pycosio.storage.s3
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
