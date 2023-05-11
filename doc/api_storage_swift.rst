airfs.storage.swift
===================

OpenStack Swift / Object Store

.. versionadded:: 1.0.0

Mount
-----

An OpenStack Swift project can be mounted using the airfs ``mount`` function.

``storage_parameters`` await arguments to pass to the
``swiftclient.client.Connection`` class from ``python-swiftclient`` Python library.

This example shows the mount of OpenStack Swift with a minimal configuration:

.. code-block:: python

    import airfs

    # Mount OpenStack Swift manually (Minimal configuration)
    airfs.mount(
        storage='swift',
        storage_parameters=dict(
            authurl='my_auth_url',
            user='my_user',
            key='my_key',
            auth_version='3',
            os_options=dict(
                region_name='my_region',
                project_id='my_project'
            )
        )
    )

    # Call of airfs on an OpenStack Swift object.
    with airfs.open(
            'https://objects.my_cloud.com/v1/my_project/my_container/my_object',
            'rt'
            ) as file:
        text = file.read()

Allow shareable URL support
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use the airfs shareable URL feature (`airfs.shareable_url`), a specific
`Temp-URL-Key` secret key must be configured on the storage (See the
`Temporary URL OpenStack documentation <https://docs.openstack.org/swift/latest/api/temporary_url_middleware.html#secret-keys>`_
for more information).

This key also required to be passed to airfs as the `temp_url_key` storage parameters:

.. code-block:: python

    import airfs

    airfs.mount(
        storage='swift',
        storage_parameters=dict(
            authurl='my_auth_url',
            user='my_user',
            key='my_key',
            auth_version='3',
            os_options=dict(
                region_name='my_region',
                project_id='my_project'
            ),
            # Pass temporary URL secret key
            temp_url_key="my_temp_url_key"
        )
    )

Limitation
~~~~~~~~~~

Only one configuration per OpenStack project can be mounted simultaneously.

Files objects classes
---------------------

.. automodule:: airfs.storage.swift
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
