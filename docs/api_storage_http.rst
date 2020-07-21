airfs.storage.http
==================

HTTP/HTTPS object read-only access.

.. versionadded:: 1.0.0

Mount
-----

The HTTP storage does not require to be mounted prior to being used.

The function can be used directly on any HTTP object reachable by the airfs host:

.. code-block:: python

    import airfs

    # Call of airfs on a object available thought HTTP over internet.
    with airfs.open('https://my_domaine.com/my_object', 'rt') as file:
        text = file.read()

Files objects classes
---------------------

.. automodule:: airfs.storage.http
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
