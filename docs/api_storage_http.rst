pycosio.storage.http
====================

HTTP/HTTPS object read only access.

Mount
-----

The HTTP storage do not require to be mount prior to be used.

Function can be used direclty on any HTTP object reachable by the Pycosio host:

.. code-block:: python

    import pycosio

    # Call of pycosio on a object available thought HTTP over internet.
    with pycosio.open('https://my_domaine.com/my_object', 'rt') as file:
        text = file.read()

Files objects classes
---------------------

.. automodule:: pycosio.storage.http
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
