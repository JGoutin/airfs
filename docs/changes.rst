Changelog
=========

1.0.0 (2018/08)
---------------

First version that implement the core machinery.

Provides cloud storage equivalent functions of:

* ``open`` / ``io.open``, ``shutil.copy``, ``os.path.getmtime``,
  ``os.path.getsize``, ``os.path.isfile``, ``os.path.relpath``.

Provides cloud objects abstract classes with following interfaces:

* ``io.RawIOBase``, ``io.BufferedIOBase``.

Adds support for following cloud storage:

* AWS S3
* OpenStack Swift

Adds read only generic HTTP/HTTPS objects support.

Possibles futures features
--------------------------

* Equivalent functions of:
    * ``os.listdir``, ``os.remove``, ``os.scandir``, ``os.stat``, ``os.walk``.
* More cloud storage.
* Extra ``max_buffers`` for swap on local disk in ``ObjectBufferedIOBase``.
* Global computer resource managements for cloud object IO.

Known issues
------------

* On Python 2, using ``pycosio.io.ObjectBufferedIOBase`` with
  ``workers_type='process'`` don't work. This will not be fixed,
  the *futures* package recommends to not use ``ProcessPoolExecutor`` on
  Python 2.
* Using ``pycosio.storage.s3.S3BufferedIO`` with ``workers_type='process'``
  in write mode may raises ``NoSuchUpload`` boto3 exception.
