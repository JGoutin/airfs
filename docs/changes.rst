Changelog
=========

1.2.0 (2018/11)
---------------

New standard library equivalent functions:

* ``shutil.copyfile``, ``os.remove`` / ``os.unlink``, ``os.rmdir``,
  ``os.listdir``, ``os.path.getctime``, ``os.stat``, ``os.lstat``.

Improvements

* Copy of objects from and to a same storage is performed directly on remote
  server if possible.

Fixes:

* ``copy`` destination can now be a storage directory and not only a local
  directory.
* ``copy`` now checks if destination parent directory exists and if files
  are not same file and raise proper exceptions.
* ``mkdir`` on HTTP storage now raises ``io.UnsupportedOperation``.
* ``mkdir``: missing ``dir_fd`` argument.
* ``getsize`` and ``getmtime`` now raises ``io.UnsupportedOperation`` if
  information not available.

1.1.0 (2018/10)
---------------

New standard library equivalent functions:

* ``os.path.exists``, ``os.path.isabs``, ``os.path.isdir``, ``os.path.ismount``,
  ``os.path.samefile``, ``os.path.splitdrive``, ``os.makedirs``, ``os.mkdir``.

Backward incompatible change:

* ``mount`` argument ``extra_url_prefix`` is renamed to more relevant and
  clearer ``extra_root``.

Improvements

* No buffer copy when using ``io.BufferedIOBase.read`` with exactly
  buffer size. This may lead performance improvement.
* Minimum packages versions are set in setup based on packages changelog or
  date.

Fixes:

* ``isfile`` now correctly returns ``False`` when used on directory.
* ``relpath`` now keeps ending ``/`` on cloud storage path (Directory marker).

1.0.0 (2018/08)
---------------

First version that implement the core machinery.

Provides cloud storage equivalent functions of:

* ``open`` / ``io.open``, ``shutil.copy``, ``os.path.getmtime``,
  ``os.path.getsize``, ``os.path.isfile``, ``os.path.relpath``.

Provides cloud objects abstract classes with following interfaces:

* ``io.RawIOBase``, ``io.BufferedIOBase``.

Adds support for following cloud storage:

* Alibaba Cloud OSS
* AWS S3
* OpenStack Swift

Adds read only generic HTTP/HTTPS objects support.

Possibles futures features
--------------------------

* Equivalent functions of:
    * ``os.removedirs``, ``os.scandir``,
      ``os.walk``, ``os.rename``, ``os.renames``, ``os.replace``,
      ``shutil.move``, ``os.chmod``, ``shutil.copytree``, ``shutil.rmtree``.
* ``mode`` support in ``makedirs`` and ``mkdir``.
* More cloud storage.
* Extra ``max_buffers`` for swap on local disk in ``ObjectBufferedIOBase``.
* Global computer resource managements for cloud object IO.
* For buckets, add a checks based on root + bucket name to find the more
  relevant mounted storage to use
  (Example: User's storage or public one using same root)
