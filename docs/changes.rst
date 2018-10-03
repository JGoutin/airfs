Changelog
=========

1.1.0 (2018/10)
---------------

New standard library equivalent functions:

* ``os.path.isabs``, ``os.path.ismount``, ``os.path.samefile``,
  ``os.path.splitdrive``.

Backward incompatible change:

* ``mount`` argument ``extra_url_prefix`` is renamed to more relevant and
  clearer ``extra_root``.

Improvements

* No buffer copy when using ``io.BufferedIOBase.read`` with exactly
  buffer size. This may lead performance improvement.
* Minimum packages versions are set in setup based on packages changelog or
  date.
* Minor fixes.

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
    * ``os.listdir``, ``os.remove`` / ``os.unlink```,
      ``os.rmdir``, ``os.removedirs``, ``os.scandir``, ``os.stat``, ``os.walk``,
      ``os.rename``, ``os.renames``, ``os.replace``, ``shutil.move``,
      ``os.chmod``, ``os.mkdir``, ``os.makedirs``, ``os.path.exists``,
      ``os.path.isdir``, ``shutil.copyfile``, ``shutil.copytree``,
      ``shutil.rmtree``.
* More cloud storage.
* Extra ``max_buffers`` for swap on local disk in ``ObjectBufferedIOBase``.
* Global computer resource managements for cloud object IO.
* Improves ``copy`` between two path in a same storage if a special function
  exists for this storage.
* For buckets, add a checks based on root + bucket name to find the more
  relevant mounted storage to use
  (Example: User's storage or public one using same root)
