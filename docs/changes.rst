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
      ``os.path.isdir``, ``os.path.isabs``, ``os.path.ismount``,
      ``os.path.splitdrive``, ``os.path.samefile``, ``shutil.copyfile``,
      ``shutil.copytree``, ``shutil.rmtree``.
* More cloud storage.
* Extra ``max_buffers`` for swap on local disk in ``ObjectBufferedIOBase``.
* Global computer resource managements for cloud object IO.
* Improves ``copy`` between two path in a same storage if a special function
  exists for this storage.
