Changelog
=========


1.3.1 (2019/04)
---------------

Fixes:

* Fix ``OSError`` exception conversion in ``copy`` and ``copyfile``.
* Azure: Fix error when trying to read an existing blog due to bad name
  handling.
* Azure: Fix query string in blob object path because not removed from URL.

1.3.0 (2019/03)
---------------

Add support for following cloud storage:

* Microsoft Azure Blobs Storage
* Microsoft Azure Files Storage

Improvements:

* ``io.RawIOBase`` can now be used for storage that supports random write access.
* OSS: Copy objects between OSS buckets without copying data on the client when
  possible.

Deprecations:

* Warn about Python 3.4 deprecation in next version.

Fixes:

* Fix unsupported operation not risen in all cases with raw and buffered IO.
* Fix call of ``flush()`` in buffered IO.
* Fix file methods not translate cloud storage exception into ``OSError``.
* Fix file not create on open in write mode (Was only created on flush).
* Fix file closed twice when using context manager.
* Fix root URL detection in some cases.
* Fix too many returned result when listing objects with a count limit.
* Fix error when trying to append on a not existing file.
* Fix ``io.RawIOBase`` not generating padding when seeking after the end of the file.
* OSS: Fix error when listing objects in a not existing directory.
* OSS: Fix read error if try to read after the end of the file.
* OSS: Fix buffered write minimum buffer size.
* OSS: Clean up multipart upload parts on failed uploads.
* OSS: Fix error when opening an existing file in 'a' mode.
* S3: Fix error when creating a bucket due to an unspecified region.
* S3: Fix unprocessed error in listing bucket content of a not existing bucket.
* S3: Clean up multipart upload parts on failed uploads.
* S3: Fix missing transfer acceleration endpoints.
* Swift: Fix error when opening an existing file in 'a' mode.

1.2.0 (2018/10)
---------------

New standard library equivalent functions:

* ``os.listdir``, ``os.lstat``, ``os.remove``, ``os.rmdir``, ``os.scandir``,
  ``os.stat``, ``os.unlink``, ``os.path.getctime``, ``os.path.islink``,
  ``shutil.copyfile``.

Improvements:

* Copy of objects from and to the same storage is performed directly on remote
  server if possible.
* Pycosio now raises ``io.UnsupportedOperation`` if an operation is not
  compatible with the current storage, this applies to all newly created function
  and following existing functions: ``getsize``,  ``getmtime``, ``mkdir``.

Fixes:

* ``io.BufferedIOBase.read`` now returns empty bytes instead of raising
  exception when trying to read if seek already at end of the file.
* ``copy`` destination can now be a storage directory and not only a local
  directory.
* ``copy`` now checks if destination parent directory exists and if files
  are not the same file and raise proper exceptions.
* ``mkdir``: missing ``dir_fd`` argument.
* ``isdir`` now correctly handle "virtual" directories (Directory that don't
  exist as a proper object, but exists in another object path).

1.1.0 (2018/10)
---------------

New standard library equivalent functions:

* ``os.path.exists``, ``os.path.isabs``, ``os.path.isdir``, ``os.path.ismount``,
  ``os.path.samefile``, ``os.path.splitdrive``, ``os.makedirs``, ``os.mkdir``.

Backward incompatible change:

* ``mount`` argument ``extra_url_prefix`` is renamed to more relevant and
  clearer ``extra_root``.

Improvements:

* No buffer copy when using ``io.BufferedIOBase.read`` with exactly
  buffer size. This may lead to performance improvement.
* Minimum packages versions are set in setup based on packages changelog or
  date.

Fixes:

* ``isfile`` now correctly returns ``False`` when used on a directory.
* ``relpath`` now keeps ending ``/`` on cloud storage path (Directory marker).

1.0.0 (2018/08)
---------------

The first version that implements the core machinery.

Provides cloud storage equivalent functions of:

* ``open`` / ``io.open``, ``shutil.copy``, ``os.path.getmtime``,
  ``os.path.getsize``, ``os.path.isfile``, ``os.path.relpath``.

Provide cloud objects abstract classes with the following interfaces:

* ``io.RawIOBase``, ``io.BufferedIOBase``.

Add support for following cloud storage:

* Alibaba Cloud OSS
* AWS S3
* OpenStack Swift

Add read-only generic HTTP/HTTPS objects support.

Known issues
------------

* Append mode doesn't work with ``ObjectBufferedIOBase``.
