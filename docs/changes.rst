Changelog
=========

1.5.0 (2020/08)
---------------

New storage support:

* GitHub (Read only).

New standard library equivalent functions:

* ``os.readlink``.

New features:

* ``airfs.shareable_url`` function to get a shareable URL of an existing object.

Improvements:

* `airfs.stat` and `airfs.lstat` now returns proper GID and UID, with default to current
  user values for storage that does not support it.
* `airfs.stat` and `airfs.lstat` `st_mode` now include permission bits with default
  value to `644` for storage that does not support it.
* `airfs.stat` and `airfs.lstat` now returns extra storage specific values with their
  original names instead of `st_` prefixed names. To allow use as stat result attributes
  and ensure consistency, returned names are lower-cased, `-` are replaced with `_` and
  other characters are limited to alphanumerical characters.
* Add `airfs.os`, `airfs.os.path` and `airfs.shutil` namespaces that mimic standards
  library namespaces. Names that are not available in Airfs fall back to their standard
  library equivalent.
* Add `st_atime_ns`, `st_mtime_ns` and `st_ctime_ns`, in the `os.stat_result` like named
  tuple returned by `airfs.stat` and `airfs.lstat`.
* Use `Black <https://github.com/psf/black>`_ for code formatting.
* airfs can now lazily auto-import a storage from an URL using any root and not only a
  specific scheme. Only public storage that does not require user specified parameters
  can be lazily auto-mounted. airfs still try to use the `http` storage to mount
  unsupported storage like public internet links.
* airfs now provides the public base exception `airfs.AirfsException` and the public
  base warning `airfs.AirfsWarning`. Users can catch them to handle airfs exceptions.
* airfs now raises `airfs.MountException` on mounting errors instead of `ValueError`.
* airfs now raises `airfs.ConfigurationException` on configuration errors.
* The S3 storage now support any supported S3 compatible storage and not only AWS.
* Add `Flake8 <https://gitlab.com/pycqa/flake8>`_,
  `Mypy <http://www.mypy-lang.org/>`_ and
  `Bandit <https://github.com/PyCQA/bandit>`_ to the Pytest sequence.
* Refactor `airfs.io.SystemBase.list_objects` to improve to support more input file
  listing and make the API simpler. Remove `airfs.io.FileSystemBase` which is now a
  duplicate.

Fixes:

* airfs now let correctly `ImportError` from dependencies be raised instead of hidding
  it by raising an exception saying that the storage is not available.
* airfs now open S3 presigned URLs as standard HTTP URLs instead of trying to use S3
  API.
* Fix the missing `follow_symlinks` argument in `airfs.copy`.
* Fix the missing keyword-only argument mark in various functions.

Deprecations:

* Warn about the Python 3.5 deprecation in the next version.

1.4.0 (2020/01)
---------------

Fork from "pycosio" unmaintained project:

* Namespace to import changed from `pycosio` to `airfs`. It is possible to upgrade by
  replacing all `pycosio` code occurrences by `airfs` or doing
  `import airfs as pycosio`.

Improvements:

* Use `__slots__` on all classes to improve performance and memory usage.

Deprecations:

* Python 2.7 and 3.4 support are removed.

1.3.3 (2019/07)
---------------

Fixes:

* Fix package including `tests/storage_package` subdirectory due to packaging toolchain
  issue.

1.3.2 (2019/05)
---------------

Fixes:

* Fix ``io.BufferedIOBase`` partially read when reading exactly by parts of the buffer
  size.
* Fix bad permission error handling ``airfs.copy``.
* Fix bad exceptions risen if no parent directory found using ``airfs.copy``.

Contributors:

* Thanks to Stewart Adam (stewartadam)for the tests and fixes for Azure.

1.3.1 (2019/04)
---------------

Fixes:

* Trying to open a file in ``a`` or ``x`` mode now raise ``PermissionError`` if not
  enough permission to determinate if the file already exists.
* Fix ``OSError`` exception conversion in ``copy`` and ``copyfile``.
* ``copy`` and ``copyfile`` now tries to copy if no read access but write access,
  instead of raising ``PermissionError`` because unable to check the parent directory
  first.
* Azure: Fix error when trying to read an existing blog due to bad name handling.
* Azure: Fix query string in blob object path because not removed from URL.
* Azure: Fix error when opening a blob in ``w`` mode if no permission to read it.

Contributors:

* Thanks to Stewart Adam (stewartadam) for the tests and fixes for Azure.

1.3.0 (2019/03)
---------------

Add support for following storage:

* Microsoft Azure Blob Storage
* Microsoft Azure File Storage

Improvements:

* ``io.RawIOBase`` can now be used for storage that supports random write access.
* OSS: Copy objects between OSS buckets without copying data on the client when
  possible.

Deprecations:

* Warn about Python 3.4 deprecation in next version.

Fixes:

* Fix unsupported operation not risen in all cases with raw and buffered IO.
* Fix call of ``flush()`` in buffered IO.
* Fix file methods not translate storage exception into ``OSError``.
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

Contributors:

* Thanks to Stewart Adam (stewartadam) for the early tests and fixes for Azure.

1.2.0 (2018/10)
---------------

New standard library equivalent functions:

* ``os.listdir``, ``os.lstat``, ``os.remove``, ``os.rmdir``, ``os.scandir``,
  ``os.stat``, ``os.unlink``, ``os.path.getctime``, ``os.path.islink``,
  ``shutil.copyfile``.

Improvements:

* Copy of objects from and to the same storage is performed directly on remote server if
  possible.
* Now raises ``io.UnsupportedOperation`` if an operation is not compatible with the
  current storage, this applies to all newly created function and following existing
  functions: ``getsize``,  ``getmtime``, ``mkdir``.

Fixes:

* ``io.BufferedIOBase.read`` now returns empty bytes instead of raising exception when
  trying to read if seek already at end of the file.
* ``copy`` destination can now be a storage directory and not only a local directory.
* ``copy`` now checks if destination parent directory exists and if files are not the
  same file and raise proper exceptions.
* ``mkdir``: missing ``dir_fd`` argument.
* ``isdir`` now correctly handle "virtual" directories (Directory that don't exist as a
  proper object, but exists in another object path).

1.1.0 (2018/10)
---------------

New standard library equivalent functions:

* ``os.path.exists``, ``os.path.isabs``, ``os.path.isdir``, ``os.path.ismount``,
  ``os.path.samefile``, ``os.path.splitdrive``, ``os.makedirs``, ``os.mkdir``.

Backward incompatible change:

* ``mount`` argument ``extra_url_prefix`` is renamed to more relevant and clearer
  ``extra_root``.

Improvements:

* No buffer copy when using ``io.BufferedIOBase.read`` with exactly buffer size. This
  may lead to performance improvement.
* Minimum packages versions are set in setup based on packages changelog or date.

Fixes:

* ``isfile`` now correctly returns ``False`` when used on a directory.
* ``relpath`` now keeps ending ``/`` on storage path (Directory marker).

1.0.0 (2018/08)
---------------

The first version that implements the core machinery.

Provides storage equivalent functions of:

* ``open`` / ``io.open``, ``shutil.copy``, ``os.path.getmtime``, ``os.path.getsize``,
  ``os.path.isfile``, ``os.path.relpath``.

Provide storage objects abstract classes with the following interfaces:

* ``io.RawIOBase``, ``io.BufferedIOBase``.

Add support for following storage:

* Alibaba Cloud OSS
* AWS S3
* OpenStack Swift

Add read-only generic HTTP/HTTPS objects support.

Known issues
------------

* Append mode doesn't work with ``ObjectBufferedIOBase``.
