"""Alibaba cloud OSS"""
from contextlib import contextmanager as _contextmanager
import re as _re

import oss2 as _oss  # type: ignore
from oss2.models import PartInfo as _PartInfo  # type: ignore
from oss2.exceptions import OssError as _OssError  # type: ignore

from airfs._core.io_base import memoizedmethod as _memoizedmethod
from airfs._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError,
    ObjectNotASymlinkError as _ObjectNotASymlinkError,
    ObjectNotImplementedError as _ObjectNotImplementedError,
)
from airfs.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase,
)

_ERROR_CODES = {
    403: _ObjectPermissionError,
    404: _ObjectNotFoundError,
    409: _ObjectPermissionError,
}


@_contextmanager
def _handle_oss_error():
    """
    Handle OSS exception and convert to class IO exceptions

    Raises:
        OSError subclasses: IO error.
    """
    try:
        yield

    except _OssError as exception:
        if exception.status in _ERROR_CODES:
            raise _ERROR_CODES[exception.status](exception.details.get("Message", ""))
        raise


class _OSSSystem(_SystemBase):
    """
    OSS system.

    Args:
        storage_parameters (dict): OSS2 Auth keyword arguments and endpoint.
            This is generally OSS credentials and configuration.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __slots__ = ("_unsecure", "_endpoint")

    SUPPORTS_SYMLINKS = True

    _CTIME_KEYS = ("Creation-Date", "creation_date")
    _MTIME_KEYS = ("Last-Modified", "last_modified")

    def __init__(self, storage_parameters=None, *args, **kwargs):
        try:
            storage_parameters = storage_parameters.copy()
            self._endpoint = storage_parameters.pop("endpoint")
        except (AttributeError, KeyError):
            raise ValueError('"endpoint" is required as "storage_parameters"')

        _SystemBase.__init__(
            self, storage_parameters=storage_parameters, *args, **kwargs
        )
        if self._unsecure:
            self._endpoint = self._endpoint.replace("https://", "http://")

    def copy(self, src, dst, other_system=None):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
            other_system (airfs._core.io_system.SystemBase subclass): Unused.
        """
        copy_source = self.get_client_kwargs(src)
        copy_destination = self.get_client_kwargs(dst)
        with _handle_oss_error():
            bucket = self._get_bucket(copy_destination)
            bucket.copy_object(
                source_bucket_name=copy_source["bucket_name"],
                source_key=copy_source["key"],
                target_key=copy_destination["key"],
            )

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        bucket_name, key = self.split_locator(path)
        kwargs = dict(bucket_name=bucket_name)
        if key:
            kwargs["key"] = key
        return kwargs

    def _get_client(self):
        """
        OSS2 Auth client

        Returns:
            oss2.Auth or oss2.StsAuth: client
        """
        return (
            _oss.StsAuth
            if "security_token" in self._storage_parameters
            else _oss.Auth
            if self._storage_parameters
            else _oss.AnonymousAuth
        )(**self._storage_parameters)

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """

        return (
            # OSS Scheme
            # - oss://<bucket>/<key>
            "oss://",
            # URL (With common aliyuncs.com endpoint):
            # - http://<bucket>.oss-<region>.aliyuncs.com/<key>
            # - https://<bucket>.oss-<region>.aliyuncs.com/<key>
            # Note: "oss-<region>.aliyuncs.com" may be replaced by another endpoint
            _re.compile(
                (r"^https?://[\w-]+.%s" % self._endpoint.split("//", 1)[1]).replace(
                    ".", r"\."
                )
            ),
        )

    def _get_bucket(self, client_kwargs):
        """
        Get bucket object.

        Returns:
            oss2.Bucket
        """
        return _oss.Bucket(
            self.client,
            endpoint=self._endpoint,
            bucket_name=client_kwargs["bucket_name"],
        )

    def islink(self, path=None, client_kwargs=None, header=None):
        """
        Returns True if object is a symbolic link.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            bool: True if object is Symlink.
        """
        header = self.head(path, client_kwargs, header)

        for key in ("x-oss-object-type", "type"):
            try:
                return header.pop(key) == "Symlink"
            except KeyError:
                continue
        return False

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_oss_error():
            bucket = self._get_bucket(client_kwargs)

            if "key" in client_kwargs:
                return bucket.head_object(key=client_kwargs["key"]).headers

            return bucket.get_bucket_info().headers

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_oss_error():
            bucket = self._get_bucket(client_kwargs)

            if "key" in client_kwargs:
                return bucket.put_object(key=client_kwargs["key"], data=b"")

            return bucket.create_bucket()

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_oss_error():
            bucket = self._get_bucket(client_kwargs)

            if "key" in client_kwargs:
                return bucket.delete_object(key=client_kwargs["key"])

            return bucket.delete_bucket()

    @staticmethod
    def _model_to_dict(model, ignore):
        """
        Convert OSS model to dict.

        Args:
            model (oss2.models.RequestResult): Model.
            ignore (tuple of str): Keys to not insert to dict.

        Returns:
            dict: Model dict version.
        """
        return {
            attr: value
            for attr, value in model.__dict__.items()
            if not attr.startswith("_") and attr not in ignore
        }

    def _list_locators(self, max_results):
        """
        Lists locators.

        args:
            max_results (int): The maximum results that should return the method.

        Yields:
            tuple: locator name str, locator header dict, has content bool
        """
        with _handle_oss_error():
            response = _oss.Service(self.client, endpoint=self._endpoint).list_buckets(
                max_keys=max_results or 100
            )

        for bucket in response.buckets:
            yield bucket.name, self._model_to_dict(bucket, ("name",)), True

    def _list_objects(self, client_kwargs, path, max_results, first_level):
        """
        Lists objects.

        args:
            client_kwargs (dict): Client arguments.
            path (str): Path to list.
            max_results (int): The maximum results that should return the method.
            first_level (bool): It True, may only first level objects.

        Yields:
            tuple: object path str, object header dict, has content bool
        """
        prefix = self.split_locator(path)[1]
        index = len(prefix)
        kwargs = dict(prefix=prefix)
        if max_results:
            kwargs["max_keys"] = max_results

        bucket = self._get_bucket(client_kwargs)

        while True:
            with _handle_oss_error():
                response = bucket.list_objects(**kwargs)

            if not response.object_list:
                raise _ObjectNotFoundError(path=path)

            for obj in response.object_list:
                yield obj.key[index:], self._model_to_dict(obj, ("key",)), False

            if response.next_marker:
                client_kwargs["marker"] = response.next_marker
            else:
                break

    def read_link(self, path=None, client_kwargs=None, header=None):
        """
        Return the path linked by the symbolic link.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            str: Path.
        """
        if client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)
        try:
            key = client_kwargs["key"]
        except KeyError:
            raise _ObjectNotASymlinkError(path=path)

        with _handle_oss_error():
            return path.rsplit(key, 1)[0] + (
                self._get_bucket(client_kwargs).get_symlink(symlink_key=key).target_key
            )

    def symlink(self, target, path=None, client_kwargs=None):
        """
        Creates a symbolic link to target.

        Args:
            target (str): Target path or URL.
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
        """
        if client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)
        target_client_kwargs = self.get_client_kwargs(target)

        if client_kwargs["bucket_name"] != target_client_kwargs["bucket_name"]:
            raise _ObjectNotImplementedError("Cross bucket symlinks are not supported")

        try:
            symlink_key = client_kwargs["key"]
            target_key = target_client_kwargs["key"]
        except KeyError:
            raise _ObjectNotImplementedError(
                "Symlinks to or from bucket root are not supported"
            )

        with _handle_oss_error():
            return self._get_bucket(client_kwargs).put_symlink(target_key, symlink_key)


class OSSRawIO(_ObjectRawIOBase):
    """Binary OSS Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a' for reading (default), writing or
            appending.
        storage_parameters (dict): OSS2 Auth keyword arguments and endpoint.
            This is generally OSS credentials and configuration.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    _SYSTEM_CLASS = _OSSSystem

    @property  # type: ignore
    @_memoizedmethod
    def _bucket(self):
        """
        Bucket client.

        Returns:
            oss2.Bucket: Client.
        """
        return self._system._get_bucket(self._client_kwargs)

    @property  # type: ignore
    @_memoizedmethod
    def _key(self):
        """
        Object key.

        Returns:
            str: key.
        """
        return self._client_kwargs["key"]

    def _read_range(self, start, end=0):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position. 0 To not specify end.

        Returns:
            bytes: number of bytes read
        """
        if start >= self._size:
            # EOF. Do not detect using 416 (Out of range) error, 200 returned.
            return bytes()

        with _handle_oss_error():
            response = self._bucket.get_object(
                key=self._key,
                headers=dict(
                    Range=self._http_range(
                        start,
                        end if end <= self._size else self._size,
                    )
                ),
            )

        return response.read()

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with _handle_oss_error():
            return self._bucket.get_object(key=self._key).read()

    def _flush(self, buffer):
        """
        Flush the write buffers of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
        """
        with _handle_oss_error():
            self._bucket.put_object(key=self._key, data=buffer.tobytes())


class OSSBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary OSS Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode or
            awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to execute the
            given calls.
        storage_parameters (dict): OSS2 Auth keyword arguments and endpoint.
            This is generally OSS credentials and configuration.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __slots__ = ("_bucket", "_key", "_upload_id")

    _RAW_CLASS = OSSRawIO

    #: Minimal buffer_size in bytes (OSS multipart upload minimal part size)
    MINIMUM_BUFFER_SIZE = 102400

    def __init__(self, *args, **kwargs):
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        self._bucket = self._raw._bucket
        self._key = self._raw._key
        self._upload_id = None

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        if self._upload_id is None:
            with _handle_oss_error():
                self._upload_id = self._bucket.init_multipart_upload(
                    self._key
                ).upload_id

        response = self._workers.submit(
            self._bucket.upload_part,
            key=self._key,
            upload_id=self._upload_id,
            part_number=self._seek,
            data=self._get_buffer().tobytes(),
        )

        self._write_futures.append(dict(response=response, part_number=self._seek))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        parts = [
            _PartInfo(
                part_number=future["part_number"], etag=future["response"].result().etag
            )
            for future in self._write_futures
        ]

        with _handle_oss_error():
            try:
                self._bucket.complete_multipart_upload(
                    key=self._key, upload_id=self._upload_id, parts=parts
                )
            except _OssError:
                self._bucket.abort_multipart_upload(
                    key=self._key, upload_id=self._upload_id
                )
                raise
