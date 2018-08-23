# coding=utf-8
"""Alibaba cloud OSS"""
from contextlib import contextmanager as _contextmanager
import re as _re

import oss2 as _oss
from oss2.models import PartInfo as _PartInfo
from oss2.exceptions import OssError as _OssError

from pycosio._core.exceptions import ObjectNotFoundError, ObjectPermissionError
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)


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
        if exception.status in (403, 404):
            raise {403: ObjectPermissionError, 404: ObjectNotFoundError}[
                exception.status](exception.details)
        raise


class _OSSSystem(_SystemBase):
    """
    OSS system.

    Args:
        storage_parameters (dict): OSS2 Auth keyword arguments and endpoint.
            This is generally OSS credentials and configuration.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """

    def __init__(self, storage_parameters=None, *args, **kwargs):
        try:
            storage_parameters = storage_parameters.copy()
            self._endpoint = storage_parameters.pop('endpoint')
        except (AttributeError, KeyError):
            raise ValueError('"endpoint" is required as "storage_parameters"')

        _SystemBase.__init__(self, storage_parameters=storage_parameters,
                             *args, **kwargs)
        if self._unsecure:
            self._endpoint = self._endpoint.replace('https://', 'http://')

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        bucket_name, key = self.relpath(path).split('/', 1)
        return dict(bucket_name=bucket_name, key=key)

    def _get_client(self):
        """
        OSS2 Auth client

        Returns:
            oss2.Auth or oss2.StsAuth: client
        """
        return (_oss.StsAuth if 'security_token' in self._storage_parameters
                else _oss.Auth if self._storage_parameters
                else _oss.AnonymousAuth)(**self._storage_parameters)

    def _get_prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str or re.Pattern: URL prefixes
        """
        return 'oss://', _re.compile(_re.sub(
            "(https?://)(oss-.+\.aliyuncs\.com)",
            r"\1[\\w-]+.\2", self._endpoint.rstrip('/'),
            count=1).replace('.', r'\.'))

    def _get_bucket(self, client_kwargs):
        """
        Get bucket object.

        Returns:
            oss2.Bucket
        """
        return _oss.Bucket(self.client, endpoint=self._endpoint,
                           bucket_name=client_kwargs['bucket_name'])

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_oss_error():
            return self._get_bucket(client_kwargs).head_object(
                key=client_kwargs['key']).headers


class OSSRawIO(_ObjectRawIOBase):
    """Binary OSS Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        storage_parameters (dict): OSS2 Auth keyword arguments and endpoint.
            This is generally OSS credentials and configuration.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _SYSTEM_CLASS = _OSSSystem

    def __init__(self, *args, **kwargs):
        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Initializes oss2.Bucket object
        self._bucket = self._system._get_bucket(self._client_kwargs)
        self._key = self._client_kwargs['key']

    def _read_range(self, start, end=0):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position.
                0 To not specify end.

        Returns:
            bytes: number of bytes read
        """
        # Get object bytes range
        try:
            with _handle_oss_error():
                response = self._bucket.get_object(key=self._key, headers=dict(
                    Range=self._http_range(
                        # Returns full file if end > size
                        start, end if end <= self._size else self._size)))

        # Check for end of file
        except _OssError as exception:
            if exception.status == 416:
                # EOF
                return bytes()
            raise

        # Get object content
        return response.read()

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with _handle_oss_error():
            return self._bucket.get_object(key=self._key).read()

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        with _handle_oss_error():
            self._bucket.put_object(
                key=self._key, data=self._get_buffer().tobytes())


class OSSBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary OSS Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode
            or awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        storage_parameters (dict): OSS2 Auth keyword arguments and endpoint.
            This is generally OSS credentials and configuration.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """

    _RAW_CLASS = OSSRawIO

    def __init__(self, *args, **kwargs):
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        self._bucket = self._raw._bucket
        self._key = self._raw._key
        self._upload_id = None

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        # Initialize multipart upload
        if self._upload_id is None:
            with _handle_oss_error():
                self._upload_id = self._bucket.init_multipart_upload(
                    self._key).upload_id

        # Upload part with workers
        response = self._workers.submit(
            self._bucket.upload_part, key=self._key, upload_id=self._upload_id,
            part_number=self._seek, data=self._get_buffer().tobytes())

        # Save part information
        self._write_futures.append(
            dict(response=response, part_number=self._seek))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        # Wait parts upload completion
        parts = [_PartInfo(part_number=future['part_number'],
                           etag=future['response'].result().etag)
                 for future in self._write_futures]

        # Complete multipart upload
        self._bucket.complete_multipart_upload(
            key=self._key, upload_id=self._upload_id, parts=parts)
