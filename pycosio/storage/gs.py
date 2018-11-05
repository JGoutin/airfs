# coding=utf-8
"""Google Cloud Storage"""
from contextlib import contextmanager as _contextmanager
from io import BytesIO as _BytesIO
import re as _re

from google.cloud.storage.client import Client as _Client
import google.cloud.exceptions as _gc_exc

from pycosio._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError)
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)


@_contextmanager
def _handle_google_exception():
    """
    Handle Google cloud exception and convert to class
    IO exceptions

    Raises:
        OSError subclasses: IO error.
    """
    try:
        yield

    except (_gc_exc.Unauthorized, _gc_exc.Forbidden) as exception:
            raise _ObjectPermissionError(exception.message)

    except _gc_exc.NotFound as exception:
            raise _ObjectNotFoundError(exception.message)


class _GSSystem(_SystemBase):
    """
    Google Cloud Storage system.

    Args:
        storage_parameters (dict): ????
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _CTIME_KEYS = ('timeCreated',)
    _MTIME_KEYS = ('updated',)
    _SIZE_KEYS = ('size',)

    def copy(self, src, dst):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
        """
        dst_client_kwargs = self.get_client_kwargs(dst)
        with _handle_google_exception():
            self.client.copy_blob(
                blob=self._get_blob(self.get_client_kwargs(src)),
                destination_bucket=self._get_bucket(dst_client_kwargs),
                new_name=dst_client_kwargs['blob_name'])

    def _get_blob(self, client_kwargs):
        """
        Get blob object.

        Returns:
            google.cloud.storage.blob.Blob: Blob object
        """
        return self._get_bucket(client_kwargs).get_blob(
            blob_name=client_kwargs['blob_name'])

    def _get_client(self):
        """
        Google storage client

        Returns:
            google.cloud.storage.client.Client: client
        """
        return _Client(**self._storage_parameters)

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        bucket_name, key = self.split_locator(path)
        kwargs = dict(bucket_name=bucket_name)
        if key:
            kwargs['blob_name'] = key
        return kwargs

    def _get_bucket(self, client_kwargs):
        """
        Get bucket object.

        Returns:
            google.cloud.storage.bucket.Bucket: Bucket object
        """
        return self.client.get_bucket(bucket_name=client_kwargs['bucket_name'])

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        return (
            # GS scheme
            # - gs://<bucket>/<blob>
            'gs://',

            # JSON API URL
            # - https://www.googleapis.com/storage/v1/<bucket>/<blob>
            'https://www.googleapis.com/storage/v1',

            # Virtual-hosted–style XML API URL
            # - http://storage.googleapis.com/<bucket>/<blob>
            # - https://storage.googleapis.com/<bucket>/<blob>
            _re.compile(r'https?://storage\.googleapis\.com'),

            # Path-hosted–style XML API URL
            # - http://<bucket>.storage.googleapis.com/<blob>
            # - https://<bucket>.storage.googleapis.com/<blob>
            _re.compile(r'https?://[\w-]+\.storage\.googleapis\.com'),

            # Authenticated Browser Downloads URL
            # - http://storage.cloud.google.com
            # - https://storage.cloud.google.com
            _re.compile(r'https?://storage\.cloud\.google\.com'))

    def _head(self, client_kwargs):
        """
        Returns object or bucket HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_google_exception():
            # Object
            if 'blob_name' in client_kwargs:
                return self._get_blob(client_kwargs)._properties

            # Bucket
            else:
                return self._get_bucket(client_kwargs)._properties

    def _list_locators(self):
        """
        Lists locators.

        Returns:
            generator of tuple: locator name str, locator header dict
        """
        with _handle_google_exception():
            for bucket in self.client.list_buckets():
                yield bucket.name, bucket._properties

    def _list_objects(self, client_kwargs, path, max_request_entries):
        """
        Lists objects.

        args:
            client_kwargs (dict): Client arguments.
            path (str): Path relative to current locator.
            max_request_entries (int): If specified, maximum entries returned
                by request.

        Returns:
            generator of tuple: object name str, object header dict
        """
        client_kwargs = client_kwargs.copy()
        if max_request_entries:
            client_kwargs['max_results'] = max_request_entries

        with _handle_google_exception():
            bucket = self._get_bucket(client_kwargs)

        while True:
            with _handle_google_exception():
                for blob in bucket.list_blobs(prefix=path, **client_kwargs):
                    yield blob.name, blob._properties

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_google_exception():
            # Object
            if 'blob_name' in client_kwargs:
                return self._get_blob(client_kwargs).upload_from_file(
                    file_obj=_BytesIO())

            # Bucket
            return self.client.create_bucket(
                bucket_name=client_kwargs['bucket_name'])

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_google_exception():
            # Object
            if 'blob_name' in client_kwargs:
                return self._get_bucket(client_kwargs).delete_blob(
                    blob_name=client_kwargs['blob_name'])

            # Bucket
            return self._get_bucket(client_kwargs).delete()


class GSRawIO(_ObjectRawIOBase):
    """Binary Google Cloud Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        storage_parameters (dict): ????
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _SYSTEM_CLASS = _GSSystem

    def __init__(self, *args, **kwargs):
        _ObjectRawIOBase.__init__(self, *args, **kwargs)
        self._blob = self._system._get_blob(self._client_kwargs)
        self._download_to_file = self._blob.download_to_file
        self._upload_from_file = self._blob.upload_from_file

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
        file_obj = _BytesIO()
        with _handle_google_exception():
            self._download_to_file(
                file_obj=file_obj, start=start, end=end if end else None)
        return file_obj.getvalue()

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        file_obj = _BytesIO()
        with _handle_google_exception():
            self._download_to_file(file_obj=file_obj)
        return file_obj.getvalue()

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        with _handle_google_exception():
            self._upload_from_file(file_obj=_BytesIO(self._write_buffer))


class GSBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary Google Cloud Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode
            or awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        storage_parameters (dict): ????
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _RAW_CLASS = GSRawIO

    def __init__(self, *args, **kwargs):
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        if self._writable:
            self._segment_name = self._client_kwargs['blob_name'] + '.%03d'
            with _handle_google_exception():
                self._get_blob = self._system._get_blob

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        # Upload segment with workers
        client_kwargs = self._client_kwargs.copy()
        client_kwargs['blob_name'] = self._segment_name % self._seek
        blob = self._get_blob(client_kwargs)

        response = self._workers.submit(
            blob.upload_from_file,
            file_obj=_BytesIO(self._get_buffer()))

        self._write_futures.append((blob, response))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        with _handle_google_exception():
            # Concatenate parts while waiting for parts upload
            final_blob = self._get_blob(self._client_kwargs)
            blobs = []

            for blob, future in self._write_futures:
                future.result()
                blobs.append(blob)

                # Composes limit reached: Concatenates now
                if len(blobs) == 32:
                    final_blob.compose(blobs)
                    blobs = [final_blob]

            # Concatenates last segments
            if blobs != [final_blob]:
                final_blob.compose(blobs)
