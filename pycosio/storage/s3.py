# coding=utf-8
"""Amazon Web Services S3"""

from contextlib import contextmanager as _contextmanager
import re as _re

import boto3 as _boto3
from botocore.exceptions import ClientError as _ClientError

from pycosio._core.compat import to_timestamp as _to_timestamp
from pycosio._core.exceptions import ObjectNotFoundError, ObjectPermissionError
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)


@_contextmanager
def _handle_client_error():
    """
    Handle boto exception and convert to class
    IO exceptions

    Raises:
        OSError subclasses: IO error.
    """
    try:
        yield

    except _ClientError as exception:
        error = exception.response['Error']
        if error['Code'] in ('403', '404'):
            raise {'403': ObjectPermissionError,
                   '404': ObjectNotFoundError}[error['Code']](error['Message'])
        raise


class _S3System(_SystemBase):
    """
    S3 system.

    Args:
        storage_parameters (dict): Boto3 Session keyword arguments.
            This is generally AWS credentials and configuration.
            This dict should contain two sub-dicts:
            'session': That pass its arguments to "boto3.session.Session"
            ; 'client': That pass its arguments to
            "boto3.session.Session.client".
            May be optional if running on AWS EC2 instances.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """

    def __init__(self, *args, **kwargs):
        self._session = None
        _SystemBase.__init__(self, *args, **kwargs)

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
        return dict(Bucket=bucket_name, Key=key)

    def _get_session(self):
        """
        S3 Boto3 Session.

        Returns:
            boto3.session.Session: session
        """
        if self._session is None:
            self._session = _boto3.session.Session(
                **self._storage_parameters.get('session', dict()))
        return self._session

    def _get_client(self):
        """
        S3 Boto3 client

        Returns:
            boto3.session.Session.client: client
        """
        client_kwargs = self._storage_parameters.get('client', dict())

        # Handles unsecure mode
        if self._unsecure:
            client_kwargs = client_kwargs.copy()
            client_kwargs['use_ssl'] = False

        return self._get_session().client("s3", **client_kwargs)

    def _get_prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str or re.Pattern: URL prefixes
        """
        region = self._get_session().region_name or '[\w-]+'
        return (
                # "s3" URL scheme
                's3://',

                # Virtual-hosted–style URL
                _re.compile('http://[\w.-]+\.s3\.amazonaws\.com'),
                _re.compile('http://[\w.-]+\.s3-%s\.amazonaws\.com' % region),

                # Path-hosted–style URL
                _re.compile('http://s3\.amazonaws\.com'),
                _re.compile('http://s3-%s\.amazonaws\.com' % region))

    @staticmethod
    def _getmtime_from_header(header):
        """
        Return the time from header

        Args:
            header (dict): Object header.

        Returns:
            float: The number of seconds since the epoch
        """
        return _to_timestamp(header['LastModified'])

    @staticmethod
    def _getsize_from_header(header):
        """
        Return the size from header

        Args:
            header (dict): Object header.

        Returns:
            int: Size in bytes.
        """
        return header['ContentLength']

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_client_error():
            return self.client.head_object(**client_kwargs)


class S3RawIO(_ObjectRawIOBase):
    """Binary S3 Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        storage_parameters (dict): Boto3 Session keyword arguments.
            This is generally AWS credentials and configuration.
            This dict should contain two sub-dicts:
            'session': That pass its arguments to "boto3.session.Session"
            ; 'client': That pass its arguments to
            "boto3.session.Session.client".
            May be optional if running on AWS EC2 instances.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _SYSTEM_CLASS = _S3System

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
        # Get object part from S3
        try:
            with _handle_client_error():
                response = self._client.get_object(
                    Range=self._http_range(start, end), **self._client_kwargs)

        # Check for end of file
        except _ClientError as exception:
            if exception.response['Error']['Code'] == 'InvalidRange':
                # EOF
                return bytes()
            raise

        # Get object content
        return response['Body'].read()

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with _handle_client_error():
            return self._client.get_object(**self._client_kwargs)['Body'].read()

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        with _handle_client_error():
            self._client.put_object(Body=self._get_buffer().tobytes(),
                                    **self._client_kwargs)


class S3BufferedIO(_ObjectBufferedIOBase):
    """Buffered binary S3 Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode
            or awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        storage_parameters (dict): Boto3 Session keyword arguments.
            This is generally AWS credentials and configuration.
            This dict should contain two sub-dicts:
            'session': That pass its arguments to "boto3.session.Session"
            ; 'client': That pass its arguments to
            "boto3.session.Session.client".
            May be optional if running on AWS EC2 instances.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """

    _RAW_CLASS = S3RawIO

    #: Minimal buffer_size in bytes (S3 multipart upload minimal part size)
    MINIMUM_BUFFER_SIZE = 5242880

    def __init__(self, *args, **kwargs):

        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        # Use multipart upload as write buffered mode
        if self._writable:
            self._upload_args = self._client_kwargs.copy()

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        # Initialize multi-part upload
        if 'UploadId' not in self._upload_args:
            with _handle_client_error():
                self._upload_args[
                    'UploadId'] = self._client.create_multipart_upload(
                    **self._client_kwargs)['UploadId']

        # Upload part with workers
        response = self._workers.submit(
            self._client.upload_part, Body=self._get_buffer().tobytes(),
            PartNumber=self._seek, **self._upload_args)

        # Save part information
        self._write_futures.append(
            dict(response=response, PartNumber=self._seek))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        # Wait parts upload completion
        for part in self._write_futures:
            part['ETag'] = part.pop('response').result()['ETag']

        # Complete multipart upload
        self._client.complete_multipart_upload(
            MultipartUpload={'Parts': self._write_futures},
            UploadId=self._upload_args['UploadId'], **self._client_kwargs)
