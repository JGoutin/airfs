# coding=utf-8
"""Amazon Web Services S3"""

from contextlib import contextmanager as _contextmanager
import boto3 as _boto3
from botocore.exceptions import ClientError as _ClientError

from pycosio._compat import to_timestamp as _to_timestamp
from pycosio.io_base import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase)


@_contextmanager
def _handle_io_exceptions():
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
            raise OSError(error['Message'])
        raise


def _upload_part(boto3_session_kwargs=None, **kwargs):
    """
    Upload part with picklable S3 client.

    Used with ProcessPoolExecutor

    Args:
        boto3_session_kwargs (dict): Boto3 Session keyword arguments.
        kwargs: see boto3 upload_part
    """
    return _boto3.session.Session(
        **(boto3_session_kwargs or {})).client('s3').upload_part(**kwargs)


class S3RawIO(_ObjectRawIOBase):
    """Binary S3 Object I/O

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        boto3_session_kwargs: Boto3 Session keyword arguments.
    """

    def __init__(self, name, mode='r', **boto3_session_kwargs):

        # Splits URL scheme is any
        try:
            path = name.split('://')[1]
        except IndexError:
            path = name
            name = 's3://' + path

        # Initializes storage
        _ObjectRawIOBase.__init__(self, name, mode)

        # Instantiates S3 client
        self._client = _boto3.session.Session(
            **boto3_session_kwargs).client("s3")

        # Prepares S3 I/O functions and common arguments
        self._get_object = self._client.get_object
        self._put_object = self._client.put_object
        self._head_object = self._client.head_object

        bucket_name, key = path.split('/', 1)
        self._client_kwargs = dict(Bucket=bucket_name, Key=key)

    def _head(self):
        """
        Returns object metadata.

        Returns:
            dict: Object metadata.
        """
        with _handle_io_exceptions():
            return self._head_object(**self._client_kwargs)

    def getsize(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        return self._head()['ContentLength']

    def getmtime(self):
        """
        Return the time of last access of path.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        return _to_timestamp(self._head()['LastModified'])

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
            with _handle_io_exceptions():
                response = self._get_object(
                    Range=self._http_range(start, end),
                    **self._client_kwargs)

        # Check for end of file
        except _ClientError as exception:
            if exception.response['Error'][
                    'Code'] == 'InvalidRange':
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
        with _handle_io_exceptions():
            return self._get_object(
                **self._client_kwargs)['Body'].read()

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        # Sends to S3 the entire file at once
        with _handle_io_exceptions():
            self._put_object(
                Body=memoryview(self._write_buffer).tobytes(),
                **self._client_kwargs)


class S3BufferedIO(_ObjectBufferedIOBase):
    """Buffered binary S3 Object I/O

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        workers_type (str): Parallel workers type: 'thread' or 'process'.
        boto3_session_kwargs: Boto3 Session keyword arguments.
    """

    _RAW_CLASS = S3RawIO

    #: Default buffer_size value in bytes (Boto3 8MB default buffer)
    DEFAULT_BUFFER_SIZE = 8388608

    #: Minimal buffer_size in bytes (S3 multipart upload minimal part size)
    MINIMUM_BUFFER_SIZE = 5242880

    def __init__(self, name, mode='r', buffer_size=None,
                 max_workers=None, workers_type='thread',
                 **boto3_session_kwargs):

        _ObjectBufferedIOBase.__init__(
            self, name, mode=mode, buffer_size=buffer_size,
            max_workers=max_workers, workers_type=workers_type,
            **boto3_session_kwargs)

        # Use same client as RAW class, but keep theses names
        # protected to this module
        self._client = self._raw._client
        self._client_kwargs = self._raw._client_kwargs

        # Use multipart upload as write buffered mode
        if self._writable:
            self._parts = []
            self._upload_args = self._client_kwargs.copy()

            if self._workers_type == 'thread':
                self._upload_part = self._client.upload_part

            # Multi processing needs external function
            else:
                self._upload_part = _upload_part
                self._upload_args['boto3_session_kwargs'] = boto3_session_kwargs

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        # Initialize multi-part upload
        if 'UploadId' not in self._upload_args:
            with _handle_io_exceptions():
                self._upload_args['UploadId'] = self._client.create_multipart_upload(
                    **self._client_kwargs)['UploadId']

        # Upload part with workers
        response = self._workers.submit(
            self._upload_part, Body=self._get_buffer().tobytes(),
            PartNumber=self._seek, **self._upload_args)

        # Save part information
        self._parts.append(dict(response=response, PartNumber=self._seek))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        # Wait parts upload completion
        for part in self._parts:
            part['ETag'] = part.pop('response').result()['ETag']

        # Complete multipart upload
        self._client.complete_multipart_upload(
            MultipartUpload={'Parts': self._parts},
            UploadId=self._upload_args['UploadId'],
            **self._client_kwargs)
