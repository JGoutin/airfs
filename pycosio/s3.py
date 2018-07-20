# coding=utf-8
"""Amazon Web Services S3"""

from contextlib import contextmanager as _contextmanager
import boto3 as _boto3
from botocore.exceptions import ClientError as _ClientError

from pycosio.abc import (
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
        code = error['Code']

        # File not found
        if code == '404':
            raise FileNotFoundError(error['Message'])

        # Unauthorized
        elif code == '403':
            raise PermissionError(error['Message'])


class S3RawIO(_ObjectRawIOBase):
    """Binary S3 Object I/O

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r' (default), 'w', 'a'
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

    def _get_metadata(self):
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
        return self._get_metadata()['ContentLength']

    def getmtime(self):
        """
        Return the time of last access of path.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        return self._get_metadata()['LastModified'].timestamp()

    def _read_range(self, start, end):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position.

        Returns:
            bytes: number of bytes read
        """
        # Get object part from S3
        try:
            with _handle_io_exceptions():
                response = self._get_object(
                    Range='bytes=%d-%d' % (start, end - 1),
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
        # Get object from seek to EOF
        with _handle_io_exceptions():
            if self._seek:
                response = self._get_object(
                    Range='bytes=%d-' % self._seek,
                    **self._client_kwargs)

            # Get object full content
            else:
                response = self._get_object(**self._client_kwargs)

        # Get object content
        return response['Body'].read()

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
        mode (str): The mode can be 'r' (default), 'w'.
            for reading (default) or writing
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        workers_type (str): Parallel workers type: 'thread' or 'process'.
        boto3_session_kwargs: Boto3 Session keyword arguments.
    """

    _RAW_CLASS = S3RawIO

    #: Default buffer_size value in bytes (Use Boto3 8MB default value)
    DEFAULT_BUFFER_SIZE = 8388608

    def __init__(self, name, mode='r', buffer_size=None,
                 max_workers=None, workers_type='thread',
                 **boto3_session_kwargs):

        _ObjectBufferedIOBase.__init__(
            self, name, mode=mode, buffer_size=buffer_size,
            max_workers=max_workers, workers_type=workers_type,
            **boto3_session_kwargs)

        # Use multipart upload as write buffered mode
        if self._writable:
            self._multipart_upload = None
            self._parts = []

        # Use same client as RAW class, but keep theses names
        # protected to this module
        self._client = self._raw._client
        self._client_kwargs = self._raw._client_kwargs

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        # Initialize multi-part upload
        if self._multipart_upload is None:
            self._seek = 0
            with _handle_io_exceptions():
                self._multipart_upload = self._client.Bucket(
                    self._client_kwargs['Bucket']).Object(
                        self._client_kwargs['Key']).initiate_multipart_upload()

        # Upload part with workers
        e_tag = self._workers.submit(
            self._client.upload_part,
            Body=memoryview(self._write_buffer).tobytes(),
            PartNumber=self._seek,
            UploadId=self._multipart_upload.id,
            **self._client_kwargs)

        # Save part information
        self._parts.append(dict(ETag=e_tag, PartNumber=self._seek))

        # Clear buffer and and advance part number/seek
        self._seek += 1
        self._write_buffer = bytearray(self._buffer_size)

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        # Wait uploads completion
        for part in self._parts:
            part['ETag'] = part['ETag'].result()

        # Complete multipart upload
        self._multipart_upload.complete(
            MultipartUpload={'Parts': self._parts})

        # Clear
        self._multipart_upload = None
        self._parts.clear()
