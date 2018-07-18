# coding=utf-8
"""Amazon Web Services S3"""

import boto3 as _boto3
from botocore.exceptions import ClientError as _ClientError

from cloudstorageio import (
    ObjectIOBase as _ObjectIOBase,
    BufferedObjectIOBase as _BufferedObjectIOBase)


# TODO: 404 error handling


class S3ObjectIO(_ObjectIOBase):
    """Binary S3 Object I/O"""

    def __init__(self, name, mode='r', **boto3_session_args):

        # Splits URL scheme is any
        try:
            path = name.split('://')[1]
        except IndexError:
            path = name
            name = 's3://' + path

        # Initializes storage
        _ObjectIOBase.__init__(self, name, mode)

        # Instantiates S3 client
        self._client = _boto3.session.Session(
            **boto3_session_args).client("s3")

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

    def readinto(self, b):
        """
        Read bytes into a pre-allocated, writable bytes-like object b,
        and return the number of bytes read.

        Args:
            b (bytes-like object): buffer.

        Returns:
            int: number of bytes read
        """
        # Get and update stream positions
        size = len(b)
        with self._seek_lock:
            start = self._seek
            end = start + size
            self._seek = end

        # Get object part from S3
        try:
            response = self._get_object(
                Range='bytes=%d-%d' % (start, end - 1),
                **self._client_kwargs)

        # Check for end of file
        except _ClientError as exception:
            error_code = exception.response['Error']['Code']
            if error_code == 'InvalidRange':
                # EOF
                return 0
            raise

        # Get object content
        body = response['Body'].read()
        body_size = len(body)
        b[:body_size] = body

        # Update stream position if end of file
        if body_size != size:
            with self._seek_lock:
                self._seek = start + body_size

        # Return read size
        return body_size

    def readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with self._seek_lock:

            # Get object starting from seek
            if self._seek:
                response = self._get_object(
                    Range='bytes=%d-' % self._seek,
                    **self._client_kwargs)

            # Get object starting from object start
            else:
                response = self._get_object(
                    **self._client_kwargs)

            # Get object content
            body = response['Body'].read()

            # Update stream position
            self._seek += len(body)
            return body

    def flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        # This send to S3 the entire file
        self._put_object(
            Body=memoryview(self._write_buffer).tobytes(),
            **self._client_kwargs)


class S3BufferedObjectIO(_BufferedObjectIOBase):
    """Buffered binary S3 Object I/O"""
    # TODO: implement write with "multipart upload"
    _RAW_CLASS = S3ObjectIO
