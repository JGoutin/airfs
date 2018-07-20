# coding=utf-8
"""Amazon Web Services S3"""

import boto3 as _boto3
from botocore.exceptions import ClientError as _ClientError

from pycosio.abc import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase)


# TODO: 404 error handling


class S3RawIO(_ObjectRawIOBase):
    """Binary S3 Object I/O"""

    def __init__(self, name, mode='r', **boto3_session_args):

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
            if exception.response['Error'][
                    'Code'] == 'InvalidRange':
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


class S3BufferedIO(_ObjectBufferedIOBase):
    """Buffered binary S3 Object I/O"""
    # TODO: implement write with "multipart upload"
    _RAW_CLASS = S3RawIO

    def __init__(self, *args, **kwargs):
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        # Use same client as RAW class, but keep theses names
        # protected to this module
        self._client = self._raw._client
        self._client_kwargs = self._raw._client_kwargs
        self._multipart_upload = None
        self._parts = []

    def _flush(self, end_of_object=False):
        """
        Flush the write buffers of the stream.

        Args:
            end_of_object (bool): If True, mark the writing as completed.
                Any following write will start from the start of object.
        """
        with self._seek_lock:
            # Initialize multi-part upload
            if not self._write_initialized:
                self._seek = 1
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

            # Complete multi-part upload
            if end_of_object:

                # Wait uploads completion
                for part in self._parts:
                    part['ETag'] = part['ETag'].result()

                # Complete multipart upload
                self._multipart_upload.complete(
                    MultipartUpload={'Parts': self._parts})

                # Clear
                self._multipart_upload = None
                self._parts.clear()
