"""Amazon Web Services S3"""
from contextlib import contextmanager as _contextmanager
from io import UnsupportedOperation as _UnsupportedOperation
import re as _re

import boto3 as _boto3  # type: ignore
from botocore.exceptions import ClientError as _ClientError  # type: ignore

from airfs._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError,
    ObjectNotImplementedError as _ObjectNotImplementedError,
)
from airfs.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase,
)

_ERROR_CODES = {
    "AccessDenied": _ObjectPermissionError,
    "NoSuchKey": _ObjectNotFoundError,
    "InvalidBucketName": _ObjectNotFoundError,
    "NoSuchBucket": _ObjectNotFoundError,
    "403": _ObjectPermissionError,
    "404": _ObjectNotFoundError,
}


@_contextmanager
def _handle_client_error():
    """
    Handle boto exception and convert to class IO exceptions.

    Raises:
        OSError subclasses: IO error.
    """
    try:
        yield

    except _ClientError as exception:
        error = exception.response["Error"]
        if error["Code"] in _ERROR_CODES:
            raise _ERROR_CODES[error["Code"]](error["Message"])
        raise


class _S3System(_SystemBase):
    """
    S3 system.

    Args:
        storage_parameters (dict): Boto3 Session keyword arguments.
            This is generally AWS credentials and configuration.
            This dict should contain two sub-dicts:
            'session': That pass its arguments to "boto3.session.Session";
            'client': That pass its arguments to "boto3.session.Session.client".
            May be optional if already configured on host.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __slots__ = ("_session",)

    _SIZE_KEYS = ("ContentLength",)
    _CTIME_KEYS = ("CreationDate",)
    _MTIME_KEYS = ("LastModified",)

    def __init__(self, *args, **kwargs):
        self._session = None
        _SystemBase.__init__(self, *args, **kwargs)

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
        with _handle_client_error():
            self.client.copy_object(CopySource=copy_source, **copy_destination)

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        bucket_name, key = self.split_locator(path)
        kwargs = dict(Bucket=bucket_name)
        if key:
            kwargs["Key"] = key
        return kwargs

    def _get_session(self):
        """
        S3 Boto3 Session.

        Returns:
            boto3.session.Session: session
        """
        if self._session is None:
            self._session = _boto3.session.Session(
                **self._storage_parameters.get("session", dict())
            )
        return self._session

    def _get_client(self):
        """
        S3 Boto3 client

        Returns:
            boto3.session.Session.client: client
        """
        client_kwargs = self._storage_parameters.get("client", dict())

        if self._unsecure:
            client_kwargs = client_kwargs.copy()
            client_kwargs["use_ssl"] = False

        return self._get_session().client("s3", **client_kwargs)

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        # "(?!.*&X-Amz-Signature=)" allow ignoring presigned URLs to open them as
        # regular HTTP files

        # Use the specified endpoint URL as root if specified to allow the use of
        # any S3 compatible storage.
        try:
            return (
                _re.compile(
                    r"^%s(?!.*&X-Amz-Signature=)"
                    % self._storage_parameters["client"]["endpoint_url"]
                ),
            )
        except KeyError:
            pass

        # Use default AWS roots
        region = self._get_session().region_name or r"[\w-]+"
        return (
            # S3 scheme
            # - s3://<bucket>/<key>
            "s3://",
            # Virtual-hosted–style URL
            # - http://<bucket>.s3.amazonaws.com/<key>
            # - https://<bucket>.s3.amazonaws.com/<key>
            # - http://<bucket>.s3-<region>.amazonaws.com/<key>
            # - https://<bucket>.s3-<region>.amazonaws.com/<key>
            _re.compile(
                r"^https?://[\w.-]+\.s3\.amazonaws\.com(?!.*&X-Amz-Signature=)"
            ),
            _re.compile(
                r"^https?://[\w.-]+\.s3-%s\.amazonaws\.com(?!.*&X-Amz-Signature=)"
                % region
            ),
            # Path-hosted–style URL
            # - http://s3.amazonaws.com/<bucket>/<key>
            # - https://s3.amazonaws.com/<bucket>/<key>
            # - http://s3-<region>.amazonaws.com/<bucket>/<key>
            # - https://s3-<region>.amazonaws.com/<bucket>/<key>
            _re.compile(r"^https?://s3\.amazonaws\.com(?!.*&X-Amz-Signature=)"),
            _re.compile(
                r"^https?://s3-%s\.amazonaws\.com(?!.*&X-Amz-Signature=)" % region
            ),
            # Transfer acceleration URL
            # - http://<bucket>.s3-accelerate.amazonaws.com
            # - https://<bucket>.s3-accelerate.amazonaws.com
            # - http://<bucket>.s3-accelerate.dualstack.amazonaws.com
            # - https://<bucket>.s3-accelerate.dualstack.amazonaws.com
            _re.compile(
                r"^https?://[\w.-]+\.s3-accelerate\.amazonaws\.com"
                r"(?!.*&X-Amz-Signature=)"
            ),
            _re.compile(
                r"^https?://[\w.-]+\.s3-accelerate\.dualstack\.amazonaws\.com"
                r"(?!.*&X-Amz-Signature=)"
            ),
        )

    @staticmethod
    def _get_time(header, keys, name):
        """
        Get time from header

        Args:
            header (dict): Object header.
            keys (tuple of str): Header keys.
            name (str): Method name.

        Returns:
            float: The number of seconds since the epoch
        """
        for key in keys:
            try:
                return header.pop(key).timestamp()
            except KeyError:
                continue
        raise _UnsupportedOperation(name)

    def _getsize_from_header(self, header):
        """
        Return the size from header

        Args:
            header (dict): Object header.

        Returns:
            int: Size in bytes.
        """
        try:
            return header.pop("ContentLength")
        except KeyError:
            raise _UnsupportedOperation("getsize")

    def _head(self, client_kwargs):
        """
        Returns object or bucket HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_client_error():
            if "Key" in client_kwargs:
                header = self.client.head_object(**client_kwargs)

            else:
                header = self.client.head_bucket(**client_kwargs)

        for key in ("AcceptRanges", "ResponseMetadata"):
            header.pop(key, None)
        return header

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_client_error():
            if "Key" in client_kwargs:
                return self.client.put_object(Body=b"", **client_kwargs)

            return self.client.create_bucket(
                Bucket=client_kwargs["Bucket"],
                CreateBucketConfiguration=dict(
                    LocationConstraint=self._get_session().region_name
                ),
            )

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_client_error():
            if "Key" in client_kwargs:
                return self.client.delete_object(**client_kwargs)

            return self.client.delete_bucket(Bucket=client_kwargs["Bucket"])

    def _list_locators(self, max_results):
        """
        Lists locators.

        args:
            max_results (int): The maximum results that should return the method.

        Yields:
            tuple: locator name str, locator header dict, has content bool
        """
        kwargs = dict()
        if max_results:
            kwargs["MaxKeys"] = max_results

        with _handle_client_error():
            response = self.client.list_buckets(**kwargs)

        for bucket in response["Buckets"]:
            yield bucket.pop("Name"), bucket, True

    def _list_objects(self, client_kwargs, path, max_results, first_level):
        """
        Lists objects.

        args:
            client_kwargs (dict): Client arguments.
            path (str): Path.
            max_results (int): The maximum results that should return the method.
            first_level (bool): It True, may only first level objects.

        Yields:
            tuple: object path str, object header dict, has content bool
        """
        prefix = self.split_locator(path)[1]
        index = len(prefix)
        kwargs = dict(Bucket=client_kwargs["Bucket"], Prefix=prefix)
        if max_results:
            kwargs["MaxKeys"] = max_results

        while True:
            with _handle_client_error():
                response = self.client.list_objects_v2(**kwargs)

            try:
                for obj in response["Contents"]:
                    yield obj.pop("Key")[index:], obj, False
            except KeyError:
                raise _ObjectNotFoundError(path=path)

            try:
                kwargs["ContinuationToken"] = response["NextContinuationToken"]
            except KeyError:
                break

    def _shareable_url(self, client_kwargs, expires_in):
        """
        Get a shareable URL for the specified path.

        Args:
            client_kwargs (dict): Client arguments.
            expires_in (int): Expiration in seconds.

        Returns:
            str: Shareable URL.
        """
        if "Key" not in client_kwargs:
            raise _ObjectNotImplementedError(
                "Shared URLs to buckets are not supported on S3"
            )
        return self.client.generate_presigned_url(
            "get_object", Params=client_kwargs, ExpiresIn=expires_in
        )


class S3RawIO(_ObjectRawIOBase):
    """Binary S3 Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a' for reading (default), writing or
            appending.
        storage_parameters (dict): Boto3 Session keyword arguments.
            This is generally AWS credentials and configuration.
            This dict should contain two sub-dicts:
            'session': That pass its arguments to "boto3.session.Session";
            'client': That pass its arguments to "boto3.session.Session.client".
            May be optional if already configured on host.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    _SYSTEM_CLASS = _S3System

    def _read_range(self, start, end=0):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position. 0 To not specify end.

        Returns:
            bytes: number of bytes read
        """
        try:
            with _handle_client_error():
                response = self._client.get_object(
                    Range=self._http_range(start, end), **self._client_kwargs
                )

        except _ClientError as exception:
            if exception.response["Error"]["Code"] == "InvalidRange":
                return bytes()
            raise

        return response["Body"].read()

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with _handle_client_error():
            return self._client.get_object(**self._client_kwargs)["Body"].read()

    def _flush(self, buffer):
        """
        Flush the write buffers of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
        """
        with _handle_client_error():
            self._client.put_object(Body=buffer.tobytes(), **self._client_kwargs)


class S3BufferedIO(_ObjectBufferedIOBase):
    """Buffered binary S3 Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode or
            awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to execute the
            given calls.
        storage_parameters (dict): Boto3 Session keyword arguments.
            This is generally AWS credentials and configuration.
            This dict should contain two sub-dicts:
            'session': That pass its arguments to "boto3.session.Session";
            'client': That pass its arguments to "boto3.session.Session.client".
            May be optional if already configured on host.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __slots__ = ("_upload_args",)

    _RAW_CLASS = S3RawIO

    #: Minimal buffer_size in bytes (S3 multipart upload minimal part size)
    MINIMUM_BUFFER_SIZE = 5242880

    def __init__(self, *args, **kwargs):
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)
        if self._writable:
            self._upload_args = self._client_kwargs.copy()

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        if "UploadId" not in self._upload_args:
            with _handle_client_error():
                self._upload_args["UploadId"] = self._client.create_multipart_upload(
                    **self._client_kwargs
                )["UploadId"]

        response = self._workers.submit(
            self._client.upload_part,
            Body=self._get_buffer().tobytes(),
            PartNumber=self._seek,
            **self._upload_args,
        )

        self._write_futures.append(dict(response=response, PartNumber=self._seek))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        for part in self._write_futures:
            part["ETag"] = part.pop("response").result()["ETag"]

        with _handle_client_error():
            try:
                self._client.complete_multipart_upload(
                    MultipartUpload={"Parts": self._write_futures},
                    UploadId=self._upload_args["UploadId"],
                    **self._client_kwargs,
                )
            except _ClientError:
                self._client.abort_multipart_upload(
                    UploadId=self._upload_args["UploadId"], **self._client_kwargs
                )
                raise
