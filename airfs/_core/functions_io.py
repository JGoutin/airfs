"""Cloud object compatibles standard library 'io' equivalent functions"""
from contextlib import contextmanager
from io import open as io_open, TextIOWrapper

from airfs._core.storage_manager import get_instance
from airfs._core.functions_core import format_and_is_storage


@contextmanager
def cos_open(
    file,
    mode="r",
    buffering=-1,
    encoding=None,
    errors=None,
    newline=None,
    closefd=True,
    opener=None,
    storage=None,
    storage_parameters=None,
    unsecure=None,
    **kwargs
):
    """
    Open file and return a corresponding file object.

    Equivalent to "io.open" or builtin "open".

    File can also be binary opened file-like object.

    .. versionadded:: 1.0.0

    Args:
        file (path-like object or file-like object or int): File path, object URL,
            opened file-like object, or file descriptor.
        mode (str): mode in which the file is opened (default to 'rb').
            see "io.open" for all possible modes. Note that all modes may
            not be supported by all kind of file and storage.
        buffering (int): Set the buffering policy.
            -1 to use default behavior,
            0 to switch buffering off,
            1 to select line buffering (only usable in text mode),
            and an integer > 1 to indicate the size in bytes of a
            fixed-size chunk buffer.
            See "io.open" for more information.
        encoding (str): The name of the encoding used to
            decode or encode the file. This should only be used in text mode.
            See "io.open" for more information.
        errors (str):  Specifies how encoding and decoding errors
            are to be handled.
            This should only be used in text mode.
            See "io.open" for more information.
        newline (str): Controls how universal newlines mode works.
            This should only be used in text mode.
            See "io.open" for more information.
        closefd (bool): If closefd is False and a file descriptor rather than a filename
            was given, the underlying file descriptor will be kept open when the file
            is closed. Must be True elsewhere (the default) otherwise an error will be
            raised.
        opener: A callable used as custom opener.
            see the standard library "open()" documentation for more information.
            Not supported on storage objects.
        storage (str): Storage name.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
            Default to False.
        kwargs: Other arguments to pass to opened object.
            Note that theses arguments may not be compatible with all kind of file and
            storage.

    Returns:
        file-like object: opened file.

    Raises:
        OSError: If the file cannot be opened.
        FileExistsError: File open in 'x' mode already exists.
    """
    if hasattr(file, "read"):
        with _text_io_wrapper(file, mode, encoding, errors, newline) as wrapped:
            yield wrapped
        return

    file, file_is_storage = format_and_is_storage(file, storage=storage)
    if file_is_storage:
        if not closefd:
            raise NotImplementedError("Cannot use closefd=False with a storage")
        with get_instance(
            name=file,
            cls="raw" if buffering == 0 else "buffered",
            storage=storage,
            storage_parameters=storage_parameters,
            mode=mode,
            unsecure=unsecure,
            **kwargs
        ) as stream:
            with _text_io_wrapper(
                stream, mode=mode, encoding=encoding, errors=errors, newline=newline
            ) as wrapped:
                yield wrapped

    else:
        with io_open(
            file, mode, buffering, encoding, errors, newline, closefd, opener
        ) as stream:
            yield stream


@contextmanager
def _text_io_wrapper(stream, mode, encoding, errors, newline):
    """Wrap a binary stream to Text stream.

    Args:
        stream (file-like object): binary stream.
        mode (str): Open mode.
        encoding (str): Stream encoding.
        errors (str): Decoding error handling.
        newline (str): Universal newlines
    """
    if "t" in mode and not hasattr(stream, "encoding"):
        text_stream = TextIOWrapper(
            stream, encoding=encoding, errors=errors, newline=newline
        )
        yield text_stream
        text_stream.flush()

    else:
        yield stream
