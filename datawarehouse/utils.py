import hashlib

from inveniautils.stream import SeekableStream


class ReadAsBytes:
    """Byte read wrapper for the SeekableStream.
    Warning: Moving the source stream between processing will lead to errors.
    """

    def __init__(self, stream: SeekableStream):
        self._stream = stream
        self._original_position = None
        self._buf = b""

        if self._stream.is_bytes:
            self._reader = lambda size: self._stream.read(size)
        else:
            self._reader = lambda size: self._stream.read(size).encode()

    def read(self, size: int = -1) -> bytes:
        # Warning: this assumes the source stream didn't move
        if len(self._buf) < size:
            temp = self._buf + self._reader(size)
        else:
            temp = self._buf
        self._buf = temp[size:]
        return temp[:size]

    def __enter__(self):
        self._original_position = self._stream.tell()
        self._stream.seek(0)
        return self

    def __exit__(self, type, value, traceback):
        self._stream.seek(self._original_position)


def get_md5(file: SeekableStream) -> str:
    """Computes the MD5 hash for a source file."""
    file_hash = hashlib.md5()
    # hash in chunks, MD5 has 128-Byte digest blocks
    size = 8192
    with ReadAsBytes(file) as bytes_stream:
        while chunk := bytes_stream.read(size):
            file_hash.update(chunk)

    return file_hash.hexdigest()
