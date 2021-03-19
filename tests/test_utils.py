import hashlib
from random import randint

from inveniautils.stream import SeekableStream

from datawarehouse.utils import ReadAsBytes, get_md5


def test_read_as_bytes():
    # show that the encoded bytes is longer than the string.
    input_string = "Γειά σου Κόσμε" * 100
    input_bytes = input_string.encode()
    assert len(input_string) < len(input_bytes)

    # test using an input string stream
    stream = SeekableStream(input_string)
    read_bytes = b""
    with ReadAsBytes(stream) as bytes_stream:
        # show that ReadAsBytes reads the correct byte size, not string size
        while chunk := bytes_stream.read(20):
            assert len(chunk) == 20
            read_bytes += chunk
    # show that the total read bytes is accurate
    assert read_bytes == input_bytes
    # rewinds original stream
    assert stream.tell() == 0

    # test using an input byte stream
    stream = SeekableStream(input_bytes)
    read_bytes = b""
    with ReadAsBytes(stream) as bytes_stream:
        # show that ReadAsBytes reads the correct byte size, not string size
        while chunk := bytes_stream.read(20):
            assert len(chunk) == 20
            read_bytes += chunk
    # show that the total read bytes is accurate
    assert read_bytes == input_bytes
    # rewinds original stream
    assert stream.tell() == 0


def test_get_md5():
    rand_str = "".join(chr(randint(0, 255)) for _ in range(123456))
    expected = hashlib.md5(rand_str.encode()).hexdigest()

    # string stream
    stream = SeekableStream(rand_str, meta1=1, meta2="2")
    assert get_md5(stream) == expected

    # byte stream
    stream = SeekableStream(rand_str.encode(), meta1=1, meta2="2", meta3=True)
    assert get_md5(stream) == expected

    # string stream
    stream = SeekableStream(rand_str + "3", meta1=1, meta2="2")
    assert get_md5(stream) != expected
