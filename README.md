[![Linux Build Status](https://travis-ci.org/Accelize/pycosio.svg?branch=master)](https://travis-ci.org/Accelize/pycosio)
[![Windows Build status](https://ci.appveyor.com/api/projects/status/g4n3jdk2a5sx0cp3?svg=true)](https://ci.appveyor.com/project/accelize-application/pycosio)
[![codecov](https://codecov.io/gh/Accelize/pycosio/branch/master/graph/badge.svg)](https://codecov.io/gh/Accelize/pycosio)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/0c9fc64f5fe94defac90140d769e1de3)](https://www.codacy.com/app/Accelize/pycosio?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Accelize/pycosio&amp;utm_campaign=Badge_Grade)
[![Documentation Status](https://readthedocs.org/projects/pycosio/badge/?version=latest)](https://pycosio.readthedocs.io/en/latest/?badge=latest)

Pycosio brings standard Python I/O to cloud objects by providing:

* Cloud objects classes with standards full featured ``io.RawIOBase`` and
  ``io.BufferedIOBase`` interfaces.
* Standard library functions equivalent to handle cloud objects and local files
  transparently:
  ``open``, ``copy``, ``getmtime``, ``getsize``, ``isfile``,
  ``relpath``

Buffered cloud objects also support following features:

* Buffered asynchronous writing of object of any size.
* Buffered asynchronous preloading in read mode.
* Blocking write or read based on memory usage limitation.
* Bandwidth optimization using parallels connections.

Example of code:

```python
import pycosio

# Open an object on AWS S3 as text for reading
with pycosio.open('s3://my_bucket/text.txt', 'rt') as file:
    text = file.read()

# Open an object on AWS S3 as binary for writing
with pycosio.open('s3://my_bucket/data.bin', 'wb') as file:
    file.write(b'binary_data')

# Copy file from local file system to OpenStack Swift
pycosio.copy(
    'my_file',
    'https://objects.mycloud.com/v1/12345678912345/my_container/my_file')

# Get size of a file over internet
pycosio.getsize('https://example.org/file')
>>> 956

```

For more information, read the [Pycosio documentation](https://pycosio.readthedocs.io).
