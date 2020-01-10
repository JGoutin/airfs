# coding=utf-8
"""Python "Remote File Systems" library

Copyright 2020 J.Goutin
Copyright 2018-2019 Accelize

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
__version__ = '1.4.0'
__copyright__ = "Copyright 2020 J.Goutin"
__licence__ = "Apache 2.0"

# Adds names to public interface
# Shadowing "open" built-in name is done to provides "airfs.open" function
from airfs._core.functions_io import cos_open as open
from airfs._core.functions_os import (
    listdir, lstat, makedirs, mkdir, remove, rmdir, scandir, stat, unlink)
from airfs._core.functions_os_path import (
    exists, getctime, getmtime, getsize, isabs, isdir, isfile, islink, ismount,
    relpath, samefile, splitdrive)
from airfs._core.functions_shutil import copy, copyfile
from airfs._core.storage_manager import mount

__all__ = list(sorted((
    # Standard library "io"
    'open',

    # Standard library "os"
    'listdir', 'lstat', 'makedirs', 'mkdir', 'remove', 'rmdir', 'scandir',
    'stat', 'unlink',

    # Standard library "os.path"
    'exists', 'getctime', 'getmtime', 'getsize', 'isabs', 'isdir', 'isfile',
    'islink', 'ismount', 'relpath', 'samefile', 'splitdrive',

    # Standard library "shutil"
    'copy', 'copyfile',

    # airfs
    'mount',)))

# Makes cleaner namespace
for _name in __all__:
    locals()[_name].__module__ = __name__
locals()['open'].__qualname__ = 'open'
locals()['open'].__name__ = 'open'
del _name
