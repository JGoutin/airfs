"""GitHub"""
from re import compile

ROOTS = (
    compile(r"https?://github\.com"),
    compile(r"https?://raw\.githubusercontent\.com"),
)
