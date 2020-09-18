"""Sphinx documentation """

import os
from os.path import abspath, dirname
import sys

SETUP_PATH = abspath(dirname(dirname(__file__)))
sys.path.insert(0, SETUP_PATH)
from setup import PACKAGE_INFO  # noqa: E402

SPHINX_INFO = PACKAGE_INFO["command_options"]["build_sphinx"]

if os.environ.get("READTHEDOCS"):
    # Prepare environment for ReadTheDocs
    from subprocess import Popen  # nosec

    current_dir = os.getcwd()
    os.chdir(SETUP_PATH)
    try:
        Popen(  # nosec
            (sys.executable, "-m", "pip", "install", "-e", ".[all]")
        ).communicate()
    finally:
        os.chdir(current_dir)

project = SPHINX_INFO["project"][1]
copyright = SPHINX_INFO["copyright"][1]
author = PACKAGE_INFO["author"]
version = SPHINX_INFO["version"][1]
release = SPHINX_INFO["release"][1]

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.coverage",
    "sphinx.ext.viewcode",
]
source_suffix = ".rst"
master_doc = "index"
language = "en"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
pygments_style = "default"
html_theme = "sphinx_rtd_theme"
htmlhelp_basename = f"{project}doc"
latex_elements = {}  # type: ignore
latex_documents = [
    (master_doc, f"{project}.tex", f"{project} Documentation", author, "manual")
]
man_pages = [
    (master_doc, PACKAGE_INFO["name"], f"{project} Documentation", [author], 1)
]
texinfo_documents = [
    (
        master_doc,
        project,
        f"{project} Documentation",
        author,
        project,
        PACKAGE_INFO["description"],
        "Miscellaneous",
    )
]
