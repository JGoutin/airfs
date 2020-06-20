"""Sphinx documentation """

# Path setup
import os
from os.path import abspath, dirname
import sys

SETUP_PATH = abspath(dirname(dirname(__file__)))
sys.path.insert(0, SETUP_PATH)


# Get Package info from setup.py
from setup import PACKAGE_INFO  # noqa: E402

SPHINX_INFO = PACKAGE_INFO["command_options"]["build_sphinx"]


# Prepare environment for ReadTheDocs
if os.environ.get("READTHEDOCS"):
    # Install dependencies
    from subprocess import Popen  # nosec

    current_dir = os.getcwd()
    os.chdir(SETUP_PATH)
    try:
        Popen(  # nosec
            (sys.executable, "-m", "pip", "install", "-e", ".[all]")
        ).communicate()
    finally:
        os.chdir(current_dir)


# Project information
project = SPHINX_INFO["project"][1]
copyright = SPHINX_INFO["copyright"][1]
author = PACKAGE_INFO["author"]
version = SPHINX_INFO["version"][1]
release = SPHINX_INFO["release"][1]


# General configuration
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


# Options for HTML output
html_theme = "sphinx_rtd_theme"


# Options for HTMLHelp output
htmlhelp_basename = "%sdoc" % project


# Options for LaTeX output
latex_elements = {}
latex_documents = [
    (master_doc, "%s.tex" % project, "%s Documentation" % project, author, "manual")
]


# Options for manual page output
man_pages = [
    (master_doc, PACKAGE_INFO["name"], "%s Documentation" % project, [author], 1)
]


# Options for Texinfo output
texinfo_documents = [
    (
        master_doc,
        project,
        "%s Documentation" % project,
        author,
        project,
        PACKAGE_INFO["description"],
        "Miscellaneous",
    )
]
