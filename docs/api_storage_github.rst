airfs.storage.github
====================

GitHub as a read-only file-system.

.. versionadded:: 1.5.0

Mount
-----

The GitHub storage does not require to be mounted prior to being used.

It can be mounted with an "personal access token" to access private repositories and to
increase the API rate limit (Which is very limited in unauthenticated mode). Private
repositories access requires a token with the `repo` scope, public repositories access
does not require to select a scope.

.. code-block:: python

    import airfs

    # Mount GitHub with an API token
    airfs.mount(
        storage='github',
        storage_parameters=dict(
            token='my_token',
        )
    )

    # Call of airfs on an GitHub object.
    with airfs.open('github://my_organization/my_repo/HEAD/my_object', 'rt') as file:
        text = file.read()

Limitation
~~~~~~~~~~

Only one GitHub configuration can be mounted simultaneously.

Usage
-----

With the GitHub storage, it is possible to navigate in any repository like any local
git repository. It is possible to navigate in any branch, tag or commit but also in the
current branch, source codes archives, releases and releases assets.

The storage supports common GitHub URLs, and some specific shortcuts.

For instance with the current project GitHub repository:

.. code-block:: python

    # Listing the main branch files
    airfs.listdir("https://github.com/JGoutin/airfs/HEAD")

    # Listing a specific branch files
    airfs.listdir("https://github.com/JGoutin/airfs/branches/master")

    # Listing a specific tag files
    airfs.listdir("https://github.com/JGoutin/airfs/tags/1.4.0")

    # Listing download published with the latest
    airfs.listdir("https://github.com/JGoutin/airfs/releases/latest/assets")

    # Listing download published with a specific release
    airfs.listdir("https://github.com/JGoutin/airfs/releases/tag/1.4.0/assets")

    # Listing all source code archives for tags and branches
    airfs.listdir("https://github.com/JGoutin/airfs/archive")

    # Getting the size of the latest release source code archive
    airfs.getsize(
        "https://github.com/JGoutin/airfs/releases/latest/archive/source_code.tar.gz")

Many references are handled like symlinks to more precises reference. This feature help
in the repositories navigation, but can also be used to get extra information:

.. code-block:: python

    from os.path import basename

    # Getting the name of the current branch
    basename(airfs.readlink("https://github.com/JGoutin/airfs/HEAD"))

    # Getting the commit of the a specific branch
    basename(airfs.readlink("https://github.com/JGoutin/airfs/branches/master"))

    # Getting the commit of the a specific tag
    basename(airfs.readlink("https://github.com/JGoutin/airfs/tags/1.4.0"))

    # Getting the tag of the latest release
    basename(airfs.readlink("https://github.com/JGoutin/airfs/releases/latest"))

This is just a subset of what is possible, read next sections for a detailed description
of the files and directories structure.

GitHub API Rate limit
---------------------

GitHub API calls are limited by a rate limit.

By default, if the rate limit is reached, Airfs waits until the limit reset.
To raise an exception instead, set the `wait_rate_limit` argument to `False` in
`storage_parameters` when mounting.

Airfs uses the GitHub API v3 (REST API) because it allows unauthenticated requests.

Therefore, using authentication with an API token when mounting the storage allow to
have a greater rate limit than using the unauthenticated default mount.

Airfs does its best to reduce API rate limit usage (using GitHub conditional requests
mechanism, caches and lazy evaluation).

Supported paths and URLs
------------------------

Variables
~~~~~~~~~

Definitions of all variables used paths and URLs in following sections:

* `:asset_name`: Filename of a release asset.
* `:branch`: Git branch name.
* `:dir_path`: Path of a directory in the Git tree. Git tree root is used if not
  specified.
* `:file_path`: Path of a file (or blob) in the Git tree.
* `:owner`: Repository owner name (User or Organization)
* `:path`: Path of a file or directory in the Git tree. Git tree root is used if not
  specified.
* `:ref`: Git reference that can be `HEAD`, a branch name, a tag name or a commit ID.
* `:repo`: Repository name
* `:tag`: Git tag name.

Files and directory structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To allow to view GitHub as a file-system, airfs provides a specific directory structure.

This structure is done to be as compatible as possible with URLs used to navigate on
the GitHub website itself.

This structure also add some extra paths and symbolic link relationships that are not
available on the GitHub website itself, theses path are commented bellow.

The parameters used in the structure are the following:

* `:asset_name`: A GitHub release asset/download filename. GitHub also provides
  `.tag.gz` and `.zip` archives for each releases that does not count as assets.
* `:branch`: A Git branch.
* `:path`: The path to any file or directory inside the repository.
* `:owner`: The GitHub user or organization.
* `:ref`: A Git reference tha can be a branch, a commit or a tag.
* `:repo`: The repository.
* `:sha`: A Git commit SHA.
* `:tag`: A Git tag.

The structure is the following:

* **`:owner`**

  * **`:repo`**

    * **archive**

      * **`:ref`.zip**
      * **`:ref`.tar.gz**

    * **blob** [8]_

      * **`:ref`** [1]_

        * **`:path`**

    * **branches**

      * **`:branch`** [1]_

        * **`:path`** [7]_

    * **commits**

      * **`:sha`**

        * **`:path`** [7]_

    * **HEAD** [2]_ [4]_

      * **`:path`**

    * **refs** [4]_

      * **heads**

        * **`:branch`** [1]_

          * **`:path`**

      * **tags**

        * **`:tag`** [1]_

          * **`:path`**

    * **releases**

      * **tag**

        * **`:tag`**

          * **source_code.zip** [5]_
          * **source_code.tar.gz** [5]_
          * **assets** [6]_

            * **`:asset_name`**

          * **tree** [7]_

            * **`:path`**

    * **latest** [3]_

      * **source_code.zip** [5]_
      * **source_code.tar.gz** [5]_
      * **assets** [6]_

        * **`:asset_name`**

      * **tree** [7]_

        * **`:path`**

    * **download**

      * **`:tag`**

        * **`:asset_name`**

    * **tags**

      * **`:tag`** [1]_

        * **`:path`** [7]_

    * **tree** [8]_

      * **`:ref`** [1]_

        * **`:path`**

.. [1] Git references that are not a commit also count as a symbolic link to the
       associated commit (`github://:owner/:repo/commits`).
.. [2] The HEAD (default branch) also count as a symbolic link to the associated branch.
       (`github://:owner/:repo/releases/branch/:branch`).
.. [3] The latest release also count as a symbolic link to the associated tagged release
       (`github://:owner/:repo/releases/tag/:tag`).
.. [4] `HEAD`, `refs/heads` and `refs/tags` extra paths follow the Git internal
       structure.
.. [5] Releases provide an extra path for direct access to the associated archives.
.. [6] Releases provide an extra path for direct access to the downloadable assets.
.. [7] The structure provides many extra paths allowing a direct access to the
       associated repository files and directories.
.. [8] `tree` and `blob` returns the same result and are both existing to allow GitHub
       URL compatibility. Files and directories can be used in both cases.

GitHub URLs
~~~~~~~~~~~

Airfs provides a specific `github://` scheme but also supports common GitHub URLs:

* `https://github.com/:owner`
* `https://github.com/:owner/:repo`
* `https://github.com/:owner/:repo/archive/:ref.zip`
* `https://github.com/:owner/:repo/archive/:ref.tar.gz`
* `https://github.com/:owner/:repo/branches`
* `https://github.com/:owner/:repo/blob/:ref/:path`
* `https://github.com/:owner/:repo/commits`
* `https://github.com/:owner/:repo/releases`
* `https://github.com/:owner/:repo/releases/latest`
* `https://github.com/:owner/:repo/releases/tag/:tag`
* `https://github.com/:owner/:repo/releases/download/:tag/:asset_name`
* `https://github.com/:owner/:repo/tags`
* `https://github.com/:owner/:repo/tree/:ref/:path`
* `https://raw.githubusercontent.com/:owner/:repo/:ref/:path` (Redirect to
  `github://:owner/:repo/tree/:ref/:path`)

Files objects classes
---------------------

.. automodule:: airfs.storage.github
   :members:
   :inherited-members:

.. toctree::
   :maxdepth: 2
