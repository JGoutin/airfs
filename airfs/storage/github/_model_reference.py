"""Wildcard reference object"""
from airfs._core.exceptions import ObjectNotFoundError
from airfs.storage.github._model_base import GithubObject
from airfs.storage.github._model_git import Branch, Commit, Tag, Tree


class Reference(GithubObject):
    """
    Can be any valid Git reference like tags, branches, commits or HEAD.

    This class lazily replaces itself with the relevant class once detected.
    """

    KEY = "ref"  # Replaced by the relevant key on evaluation
    STRUCT = Tree

    # Symlink value is dynamically set, but class value must not be None
    # (Cf. "airfs.storage.github._GithubSystem.islink")
    SYMLINK = ""

    @classmethod
    def _get_ref(cls, client, spec):
        """
        Update spec with the reference after finding which GitHub object class it is.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            dict: Raw target object header.
        """
        try:
            ref = spec["ref"]
        except KeyError:
            # Already evaluated
            return

        if ref == "HEAD":
            obj_cls = DefaultBranch
            key = obj_cls.KEY
            headers = None

        else:
            if len(ref) == 40:
                # Likely a commit full length hash
                classes = (Commit, Branch, Tag)
            else:
                classes = (Branch, Tag, Commit)

            for obj_cls in classes:
                obj_spec = spec.copy()
                key = obj_cls.KEY
                obj_spec[key] = ref
                try:
                    headers = client.get(obj_cls.HEAD.format(**obj_spec))[0]
                    break
                except ObjectNotFoundError:
                    continue
            else:
                raise ObjectNotFoundError(spec["full_path"])

        spec["object"] = obj_cls
        spec[key] = ref
        del spec["ref"]

        return headers

    @classmethod
    def list(cls, client, spec):
        """
        Returns "list" result for the detected "_GithubObject" subclass.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Yields:
            tuple: object name str, object header dict, has content bool
        """
        cls._get_ref(client, spec)
        return spec["object"].list(client, spec)

    @classmethod
    def head(cls, client, spec, **_):
        """
        Returns "head" result for the detected "_GithubObject" subclass.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            _GithubObject subclass instance: Object headers.
        """
        headers = cls._get_ref(client, spec)
        model = spec["object"]
        if headers:
            headers = model.set_header(headers)
        return model.head(client, spec, headers)

    @classmethod
    def read_link(cls, client, spec):
        """
        Returns "read_link" result for the detected "_GithubObject" subclass.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            str: Path.
        """
        cls._get_ref(client, spec)
        return spec["object"].read_link(client, spec)


class DefaultBranch(GithubObject):
    """Default Git branch"""

    STRUCT = Tree
    SYMLINK = "github://{owner}/{repo}/branches/{branch}"

    @classmethod
    def _get_branch(cls, client, spec):
        """
        Update spec with branch.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
        """
        if "branch" not in spec:
            spec["branch"] = client.get("/repos/{owner}/{repo}".format(**spec))[0][
                "default_branch"
            ]

    @classmethod
    def list(cls, client, spec):
        """
        List objects of this GitHub class matching the spec.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Yields:
            tuple: object name str, object header dict, has content bool
        """
        cls._get_branch(client, spec)
        return Branch.list(client, spec)

    @classmethod
    def head(cls, client, spec, **_):
        """
        Head the object of this GitHub class matching the spec.

        Returns a dict like object that can retrieve keys from this object response or
        its parents.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            Branch instance: Branch headers.
        """
        cls._get_branch(client, spec)
        return Branch.head(client, spec)
