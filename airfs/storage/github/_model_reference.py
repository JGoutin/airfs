"""Wildcard reference object"""
from airfs._core.exceptions import ObjectNotFoundError
from airfs.storage.github._model_base import GithubObject
from airfs.storage.github._model_git import Branch, Commit, Tag, Tree


class Reference(GithubObject):
    """
    Can be any valid Git reference like tags, branches, commits or HEAD.

    This class replaces itself with the relevant class.
    """

    @classmethod
    def _get_cls(cls, client, spec):
        """
        Get object class.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Object spec.

        Returns:
            _Model subclass: model.
        """
        ref = spec["keys"][0]

        if ref == "HEAD":
            spec["keys"].popleft()
            return DefaultBranch

        for obj_cls in (
            (Commit, Branch, Tag) if len(ref) == 40 else (Branch, Tag, Commit)
        ):
            obj_spec = spec.copy()
            obj_spec[obj_cls.KEY] = ref
            try:
                client.get(obj_cls.HEAD.format(**obj_spec))
            except ObjectNotFoundError:
                continue
            return obj_cls

        raise ObjectNotFoundError(path=spec["full_path"])

    @classmethod
    def head(cls, client, spec, headers=None):
        """
        Returns "head" result for the detected "_GithubObject" subclass.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
            headers (dict): Known header values. Missing values will be get lazily from
                parents.

        Returns:
            _GithubObject subclass instance: Object headers.
        """
        return cls._get_cls(client, spec).head(client, spec, headers)


class DefaultBranch(GithubObject):
    """Default Git branch"""

    REF = "HEAD"
    STRUCT = Tree
    SYMLINK = "https://github.com/{owner}/{repo}/branches/{branch}"

    @classmethod
    def _get_branch(cls, client, spec):
        """
        Update spec with branch.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
        """
        if "branch" not in spec:
            spec["ref"] = spec["branch"] = client.get(
                "/repos/{owner}/{repo}".format(**spec)
            )[0]["default_branch"]

    @classmethod
    def list(cls, client, spec, first_level=False):
        """
        List objects of this GitHub class matching the spec.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
            first_level (bool): It True, returns only first level objects.

        Yields:
            tuple: object name str, object header dict, has content bool
        """
        cls._get_branch(client, spec)
        return Branch.list(client, spec)

    @classmethod
    def head(cls, client, spec, headers=None):
        """
        Head the object of this GitHub class matching the spec.

        Returns a dict like object that can retrieve keys from this object response or
        its parents.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
            headers (dict): Known header values. Missing values will be get lazily from
                parents.

        Returns:
            Branch instance: Branch headers.
        """
        cls._get_branch(client, spec)
        return Branch.head(client, spec)
