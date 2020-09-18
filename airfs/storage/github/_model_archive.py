"""GitHub Tarball/Zipball archives objects"""
from airfs.storage.github._model_base import GithubObject
from airfs.storage.github._model_git import Branch, Tag
from airfs.storage.github._model_reference import Reference


class Archive(GithubObject):
    """Git tree archive"""

    KEY = "archive"
    GET = "https://github.com/{owner}/{repo}/archive/{archive}"
    HEAD_KEYS = {"Content-Type", "Content-Length"}
    HEAD_FROM = {"pushed_at": Reference, "sha": Reference}

    @classmethod
    def list(cls, client, spec, first_level=False):
        """
        List archives for all branches and tags.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
            first_level (bool): It True, returns only first level objects.

        Returns:
            generator of tuple: object name str, object header dict, has content bool
        """
        cls._raise_if_not_dir(not spec.get("archive"), spec, client)

        for parent in (Tag, Branch):
            response = client.get(parent.LIST.format(**spec))[0]
            key = parent.LIST_KEY
            parent_key = parent.KEY
            for ref in response:
                ref_spec = spec.copy()
                ref_spec[parent_key] = ref_name = ref[key]
                ref_head = parent.set_header(ref)
                for ext in (".tar.gz", ".zip"):
                    name = ref_name + ext
                    yield name, cls(client, ref_spec, ref_head, name), False

    @classmethod
    def head_obj(cls, client, spec):
        """
        Get archive headers.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            dict: Object headers.
        """
        url = cls.GET.format(**spec)
        headers = ""
        # Sometime, Content-Length is missing from response, so retry until success
        while "Content-Length" not in headers:
            headers = client.request(url, method="HEAD").headers
        return cls.set_header(headers)

    def _update_spec_parent_ref(self, parent_key):
        """
        Update the spec with the parent reference.

        Args:
            parent_key (str): The parent key (parent_class.KEY).
        """
        name = self._spec["archive"]
        self._spec[parent_key] = name.rsplit(
            ".", 2 if name.lower().endswith(".tar.gz") else 1
        )[0]
