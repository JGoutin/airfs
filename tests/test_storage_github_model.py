"""Test airfs.storage.github models."""


def test_get_client_kwargs():
    """Test get_model."""
    from airfs.storage.github import _GithubSystem
    from airfs.storage.github._model import Repo, Owner
    from airfs.storage.github._model_reference import DefaultBranch
    from airfs.storage.github._model_archive import Archive
    from airfs.storage.github._model_git import Tag, Tree, Commit, Branch
    from airfs.storage.github._model_release import (
        Release,
        ReleaseArchive,
        ReleaseAsset,
        ReleaseDownload,
        LatestRelease,
    )

    system = _GithubSystem()
    get_client_kwargs = system.get_client_kwargs

    # Owner
    spec = get_client_kwargs("my_owner")
    assert spec["object"] == Owner
    assert spec["content"] == Repo
    assert spec["owner"] == "my_owner"

    # Repos
    spec = get_client_kwargs("my_owner/my_repo")
    assert spec["object"] == Repo
    assert isinstance(spec["content"], dict)
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"

    # Archives
    spec = get_client_kwargs("my_owner/my_repo/archive")
    assert spec["object"] == Repo
    assert spec["content"] == Archive
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert "archive" not in spec

    spec = get_client_kwargs("my_owner/my_repo/archive/my_ref.tar.gz")
    assert spec["object"] == Archive
    assert spec["content"] == Archive
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["archive"] == "my_ref.tar.gz"

    # Branches/Commits/Tags
    for path, model_cls in (
        ("branches", Branch),
        ("refs/heads", Branch),
        ("commits", Commit),
        ("tags", Tag),
        ("refs/tags", Tag),
    ):
        spec = get_client_kwargs(f"my_owner/my_repo/{path}")
        assert spec["object"] == Repo
        assert spec["content"] == model_cls
        assert spec["owner"] == "my_owner"
        assert spec["repo"] == "my_repo"
        assert model_cls.KEY not in spec

        spec = get_client_kwargs(f"my_owner/my_repo/{path}/my_ref")
        assert spec["object"] == model_cls
        assert spec["content"] == Tree
        assert spec["owner"] == "my_owner"
        assert spec["repo"] == "my_repo"
        assert spec[model_cls.KEY] == "my_ref"
        assert "path" not in spec

        spec = get_client_kwargs(f"my_owner/my_repo/{path}/my_ref/my_dir/my_file")
        assert spec["object"] == Tree
        assert spec["content"] == Tree
        assert spec["owner"] == "my_owner"
        assert spec["repo"] == "my_repo"
        assert spec[model_cls.KEY] == "my_ref"
        assert spec["path"] == "my_dir/my_file"

    # HEAD
    spec = get_client_kwargs("my_owner/my_repo/HEAD")
    assert spec["object"] == DefaultBranch
    assert spec["content"] == Tree
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert "ref" not in spec

    spec = get_client_kwargs("my_owner/my_repo/HEAD/my_dir/my_file")
    assert spec["object"] == Tree
    assert spec["content"] == Tree
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["path"] == "my_dir/my_file"
    assert "ref" in spec

    # Release
    spec = get_client_kwargs("my_owner/my_repo/releases/tag")
    assert spec["object"] == Repo
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert "tag" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/tag/my_ref")
    assert spec["object"] == Release
    assert isinstance(spec["content"], dict)
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"

    spec = get_client_kwargs("my_owner/my_repo/releases/tag/my_ref/tree")
    assert spec["object"] == Release
    assert spec["content"] == Tree
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"
    assert "path" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/tag/my_ref/tree/my_dir/my_file")
    assert spec["object"] == Tree
    assert spec["content"] == Tree
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"
    assert spec["path"] == "my_dir/my_file"

    spec = get_client_kwargs("my_owner/my_repo/releases/tag/my_ref/assets")
    assert spec["object"] == Release
    assert spec["content"] == ReleaseAsset
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"
    assert "asset" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/download/my_ref")
    assert spec["object"] == ReleaseDownload
    assert spec["content"] == ReleaseAsset
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"
    assert "asset" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/tag/my_ref/assets/my_asset")
    assert spec["object"] == ReleaseAsset
    assert spec["content"] == ReleaseAsset
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"
    assert spec["asset"] == "my_asset"

    spec = get_client_kwargs("my_owner/my_repo/releases/download/my_ref/my_asset")
    assert spec["object"] == ReleaseAsset
    assert spec["content"] == ReleaseAsset
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"
    assert spec["asset"] == "my_asset"

    spec = get_client_kwargs("my_owner/my_repo/releases/tag/my_ref/archive")
    assert spec["object"] == Release
    assert spec["content"] == ReleaseArchive
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"
    assert "archive" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/tag/my_ref/archive/my_ref.zip")
    assert spec["object"] == ReleaseArchive
    assert spec["content"] == ReleaseArchive
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["tag"] == "my_ref"
    assert spec["archive"] == "my_ref.zip"

    # Latest
    spec = get_client_kwargs("my_owner/my_repo/releases/latest")
    assert spec["object"] == LatestRelease
    assert isinstance(spec["content"], dict)
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert "ref" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/latest/tree")
    assert spec["object"] == LatestRelease
    assert spec["content"] == Tree
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert "tag" not in spec
    assert "path" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/latest/tree/my_dir/my_file")
    assert spec["object"] == Tree
    assert spec["content"] == Tree
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["path"] == "my_dir/my_file"
    assert "tag" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/latest/assets")
    assert spec["object"] == LatestRelease
    assert spec["content"] == ReleaseAsset
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert "tag" not in spec
    assert "asset" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/latest/assets/my_asset")
    assert spec["object"] == ReleaseAsset
    assert spec["content"] == ReleaseAsset
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert spec["asset"] == "my_asset"
    assert "tag" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/latest/archive")
    assert spec["object"] == LatestRelease
    assert spec["content"] == ReleaseArchive
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert "tag" not in spec
    assert "archive" not in spec

    spec = get_client_kwargs("my_owner/my_repo/releases/latest/archive/source.zip")
    assert spec["object"] == ReleaseArchive
    assert spec["content"] == ReleaseArchive
    assert spec["owner"] == "my_owner"
    assert spec["repo"] == "my_repo"
    assert "tag" not in spec
    assert spec["archive"] == "source.zip"
