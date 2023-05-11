"""Test airfs._core.cache."""
import pytest


def test_hash_name():
    """Test airfs._core.cache._hash_name."""
    from airfs._core.cache import _hash_name

    assert len(_hash_name("test")) == 64, "Hash length"


def test_cache(tmpdir):
    """Test cache functions."""
    import airfs._core.cache as cache
    from time import sleep

    value_short = dict(key1=1, key2="1")
    value_long = dict(key3="", key4=True)
    name_short = "short"
    name_long = "long"
    hash_long = cache._hash_name(name_long) + "l"
    hash_short = cache._hash_name(name_short) + "s"

    cache_dir = cache.CACHE_DIR
    cache.CACHE_DIR = str(tmpdir)

    long_expiry = cache.CACHE_LONG_EXPIRY
    short_expiry = cache.CACHE_SHORT_EXPIRY

    try:
        # Test simple set and get
        cache.set_cache(name_short, value_short)
        assert cache.get_cache(name_short) == value_short
        assert tmpdir.join(hash_short).check(file=1)
        cache.set_cache(name_long, value_long, long=True)
        assert cache.get_cache(name_long) == value_long
        assert tmpdir.join(hash_long).check(file=1)

        # Test short expired
        cache.CACHE_SHORT_EXPIRY = 1e-9
        sleep(0.01)
        with pytest.raises(cache.NoCacheException):
            cache.get_cache(name_short)
        assert not tmpdir.join(hash_short).check()
        assert cache.get_cache(name_long) == value_long

        # Test long expired
        cache.CACHE_SHORT_EXPIRY = 60
        cache.CACHE_LONG_EXPIRY = 1e-9
        cache.set_cache(name_short, value_short)
        sleep(0.01)
        with pytest.raises(cache.NoCacheException):
            cache.get_cache(name_long)
        assert not tmpdir.join(hash_long).check()
        assert cache.get_cache(name_short) == value_short

        # Test clean up
        cache.set_cache(name_long, value_long, long=True)
        sleep(0.01)
        cache.clear_cache()
        assert not tmpdir.join(hash_long).check()
        assert tmpdir.join(hash_short).check(file=1)

        cache.CACHE_SHORT_EXPIRY = 1e-9
        cache.clear_cache()
        assert not tmpdir.join(hash_short).check()

    finally:
        cache.CACHE_DIR = cache_dir
        cache.CACHE_LONG_EXPIRY = long_expiry
        cache.CACHE_SHORT_EXPIRY = short_expiry
