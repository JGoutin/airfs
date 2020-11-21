"""Test Airfs configuration"""


def test_config_read_write(tmpdir):
    """Test configuration read and write"""
    import airfs._core.config as core_config
    import airfs.config as config
    import airfs._core.storage_manager as storage_manager

    config_file = core_config.CONFIG_FILE
    core_config.CONFIG_FILE = config._CONFIG_FILE = str(tmpdir.join("config.json"))

    mounted = set()

    def mount(storage=None, **_):
        """Mocked mount function"""
        mounted.add(storage)

    storage_manager_mount = storage_manager.mount
    defaults = storage_manager._DEFAULTS
    storage_manager._DEFAULTS = dict()
    storage_manager.mount = mount

    try:
        # Read not existing config
        assert core_config.read_config() is None
        assert config.get_mount("storage") is None
        storage_manager._user_mount()
        assert mounted == set()
        assert storage_manager._DEFAULTS == dict()

        # Write config
        config.set_mount("storage")
        options = dict(
            storage_parameters=dict(param=1), unsecure=True, extra_root="root"
        )
        config.set_mount("storage_with_options", **options)
        scope_options = dict(extra_root="scope")
        config.set_mount("storage", config_name="scope", **scope_options)

        # Read existing config
        assert core_config.read_config()
        assert config.get_mount("storage") == {}
        assert config.get_mount("storage_with_options") == options
        assert config.get_mount("storage", config_name="scope") == scope_options

        # Load config from storage manager
        storage_manager._user_mount()
        assert mounted == {"storage"}, "Mounted on load"
        assert storage_manager._DEFAULTS == {
            "storage": {},
            "storage_with_options": options,
        }, "Lazzy mount"

    finally:
        core_config.CONFIG_FILE = config._CONFIG_FILE = config_file
        storage_manager._DEFAULTS = defaults
        storage_manager.mount = storage_manager_mount


def test_config_directories():
    """Test directories selection"""
    import airfs._core.config as config
    import os
    import posixpath
    import ntpath

    class OsMock:
        """Mocked os module"""

        name = "posix"
        uid = 1000

        @classmethod
        def getuid(cls):
            """Mocked os.getuid function"""
            return cls.uid

    def expanduser(path):
        """Mocked os.path.expanduser function"""
        return path.replace("~", "/home/user")

    def expandvars(path):
        """Mocked os.path.expandvars function"""
        for name, value in (
            ("%APPDATA%", r"C:\Users\user\AppData\Roaming"),
            ("%LOCALAPPDATA%", r"C:\Users\user\AppData\Local"),
        ):
            path = path.replace(name, value)
        return path

    def getenv(_, default=None):
        """Mocked os.getenv function"""
        return default

    config.os = OsMock
    config.expanduser = expanduser
    config.expandvars = expandvars
    config.getenv = getenv
    config.join = posixpath.join

    try:
        # Linux standard user
        config_dir, cache_dir = config._init_paths()
        assert config_dir == "/home/user/.config/airfs"
        assert cache_dir == "/home/user/.cache/airfs"

        # Linux root user
        OsMock.uid = 0
        config_dir, cache_dir = config._init_paths()
        assert config_dir == "/etc/airfs"
        assert cache_dir == "/var/cache/airfs"

        # Windows
        OsMock.name = "nt"
        config.join = ntpath.join
        config_dir, cache_dir = config._init_paths()
        assert config_dir == r"C:\Users\user\AppData\Roaming\airfs"
        assert cache_dir == r"C:\Users\user\AppData\Local\airfs\cache"

    finally:
        config.os = os
        config.getenv = os.getenv
        config.expanduser = os.path.expanduser
        config.expandvars = os.path.expandvars
        config.join = os.path.join
