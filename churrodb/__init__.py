import json
import uuid
import acidfs
import churro
import logging
import subprocess
import transaction
import collections.abc


log = logging.getLogger(__name__)


def unique_branch_name(prefix=""):
    if prefix:
        prefix = str(prefix) + "-"
    return "{prefix}{uuid}".format(prefix=prefix, uuid=uuid.uuid4())


class JsonCodec(churro.JsonCodec):
    def encode(self, obj, stream):
        json.dump(obj, stream, default=self.encode_hook, indent=4, sort_keys=True)

# monkey-patch churro's codec object
# so that object get serialized in a reproducable manner
# and therefore result in reproducable git oid (SHA1 hashes)
# also has the side-effect of improving "diffability" of JSON documents
churro.codec = JsonCodec()


def _save(self, fs):
    self._fs = fs
    path = churro.resource_path(self)
    if not fs.exists(path):
        fs.mkdir(path)
    for name, (type, obj) in self._contents.items():
        if obj is None:
            continue
        if type == 'folder':
            if obj is churro._removed:
                try:
                    fs.rmtree(churro.resource_path(self, name))
                except FileNotFoundError as why:
                    log.warn(str(why) + " (probably a subsequent call to flush)")
            else:
                obj._save(fs)
        else:
            fspath = churro.resource_path(self, name) + churro.CHURRO_EXT
            if obj is churro._removed:
                try:
                    fs.rm(fspath)
                except FileNotFoundError as why:
                    log.warn(str(why) + " (probably a subsequent call to flush)")
            else:
                churro.codec.encode(obj, fs.open(fspath, churro.ENCODE_MODE))
                obj._dirty = False
                obj._fs = fs
    fspath = '%s/%s' % (path, churro.CHURRO_FOLDER)
    churro.codec.encode(self, fs.open(fspath,churro. ENCODE_MODE))
    self._dirty = False

# monkey-patch _save method of PersistentFolder. original version
# contains a bug. multiple calls to flush() result in multiple calls
# to fs.rm() which causes acidfs to raise FileNotFoundError on subsequent
# flushes.
churro.PersistentFolder._save = _save


def idx_find_first(self, key, subindex=None):
    found = self.idx_find(key, subindex)
    if len(found) > 0:
        return found[0]
    return None


class ChurroDbAware(object):
    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        obj._churrodb = None
        return obj

    @property
    def churrodb(self):
        return self._churrodb

    @churrodb.setter
    def churrodb(self, value):
        self._churrodb = value
        for name, attr in self.__dict__.items():
            if name not in ["__parent__", "__instance__"]:
                self.register(attr)
        if hasattr(self, "__iter__"):
            for key, item in self.items():
                self.register(item)

    def register(self, obj):
        if hasattr(obj, "churrodb"):
            if self.churrodb is not None:
                obj.churrodb = self.churrodb

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key not in ["churrodb", "_churrodb"]:
            self.register(value)

    def __setitem__(self, name, other):
        super().__setitem__(name, other)
        self.register(other)

    def _load(self, name, type, cache=True):
        obj = super()._load(name, type, cache)
        self.register(obj)
        return obj


class IIndex(object):
    def idx_find(self, key, subindex=None):
        raise NotImplementedError

    def idx_update(self, data=None):
        raise NotImplementedError

    def idx_validate(self):
        raise NotImplementedError

    @property
    def idx(self):
        """
        :return: index implementation (e.g. dict) having
        implemented __setitem__ and __getitem__ methods
        """
        raise NotImplementedError


class ChurroDbRoot(ChurroDbAware, churro.PersistentFolder):
    pass


class ChurroDb(IIndex):
    def __init__(self, repo, head="HEAD", factory=None, **kwargs):
        self._path = repo
        self._head = head
        self._churro_kwargs = kwargs
        self._data = {}
        self._churro = None
        self.fs = None

        self.make_churro(repo, head, factory, **kwargs)
        self.refresh_data()

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        obj.named_indexes = {}
        return obj

    def make_churro(self, repo, head="HEAD", factory=None, **kwargs):
        if factory is None:
            factory = ChurroDbRoot
        self._churro = churro.Churro(repo, head, factory, **kwargs)
        self.fs = self._churro.fs
        root = self.root()
        if hasattr(root, "churrodb"):
            root.churrodb = self

    def refresh_data(self):
        root = self.root()
        for k, v in root.items():
            self._data[k] = v

    def refresh_dbroot(self):
        root = self.root()
        for k, v in self.items():
            root[k] = v

    def switch(self, branch="HEAD"):
        self.make_churro(self._path, head=branch)
        self.refresh_dbroot()

    def save(self):
        try:
            transaction.commit()
        except Exception as why:
            transaction.abort()
            if isinstance(why, acidfs.ConflictError):
                message = "trying to commit this transaction caused a conflict"
            elif isinstance(why, subprocess.CalledProcessError):
                message = "problem while writing transaction to git"
            else:
                message = "problem while trying to commit this transaction"

            log.error(message)

            conflict_branch = unique_branch_name("conflict")
            self.switch(conflict_branch)
            try:
                try:
                    transaction.commit()
                except Exception as why:
                    log.error(
                        "this doesn't look good... " +
                        "failed to write problematic changeset to branch '{branch}', "
                        .format(branch=conflict_branch) +
                        "exception was '{exception}'"
                        .format(exception=why))
                    raise why

                log.error(
                    "wrote problematic changeset to branch '{branch}', "
                    .format(branch=conflict_branch) +
                    "you have to resolve this yourself")
                raise why
            finally:
                self.switch("HEAD")

    def root(self):
        return self._churro.root()

    def flush(self):
        self._churro.flush()

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def __iter__(self):
        return self._data.__iter__()

    def items(self):
        return self._data.items()

    def __len__(self):
        return self._data.__len__()

    def __nonzero__(self):
        return self._data.__nonzero__()

    def __getitem__(self, name):
        return self._data.__getitem__(name)

    def get(self, name, default=None):
        return self._data.get(name, default)

    def __contains__(self, name):
        return self._data.__contains__(name)

    def __setitem__(self, name, other):
        self._data.__setitem__(name, other)
        self.root().__setitem__(name, other)

    def __delitem__(self, name):
        self._data.__delitem__(name)
        self.root().__delitem__(name)

    remove = __delitem__

    def pop(self, name, **kwargs):
        self._data.pop(name)
        self.root().pop(name, **kwargs)

    def init_index(self):
        self.root().init_index()
        self.refresh_data()

    def idx_update(self, *args, **kwargs):
        root = self.root()
        # if hasattr(root, "idx_update"):
        #     indexes = [root]
        # else:
        #     indexes = []
        # indexes.extend(self.indexes)
        # for idx in indexes:
        #     idx.idx_update(*args, **kwargs)
        if hasattr(root, "idx_update"):
            root.idx_update(*args, **kwargs)

    def idx_find(self, *args, **kwargs):
        root = self.root()

        if hasattr(root, "idx_find"):
            return root.idx_find(*args, **kwargs)

    idx_find_first = idx_find_first

    def idx_validate(self):
        pass

    def object_by_hash(self, hashstr, text_mode=True):
        git = subprocess.Popen(
            ["git", "show", hashstr],
            cwd=self._path, stdout=subprocess.PIPE, universal_newlines=text_mode)
        return churro.codec.decode(git.stdout)


class IndexUpdateError(Exception):
    pass


class IndexesFolder(ChurroDbAware, IIndex, churro.PersistentFolder):
    def __init__(self, parent):
        parent["_index"] = self

    def idx_find(self, key, subindex=None):
        if subindex is not None:
            if subindex not in self:
                raise Exception("there is no index called '" + subindex + "'")
            return self[subindex].idx_find(key)

        found = []
        for idx in self.values():
            found.extend(idx.idx_find(key))

        return found

    def idx_update(self, data=None):
        for idx in self.values():
            idx.idx_update(data)

    def idx_validate(self):
        for idx in self.values():
            idx.idx_validate()

    @property
    def idx(self):
        return self


class AbstractDictIndex(ChurroDbAware, IIndex, churro.PersistentDict):
    def idx_find(self, key, subindex=None):
        found = []
        found.extend(self.get(key, []))
        return found

    def idx_update(self, data=None):
        pass

    def idx_validate(self):
        pass

    @property
    def idx(self):
        return self


class DotLookupDictProxy(collections.abc.Mapping):
    def __init__(self, obj):
        assert hasattr(obj, "__getitem__")
        assert hasattr(obj, "__iter__")
        assert hasattr(obj, "__len__")

        self._dict = obj

    def __getitem__(self, key):
        """resolves dotted notation on key lookup (e.g. d["a.b.c"])"""
        path = list(reversed(key.split(".")))
        res = self._dict
        while len(path) > 0:
            keyp = path.pop()
            res = res.__getitem__(keyp)
        return res

    def __iter__(self):
        return self._dict.__iter__()

    def __len__(self):
        return self._dict.__len__()


class GitObjectProxy(collections.abc.Mapping, collections.abc.Iterator):
    def __init__(self, obj, key_mapper=None):
        assert hasattr(obj, "__getitem__")
        assert hasattr(obj, "__iter__")
        assert hasattr(obj, "__len__")

        self._obj = obj
        self._iterator = None
        self._dict = {}
        self._key_mapper = key_mapper

    def __getitem__(self, key):
        return self._dict.__getitem__(key)

    def __next__(self):
        key = None
        while key is None:
            key = self._iterator.__next__()
            value = self._obj.__getitem__(key)
            if self._key_mapper is not None:
                if hasattr(value, "__getitem__")\
                        and hasattr(value, "__iter__")\
                        and hasattr(value, "__len__"):
                    proxy_value = DotLookupDictProxy(value)
                else:
                    proxy_value = value

                if hasattr(self._key_mapper, "__func__"):
                    key = self._key_mapper.__func__(key, proxy_value)
                else:
                    key = self._key_mapper(key, proxy_value)

        self._dict[key] = value
        return key

    def __iter__(self):
        self._iterator = self._obj.__iter__()
        return self

    def __len__(self):
        return self._obj.__len__()


class GitObjectHashIndex(AbstractDictIndex):
    _inverse = churro.PersistentProperty()
    name = churro.PersistentProperty()
    supply = churro.PersistentProperty()
    auxiliary = churro.PersistentProperty()
    clear_before_update = churro.PersistentProperty()

    def __init__(
            self, inverse=False, clear_before_update=False,
            supply=None, name=None):
        self._inverse = inverse
        self._db = None
        self.name = name
        self.supply = supply
        self.clear_before_update = clear_before_update
        self.auxiliary = churro.PersistentDict()
        super().__init__()

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        obj._db = None
        return obj

    @property
    def churrodb(self):
        return self._db

    @churrodb.setter
    def churrodb(self, value):
        if value is not None:
            self._db = value
            if self.name is not None:
                self._db.named_indexes[self.name] = self

    def supply_index(self):
        if self.churrodb is None:
            return
        if self.supply is not None:
            if self.name is None:
                raise Exception("must provide name for this sub-index")
            return self.churrodb.named_indexes[self.supply]
        return None

    def idx_update(self, data=None, namespace=None):
        if self.churrodb is None:
            return
        supply_index = self.supply_index()
        if supply_index is not None:
            supply_index.idx_update(data, namespace=self.name)
            return

        db = self.churrodb
        db.flush()

        if namespace is not None:
            if namespace not in self.auxiliary:
                self.auxiliary[namespace] = churro.PersistentDict()
            target = self.auxiliary[namespace]
        else:
            target = self

        if self.clear_before_update:
            target.clear()

        seen_keys = []

        for key, value in data.items():
            resource_path = churro.resource_path(value)
            if not db.fs.isdir(resource_path):
                resource_path += churro.CHURRO_EXT

            hash = db.fs.hash(resource_path)

            if self._inverse:
                target_key = hash
                target_value = key
            else:
                target_key = key
                target_value = hash

            if target_key in seen_keys:
                raise IndexUpdateError(
                    "duplicate key '{key}' for values '{value_a}' and '{value_b}'l"
                        .format(key=target_key, value_a=target[target_key], value_b=target_value))

            seen_keys.append(target_key)
            target[target_key] = target_value

    def idx_find(self, key, subindex=None):
        found = self.get(key)

        if found is None:
            for k, v in self.auxiliary.items():
                found = v.get(key)
                if found is not None:
                    break

        if found is None:
            return []
        else:
            return [found]

    idx_find_first = idx_find_first


class GitDictKeyHashIndex(GitObjectHashIndex):
    dict_key = churro.PersistentProperty()

    def __init__(self, *args, **kwargs):
        try:
            self.dict_key = kwargs.pop("dict_key")
        except KeyError:
            self.dict_key = "id"

        super().__init__(*args, **kwargs)

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        obj.git_index_key_mapper = obj.mapper_factory()
        return obj

    def mapper_factory(self):
        def git_index_key_mapper(k, v):
            return v.get(self.dict_key)
        return git_index_key_mapper

    def idx_update(self, data=None):
        super().idx_update(GitObjectProxy(data, self.git_index_key_mapper))


class IndexMixin(ChurroDbAware, IIndex):
    index_factory = IndexesFolder
    session = None

    def init_index(self):
        self.index_factory(self)

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        obj.__dirty = False
        obj._index = None
        obj._db = None
        return obj

    @property
    def churrodb(self):
        return self._db

    @churrodb.setter
    def churrodb(self, value):
        if value is not None:
            self._db = value
            if hasattr(self.idx, "churrodb"):
                self.idx.churrodb = value

    def _session(self):
        if not self.session or self.session.closed:
            self.session = _IndexSession(self)
        return self.session

    @property
    def _dirty(self):
        return self.__dirty

    @_dirty.setter
    def _dirty(self, dirty):
        self.__dirty = dirty
        if dirty:
            self._session()

    @property
    def idx(self):
        index = None
        if self._index is not None:
            index = self._index
        elif "_index" in self:
            index = self["_index"]

        return index

    @idx.setter
    def idx(self, value):
        if "_index" in self:
            self["_index"] = value
        else:
            self._index = value

    def idx_find(self, key, subindex=None):
        return self.idx.idx_find(key, subindex)

    idx_find_first = idx_find_first

    def idx_update(self, data=None):
        self.idx.idx_update(data)

    def idx_validate(self):
        self.idx.idx_validate()


class GitIndexMixin(IndexMixin):
    index_factory = GitObjectHashIndex
    git_index_key_mapper = None

    def init_index(self):
        self.idx = self.index_factory(name=self.name, supply=self.supply)

    def __init__(self, idx_name=None, idx_supply=None, **kwargs):
        super().__init__(**kwargs)
        self.name = idx_name
        self.supply = idx_supply

    def idx_update(self, data=None):
        self.idx.idx_update(GitObjectProxy(data, self.git_index_key_mapper))


class _IndexSession(object):
    closed = False

    def __init__(self, obj):
        self.obj = obj
        transaction.get().join(self)
        transaction.get().addBeforeCommitHook(self.before_commit)

    def before_commit(self):
        self.obj.idx_update(self.obj)

    def set_dirty(self):
        self._dirty = True

    def close(self):
        self.closed = True

    def sortKey(self):
        return "churrodb-1-index-" + str(id(self))

    def abort(self, tx):
        """
        Part of datamanager API.
        """
        self.close()

    tpc_abort = abort

    def tpc_begin(self, tx):
        """
        Part of datamanager API.
        """

    def commit(self, tx):
        """
        Part of datamanager API.
        """

    def tpc_vote(self, tx):
        """
        Part of datamanager API.
        """
        self.obj.idx_validate()

    def tpc_finish(self, tx):
        """
        Part of datamanager API.
        """