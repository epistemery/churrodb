import os
import json
import shutil
import acidfs
import churro
import logging
import churrodb
import unittest
import threading
import subprocess
import transaction
import unittest.mock

cwd = os.path.dirname(os.path.realpath(__file__))
churrodb_path = os.path.join(cwd, "testdata/churrodb")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Dummy(churro.Persistent):
    __module__ = "churrodb.tests"
    value = churro.PersistentProperty()

    def __init__(self, value=None):
        if value is not None:
            self.value = value

class TestDictIndex(churrodb.AbstractDictIndex):
    __module__ = "churrodb.tests"

    def idx_update(self, data=None):
        self.clear()
        if data is not None:
            for k, v in data.items():
                self[k] = "indexed"


class IndexedCollection(churrodb.IndexMixin, churro.PersistentFolder):
    __module__ = "churrodb.tests"
    index_factory = churrodb.IndexesFolder


class GitIndexedCollection(churrodb.GitIndexMixin, churro.PersistentFolder):
    __module__ = "churrodb.tests"
    _index = churro.PersistentProperty()

    def git_index_key_mapper(k, v):
        return k


class TestRootFactory(churrodb.IndexMixin, churro.PersistentFolder):
    __module__ = "churrodb.tests"
    pass


class TestTransactionManager(object):
    __module__ = "churrodb.tests"

    def sortKey(self):
        return str(id(self))

    def abort(self, tx):
        pass

    tpc_abort = abort

    def tpc_begin(self, tx):
        pass

    def commit(self, tx):
        pass

    def tpc_vote(self, tx):
        pass

    def tpc_finish(self, tx):
        pass


def object_by_hash(path, hashstr):
    git = subprocess.Popen(
        ["git", "show", hashstr],
        cwd=path, stdout=subprocess.PIPE, universal_newlines=True)
    return churro.codec.decode(git.stdout)


def read_json(path):
    with open(path, "r") as fh:
        data = json.load(fh)
    return data


def prepare_repo(path):
    if os.path.isdir(path):
        shutil.rmtree(path)


def remove_repo(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
        assert not os.path.isdir(path)


def get_branches(path):
    return subprocess.check_output(
        ["git", "branch"],
        cwd=path
    ).decode().strip().split("\n")


def threader(
        other=None,
        env=object(),
        action=lambda env: None,
        save=lambda env: None,
        finish=lambda env: None,
        catch=lambda env: None
):
    action(env)

    if other:
        other.start()
        other.join()

    try:
        save(env)
        finish(env)
    except Exception as why:
        if hasattr(env, "why"):
            env.exception = why
        catch(env)


class ChurroDbTests(unittest.TestCase):
    def setUp(self):
        self.churrodb_path = churrodb_path
        prepare_repo(self.churrodb_path)

    def tearDown(self):
        self.churrodb_path = churrodb_path
        remove_repo(self.churrodb_path)

    def test_transparent_access_to_churro_root(self):
        db = churrodb.ChurroDb(self.churrodb_path)
        t = transaction.begin()

        MockDb = unittest.mock.create_autospec(churrodb.ChurroDb)
        mock_db = MockDb(self.churrodb_path)

        prop_a = churro.Persistent()
        prop_b = churro.Persistent()
        prop_c = churro.Persistent()

        # __setitem__
        mock_db["a"] = prop_a
        db["a"] = prop_a
        db["b"] = prop_b
        db["c"] = prop_c
        keys = ["a", "b", "c"]
        values = [prop_a, prop_b, prop_c]

        # __nonzero__ (apparently Python 3 doesn't call __nonzero__ for bool()
        # we include it here for reasons of integrity
        # bool(mock_db)
        # self.assertTrue(mock_db.__nonzero__.called)
        self.assertTrue(db)
        self.assertTrue(db._data)
        self.assertTrue(db.root())
        # __len__
        len(mock_db)
        self.assertEqual(3, len(db))
        self.assertEqual(3, len(db._data))
        self.assertEqual(3, len(db.root()))
        # __getitem__
        mock_db["a"]
        self.assertIs(prop_a, db["a"])
        self.assertIs(prop_b, db["b"])
        self.assertIs(prop_c, db["c"])
        self.assertIs(prop_a, db._data["a"])
        self.assertIs(prop_b, db._data["b"])
        self.assertIs(prop_c, db._data["c"])
        self.assertIs(prop_a, db.root()["a"])
        self.assertIs(prop_b, db.root()["b"])
        self.assertIs(prop_c, db.root()["c"])
        # get
        mock_db.get("a")
        self.assertIs(prop_a, db.get("a"))
        self.assertIs(prop_b, db.get("b"))
        self.assertIs(prop_c, db.get("c"))
        # keys
        mock_db.keys()
        self.assertListEqual(keys, sorted(list(db.keys())))
        # values
        mock_db.values()
        for value in db.values():
            self.assertTrue(value in values)
        # __contains__
        "a" in mock_db
        for key in keys:
            self.assertTrue(key in db)
        # __iter__
        for k in mock_db:
            pass
        for key in db:
            keys.remove(key)
        self.assertEqual(0, len(keys))
        # items
        mock_db.items()
        for key, value in db.items():
            values.remove(value)
        self.assertEqual(0, len(values))
        # __delitem__
        del mock_db["a"]
        del db["a"]
        # pop
        mock_db.pop("a")
        db.pop("b")
        # remove
        mock_db.remove("a")
        db.remove("c")

        self.assertFalse("a" in db)
        self.assertFalse("b" in db)
        self.assertFalse("c" in db)
        self.assertFalse("a" in db._data)
        self.assertFalse("b" in db._data)
        self.assertFalse("c" in db._data)
        self.assertFalse("a" in db.root())
        self.assertFalse("b" in db.root())
        self.assertFalse("c" in db.root())

        self.assertEqual(1, mock_db.__setitem__.call_count)
        self.assertEqual(1, mock_db.__len__.call_count)
        self.assertEqual(1, mock_db.__getitem__.call_count)
        self.assertEqual(1, mock_db.get.call_count)
        self.assertEqual(1, mock_db.keys.call_count)
        self.assertEqual(1, mock_db.values.call_count)
        self.assertEqual(1, mock_db.__contains__.call_count)
        self.assertEqual(1, mock_db.__iter__.call_count)
        self.assertEqual(1, mock_db.items.call_count)
        self.assertEqual(1, mock_db.__delitem__.call_count)
        self.assertEqual(1, mock_db.pop.call_count)
        self.assertEqual(1, mock_db.remove.call_count)

        t.abort()

    def test_transparent_access_to_churro_instance(self):
        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = churro.Persistent()
        root = db.root()
        self.assertIs(db._churro.root(), root)
        self.assertTrue(root._dirty)
        db.flush()
        self.assertFalse(root._dirty)

        transaction.abort()

    def test_changing_branches(self):
        tx = transaction.begin()

        class Prop(churro.Persistent):
            c = churro.PersistentProperty()
        db = churrodb.ChurroDb(self.churrodb_path)
        db["folder"] = churro.PersistentFolder()
        db["folder"]["a"] = Prop()
        db["folder"]["b"] = Prop()

        tx.commit()
        tx = transaction.begin()
        db.switch("alternate")
        tx.commit()

        tx = transaction.begin()

        gitout = subprocess.check_output(["git", "show", "master:folder/a.churro"], cwd=self.churrodb_path)
        master_json = json.loads(gitout.decode("utf-8"))
        gitout = subprocess.check_output(["git", "show", "alternate:folder/a.churro"], cwd=self.churrodb_path)
        alternate_json = json.loads(gitout.decode("utf-8"))

        self.assertDictEqual(master_json, alternate_json)

        db.switch("HEAD")
        db["folder"]["a"].c = "d"
        tx.commit()

        gitout = subprocess.check_output(["git", "show", "master:folder/a.churro"], cwd=self.churrodb_path)
        master_json = json.loads(gitout.decode("utf-8"))
        gitout = subprocess.check_output(["git", "show", "alternate:folder/a.churro"], cwd=self.churrodb_path)
        alternate_json = json.loads(gitout.decode("utf-8"))

        self.assertTrue(master_json != alternate_json)

    def test_saving(self):
        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = churro.Persistent()
        db.save()
        result = read_json(os.path.join(self.churrodb_path, "a.churro"))

        self.assertDictEqual({
            "__churro_data__": {},
            "__churro_class__": "churro.Persistent"
        }, result)

    def test_concurrent_noconflict(self):
        # initial commit so that we do not generate a false conflict
        # strange, but see acidfs code for reference
        db = churrodb.ChurroDb(self.churrodb_path)
        db.save()
        env_a = unittest.mock.MagicMock()
        env_b = unittest.mock.MagicMock()

        env_a.exception = None

        def action_a(env):
            db = env.action_result = churrodb.ChurroDb(self.churrodb_path)
            db["a"] = Dummy()
            db["a"].value = "b"
            env.db = db

        def action_b(env):
            db = env.action_result = churrodb.ChurroDb(self.churrodb_path)
            db["a"] = Dummy()
            db["a"].value = "b"
            db["b"] = Dummy()
            db["b"].value = "c"

        def save(env):
            env.action_result.save()

        thread_b = threading.Thread(target=threader,
                                    kwargs={
                                        "env": env_b, "action": action_b, "save": save,
                                    })
        thread_a = threading.Thread(target=threader,
                                    kwargs={
                                        "env": env_a, "action": action_a, "save": save,
                                        "other": thread_b
                                    })

        thread_a.start()
        thread_a.join()

        json_a = read_json(os.path.join(self.churrodb_path, "a.churro"))
        json_b = read_json(os.path.join(self.churrodb_path, "b.churro"))

        self.assertIsNone(env_a.exception)
        self.assertEqual("b", json_a["__churro_data__"]["value"])
        self.assertEqual("c", json_b["__churro_data__"]["value"])

    @unittest.mock.patch("churrodb.unique_branch_name")
    def test_concurrent_conflict_not_mergeable_existing_object(self, unique_branch_name):
        unique_branch_name.return_value = "conflict"

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] =Dummy()
        db.save()

        env_a = unittest.mock.MagicMock()
        env_b = unittest.mock.MagicMock()

        env_a.exception = None

        def action_a(env):
            db = env.action_result = churrodb.ChurroDb(self.churrodb_path)
            db["a"] = Dummy()
            db["a"].value = "c"

        def action_b(env):
            db = env.action_result = churrodb.ChurroDb(self.churrodb_path)
            db["a"] = Dummy()
            db["a"].value = "b"

        def save(env):
            env.action_result.save()

        thread_b = threading.Thread(target=threader,
                                    kwargs={
                                        "env": env_b, "action": action_b, "save": save,
                                    })
        thread_a = threading.Thread(target=threader,
                                    kwargs={
                                        "env": env_a, "action": action_a, "save": save,
                                        "other": thread_b
                                    })

        # with Python < 3.4 we probably have to use testfixtures.log_capture decorator (see below)
        with self.assertLogs("churrodb", level="ERROR") as logged:
            thread_a.start()
            thread_a.join()

        branches = get_branches(self.churrodb_path)
        conflict_content = subprocess.check_output(["git", "show", "conflict:a.churro"], cwd=self.churrodb_path)
        conflict_json = json.loads(conflict_content.decode("utf-8"))
        master_json = read_json(os.path.join(self.churrodb_path, "a.churro"))

        self.assertIsInstance(env_a.exception, acidfs.ConflictError)
        self.assertListEqual(["conflict", "* master"], branches)
        self.assertEqual("b", master_json["__churro_data__"]["value"])
        self.assertEqual("c", conflict_json["__churro_data__"]["value"])
        self.assertListEqual(logged.output, [
            "ERROR:churrodb:trying to commit this transaction caused a conflict",
            "ERROR:churrodb:wrote problematic changeset to branch 'conflict', you have to resolve this yourself"])
        # log_capture decorator usage:
        # @testfixtures.log_capture("churrodb")
        # log_capture.check(
        # ("churrodb", "ERROR", "trying to commit this transaction caused a conflict"),
        # ("churrodb", "ERROR", "wrote problematic changeset to branch 'conflict', you have to resolve this yourself"))

    @unittest.mock.patch("churrodb.unique_branch_name")
    def test_concurrent_conflict_not_mergeable_nonexisting_object(self, mock):
        mock.return_value = "conflict"
        # this test is not final, see below for AcidFS git problem

        # initial commit so that we do not generate a false conflict
        # strange, but see acidfs code for reference
        db = churrodb.ChurroDb(self.churrodb_path)
        db.save()

        env_a = unittest.mock.MagicMock()
        env_b = unittest.mock.MagicMock()

        env_a.exception = None

        def action_a(env):
            db = env.action_result = churrodb.ChurroDb(self.churrodb_path)
            db["a"] = Dummy()
            db["a"].value = "c"

        def action_b(env):
            db = env.action_result = churrodb.ChurroDb(self.churrodb_path)
            db["a"] = Dummy()
            db["a"].value = "b"

        def save(env):
            env.action_result.save()

        thread_b = threading.Thread(target=threader,
                                    kwargs={
                                        "env": env_b, "action": action_b, "save": save,
                                    })
        thread_a = threading.Thread(target=threader,
                                    kwargs={
                                        "env": env_a, "action": action_a, "save": save,
                                        "other": thread_b
                                    })

        with self.assertLogs("churrodb", level="ERROR"):
            thread_a.start()
            thread_a.join()

        branches = get_branches(self.churrodb_path)
        conflict_content = subprocess.check_output(["git", "show", "conflict:a.churro"], cwd=self.churrodb_path)
        conflict_json = json.loads(conflict_content.decode("utf-8"))
        master_json = read_json(os.path.join(self.churrodb_path, "a.churro"))

        # AcidFS git segfault problem (see https://github.com/Pylons/acidfs/issues/3)
        # strangely enough, the bug doesn't always affect this test
        # so we log in an INFO message whether this was the case
        if isinstance(env_a.exception, subprocess.CalledProcessError):
            log.info("git segfault bug detected")

        self.assertIsInstance(env_a.exception, Exception)
        self.assertListEqual(["conflict", "* master"], branches)
        self.assertEqual("b", master_json["__churro_data__"]["value"])
        self.assertEqual("c", conflict_json["__churro_data__"]["value"])

    def test_index_update_on_commit(self):
        tx = transaction.begin()
        mock_index_factory = unittest.mock.MagicMock()
        mock_index_prop = unittest.mock.MagicMock()

        class Abc(churrodb.IndexMixin, churro.PersistentDict):
            index_factory = mock_index_factory

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = Abc()
        db["a"]._index = mock_index_prop
        db["a"].init_index()
        db["a"]["b"] = "c"
        tx.commit()

        self.assertEqual(1, mock_index_factory.call_count)
        self.assertEqual(1, mock_index_prop.idx_update.call_count)
        self.assertEqual(1, mock_index_prop.idx_validate.call_count)

    def test_index_update_on_abort(self):
        tx = transaction.begin()

        class TransMan(TestTransactionManager):
            def tpc_vote(self, tx):
                raise Exception

        mock_index_factory = unittest.mock.MagicMock()
        mock_index_prop = unittest.mock.MagicMock()

        class Abc(churrodb.IndexMixin, churro.PersistentDict):
            index_factory = mock_index_factory

        tx.join(TransMan())
        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = Abc()
        db["a"]._index = mock_index_prop
        db["a"].init_index()
        db["a"]["b"] = "c"

        try:
            tx.commit()
        except Exception:
            tx.abort()

        self.assertEqual(1, mock_index_factory.call_count)
        self.assertEqual(1, mock_index_prop.idx_update.call_count)

    def test_index_abort_transaction_on_fail(self):
        mock_index_factory = unittest.mock.MagicMock()
        mock_index_prop = unittest.mock.MagicMock()
        mock_index_prop.idx_update.side_effect = Exception

        class Abc(churrodb.IndexMixin, churro.PersistentDict):
            index_factory = mock_index_factory

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = Abc()
        db["a"]._index = mock_index_prop
        db["a"]["b"] = "c"

        self.assertRaises(Exception, lambda: transaction.commit())
        mock_index_prop.idx_update.side_effect = None
        transaction.commit()
        self.assertEqual(2, mock_index_prop.idx_update.call_count)

    def test_index_indexes_folder(self):
        tx = transaction.begin()

        mock_index_a = unittest.mock.MagicMock()
        mock_index_b = unittest.mock.MagicMock()

        class Abc(churrodb.IndexMixin, churro.PersistentDict):
            index_factory = churrodb.IndexesFolder

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = Abc()
        db["a"].init_index()
        db["a"]["_index"]["idx_a"] = mock_index_a
        db["a"]["_index"]["idx_b"] = mock_index_b
        db["a"]["b"] = "c"

        tx.commit()

        self.assertEqual(1, mock_index_a.idx_update.call_count)

        db["a"].idx_find("")
        db["a"].idx_find("", "idx_b")

        self.assertRaisesRegex(Exception, "there is no index called 'idx_c'", lambda: db["a"].idx_find("", "idx_c"))
        self.assertEqual(1, mock_index_a.idx_find.call_count)
        self.assertEqual(2, mock_index_b.idx_find.call_count)

    def test_index_full(self):
        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = IndexedCollection()
        db["a"].init_index()
        db["a"]["_index"]["idx_a"] = TestDictIndex()
        db["a"]["b"] = churro.Persistent()

        db.save()

        path = os.path.join(self.churrodb_path, "a", "_index", "idx_a.churro")

        self.assertTrue(os.path.exists(path))
        self.assertDictEqual({
            "__churro_class__": "churrodb.tests.TestDictIndex",
            "__churro_data__": {
                "data": {"_index": "indexed", "b": "indexed"}}}, read_json(path))

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"]["c"] = churro.Persistent()
        db.save()

        self.assertDictEqual({
            "__churro_class__": "churrodb.tests.TestDictIndex",
            "__churro_data__": {
                "data": {"_index": "indexed", "b": "indexed", "c": "indexed"}}}, read_json(path))

    def test_index_git_object_hash(self):
        tx = transaction.begin()
        db = churrodb.ChurroDb(self.churrodb_path)

        db["a"] = IndexedCollection()
        db["a"].init_index()
        db["a"]["_index"]["_a"] = churrodb.GitObjectHashIndex(True)
        db["a"]["_index"]["_b"] = churrodb.GitObjectHashIndex()
        db["a"]["b"] = Dummy("c")
        db["a"]["c"] = Dummy("d")
        db["a"]["d"] = Dummy("e")
        db["a"]["e"] = Dummy("f")

        db.save()

        db = churro.Churro(self.churrodb_path)
        db = db.root()
        coll = db.get("a")

        self.assertEqual("46d49b1a588f3684e0dc9f5ea6426a60512fd89d", coll.idx_find("b")[0])
        self.assertEqual("ab3a9aad770bc930fef6b1fd4eb03ad6d67fd407", coll.idx_find("c")[0])
        self.assertEqual("c6326067e195c781848cdca50797407bdbc6faeb", coll.idx_find("d")[0])
        self.assertEqual("3534c705a203d4ef38e2e4d1b6b6d2a63dd3866d", coll.idx_find("e")[0])
        self.assertEqual("c", coll[coll.idx_find("46d49b1a588f3684e0dc9f5ea6426a60512fd89d")[0]].value)
        self.assertEqual("d", coll[coll.idx_find("ab3a9aad770bc930fef6b1fd4eb03ad6d67fd407")[0]].value)
        self.assertEqual("e", coll[coll.idx_find("c6326067e195c781848cdca50797407bdbc6faeb")[0]].value)
        self.assertEqual("f", coll[coll.idx_find("3534c705a203d4ef38e2e4d1b6b6d2a63dd3866d")[0]].value)
        self.assertEqual(
            coll[coll.idx_find("46d49b1a588f3684e0dc9f5ea6426a60512fd89d")[0]].value,
            coll[coll["_index"]["_a"].idx_find_first("46d49b1a588f3684e0dc9f5ea6426a60512fd89d")].value)

        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"]["b"] = Dummy("x")

        db.save()

        db = churro.Churro(self.churrodb_path)
        db = db.root()
        coll = db.get("a")

        self.assertEqual("x", coll[coll.idx_find("a58a8f0b987cbb685ac125a060fb4ad0be7e76a0")[0]].value)

        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"]["f"] = Dummy("d")

        self.assertRaises(churrodb.IndexUpdateError, tx.commit)

        transaction.abort()

    def test_index_root_factory(self):
        tx = transaction.begin()

        class RootFactory(churrodb.IndexMixin, churro.PersistentFolder):
            pass

        db = churrodb.ChurroDb(self.churrodb_path, factory=RootFactory)
        db.init_index()
        db["_index"]["_a"] = churrodb.AbstractDictIndex({"a": "b"})
        db["a"] = Dummy()

        db.save()
        a_path = os.path.join(self.churrodb_path, "a.churro")
        index_path = os.path.join(self.churrodb_path, "_index")

        self.assertEqual(["b"], db.idx_find("a"))
        self.assertTrue(os.path.isdir(index_path))
        self.assertDictEqual({
            "__churro_data__": {"value": None},
            "__churro_class__": "churrodb.tests.Dummy"
        }, read_json(a_path))

    def test_git_object_proxy(self):
        a = {"b": "c", "d": {"e": {"f": "g"}}}
        x = churrodb.GitObjectProxy(a)

        keys = list(x.keys())

        self.assertTrue("b" in x)
        self.assertEqual(2, len(x))
        self.assertListEqual(sorted(["b", "d"]), sorted(keys))
        self.assertEqual("c", x["b"])

    def test_dot_lookup_dict_proxy(self):
        a = {"b": "c", "d": {"e": {"f": "g"}}}
        x = churrodb.DotLookupDictProxy(a)

        keys = list(x.keys())

        self.assertTrue("b" in x)
        self.assertEqual(2, len(x))
        self.assertListEqual(sorted(["b", "d"]), sorted(keys))
        self.assertEqual("c", x["b"])
        self.assertEqual("g", x["d.e.f"])

    def test_index_git_index_mixin(self):
        tx = transaction.begin()

        class MyCollectionA(churrodb.GitIndexMixin, churro.PersistentFolder):
            _index = churro.PersistentProperty()

            def git_index_key_mapper(k, v):
                return k

        class MyCollectionB(churrodb.GitIndexMixin, churro.PersistentFolder):
            @staticmethod
            def git_index_key_mapper(k, v):
                return v.get("key")

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = MyCollectionA()
        db["a"].init_index()

        db["b"] = MyCollectionB()
        db["b"]["_index"] = churro.Persistent()
        db["b"].init_index()

        db["a"]["x"] = Dummy("c")
        db["b"]["y"] = churro.PersistentDict({"d": "e", "key": "identifier"})
        db["b"]["z"] = churro.PersistentDict({"f": "g"})

        db.save()

        a_path = os.path.join(self.churrodb_path, "a", "__folder__.churro")
        b_index_path = os.path.join(self.churrodb_path, "b", "_index.churro")

        dict_a = read_json(a_path)
        dict_b = read_json(b_index_path)

        self.assertDictEqual({
            "x": "46d49b1a588f3684e0dc9f5ea6426a60512fd89d"},
            dict_a["__churro_data__"]["_index"]["__churro_data__"]["data"])
        self.assertDictEqual({
            "identifier": "ce7dbc999655ca73f204629774cda385a77599a4"},
            dict_b["__churro_data__"]["data"])

        self.assertEqual(
            "identifier",
            object_by_hash(
                self.churrodb_path,
                "ce7dbc999655ca73f204629774cda385a77599a4")["key"])

    def test_index_git_index_root_factory(self):
        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path, factory=TestRootFactory)
        db.init_index()

        db["_index"]["_git"] = churrodb.GitObjectHashIndex(name="root_git", inverse=True)
        db["coll"] = GitIndexedCollection(idx_name="coll", idx_supply="root_git")
        db["coll"].init_index()
        db["a"] = Dummy("c")
        db["coll"]["x"] = Dummy("e")

        db.save()

        tx = transaction.begin()
        db = churrodb.ChurroDb(self.churrodb_path, factory=TestRootFactory)

        self.assertEqual("c", db[db.idx_find("46d49b1a588f3684e0dc9f5ea6426a60512fd89d")[0]].value)
        self.assertEqual("e", db["coll"][db.idx_find("c6326067e195c781848cdca50797407bdbc6faeb")[0]].value)

        db["c"] = Dummy("e")
        db["coll"]["y"] = Dummy("f")
        db.save()

        db = churrodb.ChurroDb(self.churrodb_path, factory=TestRootFactory)

        self.assertEqual("c", db[db.idx_find("46d49b1a588f3684e0dc9f5ea6426a60512fd89d")[0]].value)
        self.assertEqual("e", db[db.idx_find("c6326067e195c781848cdca50797407bdbc6faeb")[0]].value)
        self.assertEqual("f", db["coll"][db.idx_find("3534c705a203d4ef38e2e4d1b6b6d2a63dd3866d")[0]].value)

    def test_index_git_dict_key_hash_index(self):
        tx = transaction.begin()
        db = churrodb.ChurroDb(self.churrodb_path, factory=TestRootFactory)
        db.init_index()
        db["_index"]["_git"] = churrodb.GitObjectHashIndex(name="git_root")

        db["a"] = IndexedCollection()
        db["a"].init_index()
        db["a"]["_index"]["_a"] = churrodb.GitDictKeyHashIndex(
            name="git_a", supply="git_root", dict_key="k")

        db["a"]["b"] = churro.PersistentDict()
        db["a"]["b"]["c"] = "d"
        db["a"]["b"]["k"] = "1"

        db.save()

        self.assertDictEqual(
            {"c": "d", "k": "1"},
            dict(object_by_hash(self.churrodb_path, db.idx_find("1")[0])))

        db = churrodb.ChurroDb(self.churrodb_path, factory=TestRootFactory)

        self.assertDictEqual(
            {"c": "d", "k": "1"},
            dict(object_by_hash(self.churrodb_path, db.idx_find("1")[0])))

    def test_index_git_index_key_persistence(self):
        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path)
        db["a"] = IndexedCollection()
        db["a"].init_index()
        db["a"]["_index"]["_git"] = churrodb.GitDictKeyHashIndex()
        db["a"]["b"] = churro.PersistentDict({"id": "1", "c": "d"})
        db["a"]["x"] = churro.PersistentDict({"id": "3", "h": "i"})

        db.save()

        self.assertEqual("ae1bc6576ccca4b0c416af2004971674e90fd501", db["a"].idx_find_first("1"))
        self.assertEqual("cc885e9312bc6b3a4006bf455f037ecbf0af6162", db["a"].idx_find_first("3"))

        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path)

        self.assertEqual("ae1bc6576ccca4b0c416af2004971674e90fd501", db["a"].idx_find_first("1"))
        self.assertEqual("cc885e9312bc6b3a4006bf455f037ecbf0af6162", db["a"].idx_find_first("3"))

        del db["a"]["b"]
        db["a"]["x"]["h"] = "j"
        db["a"]["e"] = churro.PersistentDict({"id": "2", "f": "g"})

        db.save()

        tx = transaction.begin()

        self.assertEqual("ae1bc6576ccca4b0c416af2004971674e90fd501", db["a"].idx_find_first("1"))
        self.assertEqual("de056b94c551a1b3413c9a740c6d07f163aee8ea", db["a"].idx_find_first("2"))
        self.assertEqual("2c45a823da31fc40936714d3e3e10e838acc9ad2", db["a"].idx_find_first("3"))

        tx.abort()

        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path)

        self.assertEqual("ae1bc6576ccca4b0c416af2004971674e90fd501", db["a"].idx_find_first("1"))
        self.assertEqual("de056b94c551a1b3413c9a740c6d07f163aee8ea", db["a"].idx_find_first("2"))
        self.assertEqual("2c45a823da31fc40936714d3e3e10e838acc9ad2", db["a"].idx_find_first("3"))

        tx.abort()

    def test_index_git_index_persistence(self):
        tx = transaction.begin()

        db = churrodb.ChurroDb(self.churrodb_path, factory=TestRootFactory)
        db.init_index()
        db["_index"]["_git"] = churrodb.GitObjectHashIndex(name="root_git")
        db["a"] = GitIndexedCollection(idx_name="abc", idx_supply="root_git")
        db["a"].init_index()

        db["a"]["x"] = churro.PersistentDict({"a": "b"})

        db.save()

        self.assertEqual("d6e9f0f90bf4fe6acdc173607d70d8a8d7a0f612", db.idx_find_first("x"))

        db = churrodb.ChurroDb(self.churrodb_path)

        self.assertEqual("d6e9f0f90bf4fe6acdc173607d70d8a8d7a0f612", db.idx_find_first("x"))


if __name__ == "__main__":
    unittest.main(module="tests")