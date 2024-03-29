import dane.config
from dane import Document, Task
import unittest
from yacs.config import CfgNode
import os
from tempfile import TemporaryDirectory
from importlib import reload
from .test_util import DummyHandler


# Run this test file from the root dir: python -m test.test_dane
# TODO unit tests should be completely rewritten
class TestDocument(unittest.TestCase):
    def setUp(self):
        self.doc = Document(
            {"id": "TEST123", "url": "http://127.0.0.1/example", "type": "Text"},
            {"id": "TEST", "type": "Software"},
        )

        self.dummy = DummyHandler()

    def test_serialize(self):
        serialised = self.doc.to_json()
        self.assertIsInstance(serialised, str)

        new_doc = Document.from_json(serialised)
        self.assertIsInstance(new_doc, Document)

        self.assertEqual(self.doc.target["id"], new_doc.target["id"])
        self.assertEqual(self.doc.creator["id"], new_doc.creator["id"])

    def test_register(self):
        self.doc.set_api(self.dummy)
        self.assertIsInstance(self.doc.api, DummyHandler)

        self.doc.register()

        self.assertIsNotNone(self.doc._id)


class TestTask(unittest.TestCase):
    def setUp(self):
        self.task = Task("TEST")
        self.dummy = DummyHandler()

        # depend on doc being able to register for testing tasks
        self.doc = Document(
            {"id": "TEST123", "url": "http://127.0.0.1/example", "type": "Text"},
            {"id": "TEST", "type": "Software"},
        )
        self.doc.set_api(self.dummy)
        self.doc.register()

    def test_serialize(self):

        serialised = self.task.to_json()
        self.assertIsInstance(serialised, str)

        new_task = Task.from_json(serialised)
        self.assertIsInstance(new_task, Task)

        self.assertEqual(self.task.key, new_task.key)
        self.assertEqual(self.task.priority, new_task.priority)

    def test_assign(self):
        self.task.set_api(self.dummy)

        self.task.assign(self.doc._id)


class TestConfig(unittest.TestCase):
    def tearDown(self):
        # Remove them here, as to not cross-contaminate other tests
        if os.path.exists("base_config.yml"):
            os.remove("base_config.yml")
        if os.path.exists("config.yml"):
            os.remove("config.yml")

        if "DANE_HOME" in os.environ.keys():
            del os.environ["DANE_HOME"]

    def test_import_config(self):
        reload(dane.config)
        cfg = dane.config.cfg

        self.assertIsInstance(cfg, CfgNode)
        self.assertIsInstance(cfg.DANE, CfgNode)

    def test_local_config(self):
        with open("config.yml", "w") as f:
            f.write("TEST:\n")
            f.write('  SCOPE: "LOCAL"\n')

        reload(dane.config)
        cfg = dane.config.cfg

        self.assertIsInstance(cfg.TEST, CfgNode)
        self.assertEqual(cfg.TEST.SCOPE, "LOCAL")

    def test_global_config(self):
        with TemporaryDirectory() as tmpdirname:
            os.environ["DANE_HOME"] = tmpdirname
            with open(os.path.join(tmpdirname, "config.yml"), "w") as f:
                f.write("TEST:\n")
                f.write('  SCOPE: "GLOBAL"\n')

            reload(dane.config)
            cfg = dane.config.cfg

            self.assertIsInstance(cfg.TEST, CfgNode)
            self.assertEqual(cfg.TEST.SCOPE, "GLOBAL")


if __name__ == "__main__":
    unittest.main(buffer=True)
