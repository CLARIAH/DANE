import unittest
import DANE
import sys
sys.path.append('../examples/')
from dummyhandler import DummyHandler
from yacs.config import CfgNode
import os
from tempfile import TemporaryDirectory
from importlib import reload
import DANE.config

class TestJob(unittest.TestCase):
    # TODO This implicitly also tests tasks, perhaps should make this explicit

    def setUp(self):
        self.job = DANE.Job(source_url='http://127.0.0.1/example', 
                source_id='TEST123',
                tasks=DANE.taskSequential(['TEST']))

        self.dummy = DummyHandler()

    def test_serialize(self):
        serialised = self.job.to_json()
        self.assertIsInstance(serialised, str)

        new_job = DANE.Job.from_json(serialised)
        self.assertIsInstance(new_job, DANE.Job)

        self.assertEqual(self.job.source_id, new_job.source_id)
        self.assertEqual(self.job.source_url, new_job.source_url)
        self.assertEqual(self.job.tasks.to_json(), new_job.tasks.to_json())

    def test_register_and_run(self):
        self.job.set_api(self.dummy)
        self.assertIsInstance(self.job.api, DummyHandler)
        
        self.job.apply(lambda x: (self.assertIsInstance(x.api, DummyHandler)))
        # API is good to go!

        self.job.register()

        self.assertIsNotNone(self.job.job_id)
        self.job.apply(lambda x: (self.assertEqual(x.job_id, self.job.job_id)))
        self.job.apply(lambda x: (self.assertIsNotNone(x.task_id)))
        # job is registered, tasks are registered

        self.job.run()
        self.assertTrue(self.job.isDone())

class TestConfig(unittest.TestCase):
    
    def tearDown(self):
        # Remove them here, as to not cross-contaminate other tests
        if os.path.exists('base_config.yml'):
            os.remove('base_config.yml')
        if os.path.exists('config.yml'):
            os.remove('config.yml')
        
        if 'DANE_HOME' in os.environ.keys():
            del os.environ['DANE_HOME']

    def test_import_config(self):
        reload(DANE.config)
        cfg = DANE.config.cfg
        
        self.assertIsInstance(cfg, CfgNode)
        self.assertIsInstance(cfg.DANE, CfgNode)

    def test_base_config(self):
        with open('base_config.yml', 'w') as f:
            f.write('TEST:\n')
            f.write('  SCOPE: "BASE"\n')

        reload(DANE.config)
        cfg = DANE.config.cfg

        self.assertIsInstance(cfg.TEST, CfgNode)
        self.assertEqual(cfg.TEST.SCOPE, "BASE")

    def test_local_config(self):
        with open('config.yml', 'w') as f:
            f.write('TEST:\n')
            f.write('  SCOPE: "LOCAL"\n')

        reload(DANE.config)
        cfg = DANE.config.cfg


        self.assertIsInstance(cfg.TEST, CfgNode)
        self.assertEqual(cfg.TEST.SCOPE, "LOCAL")

    def test_global_config(self):
        with TemporaryDirectory() as tmpdirname:
            os.environ['DANE_HOME'] = tmpdirname
            with open(os.path.join(tmpdirname, 'config.yml'), 'w') as f:
                f.write('TEST:\n')
                f.write('  SCOPE: "GLOBAL"\n')

            reload(DANE.config)
            cfg = DANE.config.cfg

            self.assertIsInstance(cfg.TEST, CfgNode)
            self.assertEqual(cfg.TEST.SCOPE, "GLOBAL")

if __name__ == '__main__':
    unittest.main(buffer=True)
