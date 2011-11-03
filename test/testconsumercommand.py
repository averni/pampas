

import pampas
import unittest
import time
import logging

logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] %(module)15s:%(name)10s:%(lineno)4d [%(levelname)6s]:  %(message)s")

def testf(evt):
    print "Ricevuto"

def testcb(consumer, event):
    print "TESTCB received"
    return "OK"

class TestPipelineProcessor(unittest.TestCase):
    def setUp(self):
        #self.amqparams = {'host_and_ports':[('localhost', 61116)]}
        self.amqparams = {'host_and_ports':[('liq-1-nlpbke', 61116)]}
        self.destination = '/queue/test'
        self.factory = pampas.AMQClientFactory(self.amqparams)
        self.factory.setMessageQueue(self.destination)
        #self.proc = pampas.PipelineProcessor('testpipe', './etc')
        monitor = self.factory.createConsumerClient()
        monitor.connect()
        self.monitor = monitor
        self.procs = []

    def testASpawnProcess(self):
        print "Spawning worker"
        self.procs = self.factory.spawnConsumers(testf, 1, cmdparams={'test': testcb})
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            self.factory.disconnectAll()
            raise SystemExit()
        print "Process spawned. Testing..."
        self.assertEqual(len(self.monitor.ping()), 1)

    def testConsumerCommand(self):
        self.monitor.command("test", 'prova')
        print "Messages sent, waiting for consumers"
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            self.factory.disconnectAll()
            raise SystemExit()

    def testZStopConsumers(self):
        print "Stopping consumer"
        self.monitor.stopConsumers()
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            self.factory.disconnectAll()
            raise SystemExit()

        self.assertEqual(len(self.monitor.ping()),  0)
        
    def tearDown(self):
        self.factory.disconnectAll()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPipelineProcessor)
    unittest.TextTestRunner(verbosity=2).run(suite)
