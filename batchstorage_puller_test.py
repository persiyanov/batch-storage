from __future__ import unicode_literals

from batchstorage.puller import BatchPuller
import datetime
import time
import numpy as np


if __name__ == '__main__':
    puller = BatchPuller()
    print "Try pulling 100 batches"

    for i in xrange(100):
        batch = puller.next_batch()
        print "Got batch #{:>2} on {}. Current queue size = {}".format(i, datetime.datetime.now(), puller.batch_queue_size())
        time.sleep(0.5)  # Imitating one step of train_op.
        assert np.array_equal(batch, np.ones((64,224,224,3))*(i+1))
