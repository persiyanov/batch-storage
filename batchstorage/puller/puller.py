from __future__ import unicode_literals

from batchstorage import base


class BatchPuller(base.BatchStorage):
    TOTAL_BATCHES_KEY = 'total-batches'

    def __init__(self, **kwargs):
        super(BatchPuller, self).__init__(**kwargs)

    def total_batches(self):
        return self._redis.get(self.TOTAL_BATCHES_KEY)

    def next_batch(self):
        batch = self.deserialize(self._redis.brpop(self._batch_queue_key)[1])
        self._publish_got_batch()
        return batch

    def _publish_got_batch(self):
        self._redis.publish(self.GOT_BATCH_CHANNEL, None)
        return
