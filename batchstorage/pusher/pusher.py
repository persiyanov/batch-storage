from __future__ import unicode_literals


import logging
from batchstorage import base


logger = logging.getLogger(__name__)


class BatchPusher(base.BatchStorage):
    def __init__(self, buffer_size=100, **kwargs):
        super(BatchPusher, self).__init__(**kwargs)

        self._buffer_size = buffer_size
        self._pubsub = self._redis.pubsub()
        self._pubsub.subscribe(self.GOT_BATCH_CHANNEL)

        self._clear_batch_queue()

    def next_batch(self):
        """Must return picklable object. E.g., list of numpy arrays, numpy array, strings...
        """
        raise NotImplementedError("You must implement this method using your processing pipeline.")

    def _clear_batch_queue(self):
        logger.info("Clearing batch queue '%s'" % self._batch_queue_key)
        self._redis.delete(self._batch_queue_key)

    def _push(self, batch):
        logger.info("Pushing batch.")
        self._redis.lpush(self._batch_queue_key, self.serialize(batch))

    def start(self):
        while True:
            try:
                batch = self.next_batch()
                while self.batch_queue_size() >= self._buffer_size:
                    self._pubsub.listen()
                self._push(batch)
            except StopIteration:
                return
