from __future__ import unicode_literals

from six import BytesIO
import joblib
import redis


class BatchStorage(object):
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 7007

    BATCH_QUEUE_DEFAULT_KEY = 'batch-queue'
    GOT_BATCH_CHANNEL = 'got-batch'

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, name=None):
        self._redis = redis.Redis(host=host, port=port)
        self._batch_queue_key = "{}.{}".format(self.BATCH_QUEUE_DEFAULT_KEY, name or 'default')

    @classmethod
    def serialize(cls, data):
        s = BytesIO()
        joblib.dump(data, s)
        return s.getvalue()

    @classmethod
    def deserialize(cls, bytes_string):
        return joblib.load(BytesIO(bytes_string))

    def batch_queue_size(self):
        return self._redis.llen(self._batch_queue_key)
