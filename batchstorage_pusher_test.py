from __future__ import unicode_literals

import numpy as np

from batchstorage.pusher import BatchPusher
import logging.config


logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'standard',
            }
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': True,
            },
        },
    })


class CustomPusher(BatchPusher):
    def __init__(self, **kwargs):
        super(CustomPusher, self).__init__(**kwargs)
        self.cntr = 0

    def next_batch(self):
        self.cntr += 1
        return np.ones((64,224,224,3))*self.cntr


if __name__ == '__main__':
    pusher = CustomPusher(buffer_size=100)
    pusher.start()
