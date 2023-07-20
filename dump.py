import os
import datetime

import biothings, config
biothings.config_for_app(config)
from config import DATA_ARCHIVE_ROOT

import biothings.hub.dataload.dumper

class PdbDumper(biothings.hub.dataload.dumper.DummyDumper):
    SRC_NAME = "covid_pdb_datasets"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)

    SCHEDULE = "30 11 * * *"

    __metadata__ = {
        "src_meta": {
            'license_url': 'https://www.rcsb.org/pages/usage-policy',
            'licence': 'CC0 1.0 Universal',
            'url': 'https://www.rcsb.org/news?year=2020&article=5e74d55d2d410731e9944f52&feature=true'
        }
    }


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_release()

    def set_release(self):
        self.release = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M')
