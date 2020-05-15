import biothings.hub.dataload.uploader
import os

import biothings
import config
import requests
biothings.config_for_app(config)

MAP_URL = "https://raw.githubusercontent.com/SuLab/outbreak.info-resources/master/outbreak_resources_es_mapping.json"
MAP_VARS = ["@type", "author", "citedBy", "curatedBy", "dateCreated", "dateModified", "datePublished", "description", "doi", "funding", "identifier", "isBasedOn", "keywords", "measurementParameter", "measurementTechnique", "name", "relatedTo", "url"]

# when code is exported, import becomes relative
try:
    from covid_pdb_datasets.parser import load_annotations as parser_func
except ImportError:
    from .parser import load_annotations as parser_func


class PDBUploader(biothings.hub.dataload.uploader.BaseSourceUploader):

    # main_source = "covid_pdb_datasets"
    name = "covid_pdb_datasets"
    __metadata__ = {
        "src_meta": {
            'license_url': 'https://www.rcsb.org/pages/usage-policy',
            'licence': 'CC0 1.0 Universal',
            'url': 'https://www.rcsb.org/news?year=2020&article=5e74d55d2d410731e9944f52&feature=true'
        }
    }
    idconverter = None
    storage_class = biothings.hub.dataload.storage.BasicStorage

    def load_data(self, data_folder):
        if data_folder:
            self.logger.info("Load data from directory: '%s'", data_folder)
        return parser_func(data_folder)

    @classmethod
    def get_mapping(klass):
        r = requests.get(MAP_URL)
        if(r.status_code == 200):
            mapping = r.json()
            mapping_dict = { key: mapping[key] for key in MAP_VARS }
            return mapping_dict
