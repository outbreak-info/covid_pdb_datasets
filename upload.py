import biothings.hub.dataload.uploader
import os

import biothings
import config
biothings.config_for_app(config)


# when code is exported, import becomes relative
try:
    from covid_pdb_datasets.parser import load_annotations as parser_func
except ImportError:
    from .parser import load_annotations as parser_func


class PDBUploader(biothings.hub.dataload.uploader.BaseSourceUploader):

    # main_source = "covid_pdb_datasets"
    name = "pdb"
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
        return {
            "@type": {
                "normalizer": "keyword_lowercase_normalizer",
                "type": "keyword"
            },
            "author": {
                "properties": {
                    "@type": {
                        "normalizer": "keyword_lowercase_normalizer",
                        "type": "keyword"
                    },
                    "affiliation": {
                        "properties": {
                            "name": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword"
                            }
                        }
                    },
                    "name": {
                        "type": "text"
                    },
                    "title": {
                        "type": "text"
                    },
                    "role": {
                        "normalizer": "keyword_lowercase_normalizer",
                        "type": "keyword"
                    }
                }},
            "curatedBy": {
                "properties": {
                    "@type": {
                        "normalizer": "keyword_lowercase_normalizer",
                        "type": "keyword"
                    },
                    "name": {
                        "type": "text"
                    },
                    "url": {
                        "type": "text"
                    },
                    "versionDate": {
                        "normalizer": "keyword_lowercase_normalizer",
                        "type": "keyword"
                    },
                }
            },
            "dateCreated": {
                "type": "keyword"
            },
            "dateModified": {
                "type": "keyword"
            },
            "datePublished": {
                "type": "keyword"
            },
            "description": {
                "type": "text"
            },
            "doi": {
                "type": "text"
            },
            "identifier": {
                "normalizer": "keyword_lowercase_normalizer",
                "type": "keyword"
            },
            "isBasedOn": {
                "properties": {
                    "@type": {
                        "normalizer": "keyword_lowercase_normalizer",
                        "type": "keyword"
                    },
                    "identifier": {
                        "type": "text"
                    },
                    "name": {
                        "type": "text"
                    },
                    "description": {
                        "type": "text"
                    },
                    "url": {
                        "type": "text"
                    },
                    "datePublished": {
                        "type": "text"
                    }
                }
            },
            "citedBy": {
                "properties": {
                    "@type": {
                        "normalizer": "keyword_lowercase_normalizer",
                        "type": "keyword"
                    },
                    "identifier": {
                        "type": "text"
                    },
                    "name": {
                        "type": "text"
                    },
                    "description": {
                        "type": "text"
                    },
                    "url": {
                        "type": "text"
                    },
                    "datePublished": {
                        "type": "text"
                    }
                }
            },
            "keywords": {
                "normalizer": "keyword_lowercase_normalizer",
                "type": "keyword",
                "copy_to": ["all"]
            },
            "measurementParameter": {
                "properties": {
                    "resolution": {
                        "type": "text"
                    }
                }
            },
            "measurementTechnique": {
                "normalizer": "keyword_lowercase_normalizer",
                "type": "keyword"
            },
            "name": {
                "type": "text"
            },
            "relatedTo": {
                "properties": {
                    "@type": {
                        "normalizer": "keyword_lowercase_normalizer",
                        "type": "keyword"
                    },
                    "identifier": {
                        "type": "text"
                    },
                    "pmid": {
                        "type": "text"
                    },
                    "url": {
                        "type": "text"
                    },
                    "citation": {
                        "type": "text"
                    }
                }
            },
            "url": {
                "type": "text"
            }
        }
