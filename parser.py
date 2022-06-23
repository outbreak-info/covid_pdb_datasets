import os

import pandas as pd
import requests
from datetime import date

from biothings.utils.common import open_anyfile
from outbreak_parser_tools.logger import get_logger
logger = get_logger('pdb')

PDB_API = "https://data.rcsb.org/rest/v1/core/entry"

def paginate_through_PDB_ids(index=0):
    url = 'https://www.rcsb.org/search/data'
    payload =  {'report': 'search_summary', 'request': {'query': {'type': 'group', 'nodes': [{'type': 'group', 'nodes': [{'type': 'group', 'nodes': [{'type': 'terminal', 'service': 'text', 'parameters': {'attribute': 'rcsb_entity_source_organism.taxonomy_lineage.name', 'operator': 'exact_match', 'value': 'SARS-CoV-2'}}], 'logical_operator': 'and'}], 'logical_operator': 'and', 'label': 'text'}], 'logical_operator': 'and'}, 'return_type': 'entry', 'request_options': {'paginate': {'start': index, 'rows': 100}, 'scoring_strategy': 'combined', 'sort': [{'sort_by': 'score', 'direction': 'desc'}]}, 'request_info': {'query_id': '3f095c89d4be9220d8eaaca0c98ef2ed'}}, 'getDrilldown': True, 'attributes': None}
    response = requests.post(url, json=payload).json()
    if response.get('statusCode') == 999:
        return None, None

    ids = [i['identifier'] for i in response['result_set']]
    return set(ids), response['result_set_count']

def get_PDB_ids():
    ids, length = paginate_through_PDB_ids()
    index = 100
    while index < length:
        new_ids, _ = paginate_through_PDB_ids(index)
        if new_ids is None:
            logger.warning(f'breaking index {index} length {length},  no new ids found')
            break
        ids = ids.union(new_ids)
        index += 100
    return ids

def getPDB():
    ids = get_PDB_ids()
    df = []
    total = len(ids)
    for i,id in enumerate(ids):
        if(i%10 == 0):
            logger.info(f"finished {i} of {total}")
        df.append(getPDBmetadata(id))
    logger.warning("Finished getting PDB metadata")
    return(df)


def getPDBmetadata(id):
    resp = requests.get(f"{PDB_API}/{id}")
    if resp.status_code == 200:
        raw_data = resp.json()

        today = date.today().strftime("%Y-%m-%d")
        md = {"@type": "Dataset"}
        md["name"] = raw_data["struct"]["title"]
        md["description"] = raw_data["struct"]["title"]
        md["_id"] = f"pdb{raw_data['rcsb_id']}"
        md["identifier"] = raw_data["rcsb_id"]
        md["doi"] = f"10.2210/{md['_id']}/pdb"
        md["author"] = [{"@type": "Person", "name": author["name"]} for author in raw_data["audit_author"]]
        md["citedBy"] = [getCitation(citation) for citation in raw_data["citation"]]
        if("pdbresolution" in raw_data.get("pdbx_vrpt_summary", {}).keys()):
            md["measurementParameter"] = {"resolution": raw_data["pdbx_vrpt_summary"]["pdbresolution"]}
        md["measurementTechnique"] = [technique["method"].lower() for technique in raw_data["exptl"]]
        if("pdbx_audit_support" in raw_data.keys()):
            md["funding"] = [getFunding(funder) for funder in raw_data["pdbx_audit_support"]]
        md["datePublished"] = raw_data["rcsb_accession_info"]["deposit_date"][0:10]
        md["dateModified"] = raw_data["rcsb_accession_info"]["revision_date"][0:10]
        md["keywords"] = getKeywords(raw_data)
        md["url"] = f"https://www.rcsb.org/structure/{md['identifier']}"
        md["curatedBy"] = {"@type": "Organization", "url": md["url"], "name": "The Protein Data Bank", "curationDate": today}
        if("rcsb_external_references" in raw_data.keys()):
            md["sameAs"] = [link["link"] for link in raw_data["rcsb_external_references"]]
        return(md)
    else:
        # logger.info(f"ID {id} returned an error from the API")
        logger.warning(f"ID {id} returned an error from the API")

def getCitation(citation):
    cite = {"@type": "Publication"}
    cite["journalNameAbbrev"] = citation["journal_abbrev"]
    cite["name"] = citation["title"]
    cite["author"] = [{"@type": "Person", "name": author} for author in citation["rcsb_authors"]]
    if(("page_first" in citation.keys()) & ("page_last" in citation.keys())):
        cite["pagination"] = f"{citation['page_first']} - {citation['page_last']}"
    if("journal_volume" in citation.keys()):
        cite["volumeNumber"] = citation["journal_volume"]
    if("year" in citation.keys()):
        cite["datePublished"] = citation["year"]
    if("pdbx_database_id_doi" in citation.keys()):
        cite["doi"] = citation["pdbx_database_id_doi"]
    if("pdbx_database_id_pub_med" in citation.keys()):
        cite["pmid"] = citation["pdbx_database_id_pub_med"]
    return(cite)

def getFunding(funding):
    funder = {"@type": "Organization", "name": funding["funding_organization"]}
    obj = {"@type": "MonetaryGrant", "funder": funder}
    if("grant_number" in funding.keys()):
        obj["identifier"] = funding["grant_number"]
    return(obj)

def getKeywords(result):
    keys = []
    if("pdbx_keywords" in result["struct_keywords"].keys()):
        keys.extend(result["struct_keywords"]["pdbx_keywords"].split(","))
    if("text" in result["struct_keywords"].keys()):
        keys.extend(result["struct_keywords"]["text"].split(","))

    keys = [key.strip() for key in keys]
    return(list(pd.np.unique(keys)))

def load_annotations(data_folder):
    docs = getPDB()
    for doc in docs:
        yield doc

if __name__ == '__main__':
    import json
    j = [i for i in load_annotations('./')]
    with open('d.json', 'w') as d:
        for m in j:
            json.dump(m, d)
            d.write('\n')
