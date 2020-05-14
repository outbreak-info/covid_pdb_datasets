import os

import pandas as pd
import requests
from datetime import date

from biothings.utils.common import open_anyfile
from biothings import config
logging = config.logger

# List of coronavirus-related PDB ids, curated by RCSB
# PDB_IDS = "https://cdn.rcsb.org/rcsb-pdb/general_information/news_publications/SARS-Cov-2-LOI/SARS-Cov-2-all-LOI.tsv"
PDB_API = "https://data.rcsb.org/rest/v1/core/entry"

def getPDB(raw_ids, pdb_api):
    # raw_ids = pd.read_csv(pdb_ids, sep="\t")
    ids = pd.np.unique(raw_ids["PDB structures complexed with Ligands of Interest (LOI)"])
    df = []
    total = len(ids)
    for i,id in enumerate(ids):
        if(i%10 == 0):
            print(f"finished {i} of {total}")
        df.append(getPDBmetadata(pdb_api, id))
    logging.warning("Finished getting PDB metadata")
    return(df)


def getPDBmetadata(pdb_api, id):
    resp = requests.get(f"{pdb_api}/{id}")
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
    if("pdbresolution" in raw_data["pdbx_vrpt_summary"].keys()):
        md["measurementParameter"] = {"resolution": raw_data["pdbx_vrpt_summary"]["pdbresolution"]}
    md["measurementTechnique"] = [technique["method"].lower() for technique in raw_data["exptl"]]
    if("pdbx_audit_support" in raw_data.keys()):
        md["funding"] = [getFunding(funder) for funder in raw_data["pdbx_audit_support"]]
    md["datePublished"] = raw_data["rcsb_accession_info"]["deposit_date"][0:10]
    md["dateModified"] = raw_data["rcsb_accession_info"]["revision_date"][0:10]
    md["keywords"] = getKeywords(raw_data)
    md["url"] = f"https://www.rcsb.org/structure/{md['identifier']}"
    md["curatedBy"] = {"@type": "Organization", "url": md["url"], "name": "The Protein Data Bank", "updatedDate": today}
    if("rcsb_external_references" in raw_data.keys()):
        md["sameAs"] = [link["link"] for link in raw_data["rcsb_external_references"]]
    return(md)

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
    keys.extend(result["struct_keywords"]["pdbx_keywords"].split(","))
    keys.extend(result["struct_keywords"]["text"].split(","))

    keys = [key.strip() for key in keys]
    return(list(pd.np.unique(keys)))

# raw_ids = pd.read_csv(PDB_IDS, sep="\t")
# getPDB(raw_ids, PDB_API)

def load_annotations(data_folder):
    infile = os.path.join(data_folder,"SARS-Cov-2-all-LOI.tsv")
    assert os.path.exists(infile)

    with open_anyfile(infile,mode='r') as file:
        data = file.read()
        docs = getPDB(data, PDB_API)
        for doc in docs:
            yield doc
