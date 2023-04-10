import configparser
import json

from fastapi import HTTPException
from .RepositoryBase import RepoBase

config = configparser.ConfigParser()
config.read('metarepo.conf')

class LocalRepository(RepoBase):
    
    def _read_repo(self):
        config_field = "LOCAL"
        filename = config.get(config_field, 'local_file', fallback="meta.repo")
        try:
            with open(filename, 'r') as fin:
                repo = json.load(fin)
        except: # Either the file is bad or it's not json
            raise HTTPException(status_code=500,
                                detail=f"Could not read repo file {filename}")
        return repo
    
    def _write_repo(self, repo):
        config_field = "LOCAL"
        filename = config.get(config_field, 'local_file', fallback="meta.repo")
        try:
            with open(filename, 'w') as fout:
                json.dump(repo, fout)
        except: # Either the file is bad or it's not json
            raise HTTPException(status_code=500,
                                detail=f"Could not write to repo file {filename}")

    
    def find(self, filters: dict=None, groups: list=None, page: int=0):
        # Read json directly from a file
        repo = self._read_repo()
                    
        # Run through each metasheet found. If it matches the filters, we're good
        results = []
        for doc_id in repo:
            metasheet = repo[doc_id]
            valid = True
            for _filter in filters:
                if _filter not in metasheet or not filters[_filter] == metasheet[_filter]:
                    valid = False
                    continue
            if groups and metasheet['siteMetadata']['tenant'] not in groups:
                valid = False
            if valid:
                results.append(metasheet)
                
        return results
        
    
    def notate(self, doc: dict) -> None:
        repo = self._read_repo()
            
        doc_id = doc['docId']
        if doc_id in repo:
            raise HTTPException(status_code=500,
                                detail=f"Document {doc_id} already exists in repo!")
       
        repo[doc_id] = doc
        
        self._write_repo(repo)

    def update(self, doc_id: str, update_fields: dict) -> None:
        repo = self._read_repo()

        if doc_id not in repo:
            raise HTTPException(status_code=500,
                                detail=f"Document {doc_id} does not exist in repo!")
            
        metasheet = repo[doc_id]
        for field in update_fields:
            metasheet[field] = update_fields[field]
        repo[doc_id] = metasheet

        self._write_repo(repo)
    