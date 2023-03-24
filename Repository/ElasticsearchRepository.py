import configparser

from elasticsearch import Elasticsearch
from fastapi import HTTPException

from .RepositoryBase import RepoBase

config = configparser.ConfigParser()
config.read('repo.conf')

class ElasticsearchRepository(RepoBase):

    def _connect_elasticsearch(self):
        """Perform the connection to elasticsearch, using details from the config"""
        config_field = "ELASTICSEARCH"
        els = Elasticsearch(
             config.get(config_field, "elastic_url"),
             ssl_assert_fingerprint=config.get(config_field, "cert_fingerprint"),
             basic_auth=(
                    config.get(config_field, "elastic_user"),
                    config.get(config_field, "elastic_password")))
        return els
    
    def find(self, filters: dict=None, groups: list=None, page: int=0):
        if filters is None: filters = {}
        if groups is None: groups = []
        
        if not filters and not groups:
            query = {"match_all": {}}
        elif not filters and groups:
            raise HTTPException(
                status_code=401,
                detail="If we're doing a find all, do NOT include groups")
        else: # We have filters. We might not have groups, depending on tenancy and exactly what is being checked
            query = {"bool": {"must": []}}
            for tag in filters:
                val = filters[tag]
                query_field = {"match": {tag: val}}
                query["bool"]["must"].append(query_field)
            if groups:
                group_query = {"bool": {"should": []}}
                for group in groups:
                    group_query["bool"]["should"].append(
                        {"term": {"siteMetadata.tenant": group}})
                query["bool"]["must"].append(group_query)
                

        
        # We can provide this query as is. It'll get sanitized when it gets
        # converted from dict to json
        els = self._connect_elasticsearch()
        results = els.search(index="meta", query=query, size=1000, from_=page*1000)

        # We only want to return the actual sheets, not the elasticsearch cruft
        results = results["hits"]["hits"]
        results = [doc["_source"] for doc in results]

        return results
    
    def notate(self, doc: dict) -> None:
        doc_id = doc['docId']
        try:
            els = self._connect_elasticsearch()
            els.create(index="meta", id=doc_id, document=doc)
        except:
            raise HTTPException(status_code=500,
                                detail="Update failed for unknown reason")

    def update(self, doc_id: str, update_fields: dict) -> None:
        try:
            els = self._connect_elasticsearch()
            update = els.update(index="meta", id=doc_id, doc=update_fields, refresh=True)
        except:
            raise HTTPException(status_code=500,
                                detail="Update failed for unknown reason")

        # Elasticsearch didn't crash, but make sure the update was actually successful
        if update["result"] not in ['successful', 'updated', 'noop']:
            raise HTTPException(status_code=500,
                                detail="Update failed for unknown reason")
