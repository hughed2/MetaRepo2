import configparser
import time
import uuid

from elasticsearch import Elasticsearch
from fastapi import HTTPException
from enum import Enum

config = configparser.ConfigParser()
config.read('metarepo.conf')

from auth import getGroups

from _resolver import getMetaSite, getMetaTarget 

### DOC STATUS NAMES
class DocStatus(Enum):
    IN_PROGRESS = 0
    AVAILABLE = 1
    DELETED = 2


#### HELPER METHODS

def connectElasticsearch():
    es = Elasticsearch(config.get("ELASTICSEARCH", "elastic_url"),
                       ssl_assert_fingerprint=config.get("ELASTICSEARCH", "cert_fingerprint"),
                       basic_auth=(config.get("ELASTICSEARCH", "elastic_user"),
                                   config.get("ELASTICSEARCH", "elastic_password")))
    return es

def findElasticsearch(filters, userInfo):
    # We need to construct a search using the elasticsearch DSL
    # For now, we're just doing a filter--"and" join all search parameters
    query = {"query": {"bool" : {"must" : []}}}
    for tag in filters:
        val = filters[tag]
        if not isinstance(val, str) and not isinstance(val, int) and not isinstance(val, float) :
           raise HTTPException(status_code=400, detail="filters field must map a string to a string, int, or float")
        queryField = {"match" : {tag : val }}
        query["query"]["bool"]["must"].append(queryField)
        
    # user can only see docs if they match the group
    groups = getGroups(userInfo)
    groups = [group['idmGroupId'] for group in groups]
    groups.append(userInfo["username"]) # sso is a valid "group"
    groupQuery = {"bool" : {"should" : []}}
    for group in groups:
        groupQuery["bool"]["should"].append({"term" : {"siteMetadata.tenant" : group}})
    query["query"]["bool"]["must"].append(groupQuery)
    

    # We can provide this query as is. It'll get sanitized when it gets converted from dict to json
    es = connectElasticsearch()
    results = es.search(index="meta", body=query)

    # We only want to return the actual sheets, not the elasticsearch cruft
    results = results["hits"]["hits"]
    results = [doc["_source"] for doc in results]
    return results
    
def createDoc(notateBody, userInfo):
    # For a first draft, we're assuming every doc corresponds to an s3 file
    # So we need to fill in our fields for a DT4D s3 metasheet, and then add to elasticsearch
    docId = str(uuid.uuid4())
    metasheet = {}
    metasheet['docId'] = docId # This is going to be redundant with the elasticsearch ID
    metasheet['docSetId'] = notateBody.docSetId or []
    metasheet['status'] = DocStatus.AVAILABLE.value
    metasheet['displayName'] = notateBody.displayName or ''
    metasheet['timestamp'] = time.time()
    
    # Initialize our metadata fields in case the user didn't supply them
    metasheet['metadata'] = notateBody.metadata or {}
    notateBody.targetMetadata = notateBody.targetMetadata or {}
    notateBody.siteMetadata = notateBody.siteMetadata or {}
    metasheet['frameworkArchive'] = []
    metasheet['metadataArchive'] = []
    metasheet['targetMetadataArchive'] = []
    metasheet['siteMetadataArchive'] = []


    if 'targetType' not in notateBody:
        raise HTTPException(status_code=400, detail="Must include a targetType")
    metaTarget = getMetaTarget(notateBody['siteType'])()
    metasheet['targetMetadata'] = metaTarget.validateTargetMetadata(notateBody, userInfo)

    if 'siteType' not in notateBody:
        raise HTTPException(status_code=400, detail="Must include a siteType")
    metaSite = getMetaSite(notateBody['siteType'])()
    metasheet['siteMetadata'] = metaSite.validateSiteMetadata(notateBody, userInfo)

    es = connectElasticsearch()
    es.create(index="meta", id=docId, document=metasheet)
    
    retVal = {'docId' : docId}
    
    return retVal

def updateDoc(notateBody, userInfo):
    # Given a docId of a previously created document, update it
    # Note that if metadata fields are updated, they're saved in the archive
    
    # First, make sure the document exists and is available to the user
    docId = notateBody.docId
    timestamp = time.time()
    archiveFormat = {"timestamp" : timestamp,
                     "userId" : userInfo["username"],
                     "comment" : notateBody.archiveComment or '',
                     "previous" : {}}
    findFilters = {"docId" : docId}
    doc = findElasticsearch(findFilters, userInfo)
    if not doc:
        raise HTTPException(status_code=404, detail="No matching document found")
    doc = doc[0] # findElasticsearch for a docId returns a list, so extract the doc

    # We found a document, so initialize and construct the query, validating as we go
    updateQuery = {"doc" : {}}
    
    # There are two framework level fields that might be changed, update them and then the archive if needed
    oldFramework = {}
    if notateBody.docSetId is not None:
        updateQuery["doc"]["displayName"] = notateBody.displayName
        oldFramework["displayName"] = doc["displayName"]
        
    if notateBody.displayName is not None:
        updateQuery["doc"]["docSetId"] = notateBody.docSetId
        oldFramework["docSetId"] = doc["docSetId"]
        
    # oldFramework keeps track of framework level metadata changes--if it has anything, we need to add it to the archive
    if oldFramework:
        archiveFormat["previous"] = oldFramework
        frameworkArchive = doc["frameworkArchive"]
        frameworkArchive.append(archiveFormat)
        updateQuery["doc"]["frameworkArchive"] = frameworkArchive
    
        
    # If we update any metadata, save it in the archive
    if notateBody.metadata is not None:
        updateQuery["doc"]["metadata"] = notateBody.metadata
        archiveFormat["previous"] = doc["metadata"]
        metadataArchive = doc["metadataArchive"]
        metadataArchive.append(archiveFormat)
        updateQuery["doc"]["metadataArchive"] = metadataArchive

    metaTarget = getMetaTarget(doc['targetType'])()
    updateQuery = metaTarget.updateTargetMetadata(doc, notateBody, updateQuery, archiveFormat)

    metaSite = getMetaSite(doc['siteType'])()
    updateQuery = metaSite.updateSiteMetadata(doc, notateBody, updateQuery, archiveFormat)

    es = connectElasticsearch()
    update = es.update(index="meta", id=docId, body=updateQuery, refresh=True)
    if update["result"] not in ['successful', 'updated', 'noop'] :
        raise HTTPException(status_code=500, detail="Update failed for unknown reason")
    return ''

