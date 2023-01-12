import configparser
import re
import requests
import time
import uuid

from elasticsearch import Elasticsearch
from enum import Enum
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import List, Union

config = configparser.ConfigParser()
config.read('metarepo.conf')

metaRepoApp = FastAPI()

### DOC STATUS NAMES
class DocStatus(Enum):
    IN_PROGRESS = 0
    AVAILABLE = 1
    DELETED = 2


#### HELPER METHODS

def getS3(targetMetadata):
    targetMetadata['storageKey'] = 0
    targetMetadata['bucketName'] = 0
    targetMetadata['repositoryIdentifier'] = 0
    return targetMetadata

def authenticate(token):
    # TODO: Does logging in as SSO properly update expiresAt time?
    if token is None: return None
    if not token.startswith("Bearer "): # We're using bearer tokens, it may or may not come with the prefix
        token = "Bearer " + token
    url = config.get('AUTHSERVICE', 'admin_url') + config.get('AUTHSERVICE', 'checkAuth_endpoint')
    s = requests.session()
    m = s.post(url, headers={"Authorization": token})
    if m.status_code == 200:
        return m.json()
    return None

def getGroups(userInfo):
    if userInfo is None: return []
    groups = userInfo['ownerGroups']
    return groups

def inGroup(userInfo, group, sso_allowed=False):
    groups = getGroups(userInfo)
    for usrGroup in groups:
        if group == usrGroup['idmGroupId']:
            return True
    if sso_allowed and userInfo["username"] == group:
        return True
    return False

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
        groupQuery["bool"]["should"].append({"term" : {"systemMetadata.tenant" : group}})
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
    sso = userInfo["username"]
    metasheet = {}
    metasheet['docId'] = docId # This is going to be redundant with the elasticsearch ID
    metasheet['docSetId'] = notateBody.docSetId or []
    metasheet['status'] = DocStatus.IN_PROGRESS.value
    metasheet['displayName'] = notateBody.displayName or ''
    metasheet['timestamp'] = time.time()
    
    # Initialize our metadata fields in case the user didn't supply them
    notateBody.metadata = notateBody.metadata or {}
    notateBody.targetMetadata = notateBody.targetMetadata or {}
    notateBody.systemMetadata = notateBody.systemMetadata or {}

    # Make sure targetMetadata doesn't have any fields it's not supposed to    
    targetMetadataFields = ['fileName', 'filePath', 'fileSize']
    for key in notateBody.targetMetadata:
        if key not in targetMetadataFields:
            raise HTTPException(status_code=400, detail="Incorrect targetMetadata field %s!" % key)

    # Finalize targetMetadata
    if 'fileName' not in notateBody.targetMetadata:
        raise HTTPException(status_code=400, detail="targetMetadata must include a fileName!")
    if 'filePath' not in notateBody.targetMetadata:
        raise HTTPException(status_code=400, detail="targetMetadata must include a filePath!")
    if 'fileSize' not in notateBody.targetMetadata:
        raise HTTPException(status_code=400, detail="targetMetadata must include a fileSize!")

    # Generate storageKey, bucketName, and repositoryIdentifier
    notateBody.targetMetadata = getS3(notateBody.targetMetadata)

    # Make sure systemMetadata doesn't have any fields it's not supposed to    
    systemMetadataFields = ['type', 'versionMajor', 'versionMinor', 'versionPatch', 'tenant', 'workflowId', 'parentWorkflowId', 'originatorWorkflowId']
    for key in notateBody.systemMetadata:
        if key not in systemMetadataFields:
            raise HTTPException(status_code=400, detail="Incorrect targetMetadata field %s!" % key)

    # Finalize systemMetadata
    # Users can do whatever 'type' they want, but for now we typically use SIM and TOOL
    if 'type' not in notateBody.systemMetadata:
        raise HTTPException(status_code=400, detail="systemMetadata must include a type!")
    if 'versionMajor' not in notateBody.systemMetadata:
        notateBody.systemMetadata['versionMajor'] = 1
    if 'versionMinor' not in notateBody.systemMetadata:
        notateBody.systemMetadata['versionMinor'] = 0
    if 'versionPatch' not in notateBody.systemMetadata:
        notateBody.systemMetadata['versionPatch'] = 0

    if 'tenant' not in notateBody.systemMetadata:
        raise HTTPException(status_code=400, detail="systemMetadata must include a tenant!")
    if not inGroup(userInfo, notateBody.systemMetadata['tenant'], True):
        raise HTTPException(status_code=401, detail="user does not belong to this tenant!")
        
    if 'workflowId' not in notateBody.systemMetadata:
        raise HTTPException(status_code=400, detail="systemMetadata must include a workflowId!")
    if 'parentWorkflowId' not in notateBody.systemMetadata:
        raise HTTPException(status_code=400, detail="systemMetadata must include a parentWorkflowId!")
    if 'originatorWorkflowId' not in notateBody.systemMetadata:
        raise HTTPException(status_code=400, detail="systemMetadata must include a originatorWorkflowId!")
        
    notateBody.systemMetadata['userId'] = sso
    

    metasheet['metadata'] = notateBody.metadata
    metasheet['targetMetadata'] = notateBody.targetMetadata
    metasheet['systemMetadata'] = notateBody.systemMetadata
    metasheet['metadataArchive'] = {}
    metasheet['targetMetadataArchive'] = {}
    metasheet['systemMetadataArchive'] = {}

    es = connectElasticsearch()
    es.create(index="meta", id=docId, document=metasheet)
    
    retVal = {'docId' : docId,
              'systemVals' : {'storageKey' : notateBody.targetMetadata['storageKey'],
                              'bucketName' : notateBody.targetMetadata['bucketName'],
                              'reposoitoryIdentifier' : notateBody.targetMetadata['repositoryIdentifier']}}
    
    return retVal

def completeDoc(docId, userInfo):
    # When a document is created, it starts with an IN_PROGRESS status, then the user has to upload the file, then let the API know to switch to AVAILABLE status
    #  Make sure that a document exists that 1) belongs to a group the user is in, and 2) is in progress
    findFilters = {"docId" : docId, "status" : DocStatus.IN_PROGRESS.value}
    doc = findElasticsearch(findFilters, userInfo)
    if not doc:
        raise HTTPException(status_code=404, detail="No matching document found")

    # We've found a matching document, so we can actually do an update
    updateQuery = {"doc" : {"status" : DocStatus.AVAILABLE.value}}
    es = connectElasticsearch()
    update = es.update(index="meta", id=docId, body=updateQuery, refresh=True)
    if update["result"] not in ['successful', 'updated', 'noop'] :
        raise HTTPException(status_code=500, detail="Update failed for unknown reason")
    return ''
    
    

def updateDoc(notateBody, userInfo):
    # Given a docId of a previously created document, update it
    # Note that if metadata fields are updated, they're saved in the archive
    
    # First, make sure the document exists and is available to the user
    docId = notateBody.docId
    timestamp = time.time()
    findFilters = {"docId" : docId}
    doc = findElasticsearch(findFilters, userInfo)
    if not doc:
        raise HTTPException(status_code=404, detail="No matching document found")
    doc = doc[0] # findElasticsearch for a docId returns a list, so extract the doc

    # We found a document, so initialize and construct the query, validating as we go
    updateQuery = {"doc" : {}}
    
    if notateBody.docSetId is not None:
        updateQuery["doc"]["docSetId"] = notateBody.docSetId
    
    if notateBody.displayName is not None:
        updateQuery["doc"]["displayName"] = notateBody.displayName
        
    # If we update any metadata, save it in the archive
    if notateBody.metadata is not None:
        updateQuery["doc"]["metadata"] = notateBody.metadata
        oldMetadata = doc["metadata"]
        metadataArchive = doc["metadataArchive"]
        metadataArchive[timestamp] = oldMetadata
        updateQuery["doc"]["metadataArchive"] = metadataArchive

    if notateBody.targetMetadata is not None:
        # Make sure targetMetadata doesn't have any fields it's not supposed to    
        targetMetadataFields = ['fileName', 'filePath', 'fileSize']
        for key in notateBody.targetMetadata:
            if key not in targetMetadataFields:
                raise HTTPException(status_code=400, detail="Incorrect targetMetadata field %s!" % key)

        updateQuery["doc"]["targetMetadata"] = notateBody.targetMetadata
        oldTargetMetadata = doc["targetMetadata"]
        targetMetadataArchive = doc["targetMetadataArchive"]
        targetMetadataArchive[timestamp] = oldTargetMetadata
        updateQuery["doc"]["targetMetadataArchive"] = targetMetadataArchive

    if notateBody.systemMetadata is not None:
        # Make sure systemMetadata doesn't have any fields it's not supposed to    
        systemMetadataFields = ['type', 'versionMajor', 'versionMinor', 'versionPatch', 'tenant', 'workflowId', 'parentWorkflowId', 'originatorWorkflowId']
        for key in notateBody.systemMetadata:
            if key not in systemMetadataFields:
                raise HTTPException(status_code=400, detail="Incorrect targetMetadata field %s!" % key)

        updateQuery["doc"]["systemMetadata"] = notateBody.systemMetadata
        oldsystemMetadata = doc["systemMetadata"]
        systemMetadataArchive = doc["systemMetadataArchive"]
        systemMetadataArchive[timestamp] = oldsystemMetadata
        updateQuery["doc"]["systemMetadataArchive"] = systemMetadataArchive

    es = connectElasticsearch()
    update = es.update(index="meta", id=docId, body=updateQuery, refresh=True)
    if update["result"] not in ['successful', 'updated', 'noop'] :
        raise HTTPException(status_code=500, detail="Update failed for unknown reason")
    return ''

### REQUEST BODY STRUCTURES

class NotateBody(BaseModel):
    docId: Union[str, None] = None
    docSetId: Union[List[str], None] = None
    displayName: Union[str, None] = None
    metadata: Union[dict, None] = None
    systemMetadata: Union[dict, None] = None
    targetMetadata: Union[dict, None] = None
    
class CompleteBody(BaseModel):
    docId: str = ''

class FindBody(BaseModel):
    filters: dict = []

### API ENDPOINTS

@metaRepoApp.post("/metarepo/notate")
def notate(notateBody: NotateBody,
         authorization: Union[str, None] = Header(default=None)):
    authorization = authenticate(authorization)
    if authorization is None or int(authorization["expiresAt"]) < time.time()*1000: # expiresAt is in ms, time.time() is in seconds
        raise HTTPException(status_code=401)
    
    if notateBody.docId is None:
        retVal = createDoc(notateBody, authorization)
    else:
        retVal = updateDoc(notateBody, authorization)
        
    return retVal

@metaRepoApp.post("/metarepo/complete")
def complete(completeBody: CompleteBody,
         authorization: Union[str, None] = Header(default=None)):
    authorization = authenticate(authorization)
    if authorization is None or int(authorization["expiresAt"]) < time.time()*1000:
        raise HTTPException(status_code=401)
    return completeDoc(completeBody.docId, authorization)
    

@metaRepoApp.get("/metarepo/find")
def find(findBody: FindBody,
         authorization: Union[str, None] = Header(default=None)):
    authorization = authenticate(authorization)
    if not authorization or int(authorization["expiresAt"]) < time.time()*1000: # expiresAt is in ms, time.time() is in seconds
        raise HTTPException(status_code=401)

    # user can only see available docs
    findBody.filters["status"] = DocStatus.AVAILABLE.value

    return findElasticsearch(findBody.filters, authorization)
    