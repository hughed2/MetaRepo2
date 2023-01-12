import time

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import List, Union

from _impl import *

metaRepoApp = FastAPI()

### REQUEST BODY STRUCTURES

class NotateBody(BaseModel):
    docId: Union[str, None] = None
    docSetId: Union[List[str], None] = None
    displayName: Union[str, None] = None
    userMetadata: Union[dict, None] = None
    siteClass: Union[str, None] = None    
    siteMetadata: Union[dict, None] = None
    targetClass: Union[str, None] = None
    targetMetadata: Union[dict, None] = None
    archiveComment: Union[str, None] = None
    
class FindBody(BaseModel):
    filters: dict = {}

### API ENDPOINTS

@metaRepoApp.post("/metarepo/notate")
def notate(notateBody: NotateBody,
         authorization: Union[str, None] = Header(default=None)) -> str:
    authorization = authenticate(authorization)
    if authorization is None or int(authorization["expiresAt"]) < time.time()*1000: # expiresAt is in ms, time.time() is in seconds
        raise HTTPException(status_code=401)
    
    if notateBody.docId is None:
        retVal = createDoc(notateBody, authorization)
    else:
        retVal = updateDoc(notateBody, authorization)
        
    return retVal

@metaRepoApp.get("/metarepo/find")
def find(findBody: FindBody,
         authorization: Union[str, None] = Header(default=None)) -> List[dict]:
    authorization = authenticate(authorization)
    if not authorization or int(authorization["expiresAt"]) < time.time()*1000: # expiresAt is in ms, time.time() is in seconds
        raise HTTPException(status_code=401)

    # user can only see available docs
    findBody.filters["status"] = DocStatus.AVAILABLE.value

    return findElasticsearch(findBody.filters, authorization)
    
    
@metaRepoApp.get("/metarepo/admin/list")
def list(page: int,
         authorization: Union[str, None] = Header(default=None)) -> List[dict]:
    authorization = authenticate(authorization)
    if not authorization or int(authorization["expiresAt"]) < time.time()*1000: # expiresAt is in ms, time.time() is in seconds
        raise HTTPException(status_code=401)

    return listElasticsearch(page, authorization)
    
@metaRepoApp.post("/metarepo/admin/forceNotate")
def forceNotate(metasheet: dict,
         authorization: Union[str, None] = Header(default=None)) -> str:
    authorization = authenticate(authorization)
    if authorization is None or int(authorization["expiresAt"]) < time.time()*1000: # expiresAt is in ms, time.time() is in seconds
        raise HTTPException(status_code=401)
    
    retVal = forceNotate(metasheet, authorization)
        
    return retVal
