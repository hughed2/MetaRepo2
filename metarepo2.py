import time

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import List, Union

from _impl import authenticate, createDoc, updateDoc, DocStatus, findElasticsearch

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
    
class FindBody(BaseModel):
    filters: dict = {}
    comment: Union[str, None] = ''

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
    