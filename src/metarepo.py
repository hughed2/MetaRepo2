""" The basic API for the Meta Repo. This file should never contain much more
than is needed to understand each endpoint's inputs and output """

from typing import List, Union
from fastapi import FastAPI, Header
from pydantic import BaseModel

from . import _metaImpl

from .auth import authenticate, check_authorization

app = FastAPI()

### REQUEST BODY STRUCTURES

# The following line is an instruction to "pylint" so devs don't get errors
# pylint: disable=too-few-public-methods missing-class-docstring

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

@app.post("/notate")
def notate(notate_body: NotateBody,
         authorization: Union[str, None] = Header(default=None)) -> str:
    """ Add or update a document within the metarepo """
    authorization = authenticate(authorization)
    check_authorization(authorization)

    if notate_body.docId is None:
        ret_val = _metaImpl.create_doc(notate_body, authorization)
    else:
        ret_val = _metaImpl.update_doc(notate_body, authorization)

    return str(ret_val)

@app.get("/find")
def find(find_body: FindBody,
         authorization: Union[str, None] = Header(default=None)) -> List[dict]:
    """ Use filters to find a document within the metarepo """
    authorization = authenticate(authorization)
    check_authorization(authorization)

    # user can only see available docs
    find_body.filters["status"] = _metaImpl.DocStatus.AVAILABLE.value

    return _metaImpl.find(find_body.filters, authorization)

@app.get("/admin/find_all")
def find_all(page: int,
         authorization: Union[str, None] = Header(default=None)) -> List[dict]:
    """ Admin only: list all documents within the metarepo """
    authorization = authenticate(authorization)
    check_authorization(authorization)

    return _metaImpl.find_all(page, authorization)

@app.post("/admin/forceNotate")
def force_notate(metasheet: dict,
         authorization: Union[str, None] = Header(default=None)) -> str:
    """ Admin only: add a doc to the metarepo without validation """
    authorization = authenticate(authorization)
    check_authorization(authorization)

    ret_val = _metaImpl.force_notate(metasheet, authorization)

    return ret_val
