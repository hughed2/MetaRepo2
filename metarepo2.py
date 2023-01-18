""" The basic API for MetaRepo2. This file should never contain much more
than is needed to understand each endpoint's inputs and output """

from typing import List, Union
from attr import define
from fastapi import FastAPI, Header

import _impl

from auth import authenticate, check_authorization

metaRepoApp = FastAPI()

### REQUEST BODY STRUCTURES

# The following line is an instruction to "pylint" so devs don't get errors
# pylint: disable=too-few-public-methods missing-class-docstring

@define
class NotateBody:
    docId: Union[str, None] = None
    docSetId: Union[List[str], None] = None
    displayName: Union[str, None] = None
    userMetadata: Union[dict, None] = None
    siteClass: Union[str, None] = None
    siteMetadata: Union[dict, None] = None
    targetClass: Union[str, None] = None
    targetMetadata: Union[dict, None] = None
    archiveComment: Union[str, None] = None

@define
class FindBody():
    filters: dict = {}

### API ENDPOINTS

@metaRepoApp.post("/metarepo/notate")
def notate(notate_body: NotateBody,
         authorization: Union[str, None] = Header(default=None)) -> str:
    """ Add or update a document within the metarepo """
    authorization = authenticate(authorization)
    check_authorization(authorization)

    if notate_body.docId is None:
        ret_val = _impl.create_doc(notate_body, authorization)
    else:
        ret_val = _impl.update_doc(notate_body, authorization)

    return ret_val

@metaRepoApp.get("/metarepo/find")
def find(find_body: FindBody,
         authorization: Union[str, None] = Header(default=None)) -> List[dict]:
    """ Use filters to find a document within the metarepo """
    authorization = authenticate(authorization)
    check_authorization(authorization)

    # user can only see available docs
    find_body.filters["status"] = _impl.DocStatus.AVAILABLE.value

    return _impl.find_elasticsearch(find_body.filters, authorization)

@metaRepoApp.get("/metarepo/admin/find_all")
def find_all(page: int,
         authorization: Union[str, None] = Header(default=None)) -> List[dict]:
    """ Admin only: list all documents within the metarepo """
    authorization = authenticate(authorization)
    check_authorization(authorization)

    return _impl.find_all_elasticsearch(page, authorization)

@metaRepoApp.post("/metarepo/admin/forceNotate")
def force_notate(metasheet: dict,
         authorization: Union[str, None] = Header(default=None)) -> str:
    """ Admin only: add a doc to the metarepo without validation """
    authorization = authenticate(authorization)
    check_authorization(authorization)

    ret_val = _impl.force_notate(metasheet, authorization)

    return ret_val
