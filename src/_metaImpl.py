""" Implementation code for the metarepo. See the API and functions for details"""

import configparser
import time
import uuid

from enum import Enum
from fastapi import HTTPException

from _resolver import get_meta_site, get_meta_target, get_repo
from auth import get_groups


config = configparser.ConfigParser()
config.read('metarepo.conf')



# DOC STATUS NAMES


class DocStatus(Enum):
    """An enum for different doc status types. Feel free to add more in inherited classes"""
    IN_PROGRESS = 0
    AVAILABLE = 1
    DELETED = 2


# HELPER METHODS

def find_all(page, user_info):
    """Returns all documents, 1000 at a time. Fow now, it's admin only.
    "page" allows pagination for more docs, with a max of 10k results"""

    # Check the user group -- only admins allowed for now
    groups = get_groups(user_info)
    groups = [group['idmGroupId'] for group in groups]
    admin_group = config.get("ADMIN", "admin_group", fallback=None)
    if admin_group is None or admin_group not in groups:
        raise HTTPException(
            status_code=401,
            detail="Only members of the admin group may use the list query")
            
    # Now perform a match_all query with the appropriate page
    repo = get_repo()()
    results = repo.find(page=page)

    return results


def find(filters, user_info):
    """Construct a search using the elasticsearch DSL
    # For now, we're just doing a filter--"and" join all search parameters"""
    for tag in filters:
        val = filters[tag]
        if not isinstance(
                val,
                str) and not isinstance(
                val,
                int) and not isinstance(
                val,
                float):
            raise HTTPException(
                status_code=400,
                detail="filters field must map a string to a string, int, or float")

    # user can only see docs if they match the group
    groups = get_groups(user_info)
    groups = [group['idmGroupId'] for group in groups]
    groups.append(user_info["username"])  # sso is a valid "group"

    repo = get_repo()()
    results = repo.find(filters, groups)

    return results


def create_doc(notate_body, user_info):
    """Add a document to the repo
    For a first draft, we're assuming every doc corresponds to an s3 file
    So we need to fill in our fields for a DT4D s3 metasheet, and then add to elasticsearch"""

    doc_id = str(uuid.uuid4())
    metasheet = {}
    # This is going to be redundant with the elasticsearch ID
    metasheet['docId'] = doc_id
    metasheet['docSetId'] = notate_body.docSetId or []
    metasheet['status'] = DocStatus.AVAILABLE.value
    metasheet['displayName'] = notate_body.displayName or ''
    metasheet['timestamp'] = time.time()

    # Initialize our metadata fields in case the user didn't supply them
    metasheet['userMetadata'] = notate_body.userMetadata or {}
    notate_body.targetMetadata = notate_body.targetMetadata or {} # We'll fill in the actual metasheet next
    notate_body.siteMetadata = notate_body.siteMetadata or {}
    metasheet['frameworkArchive'] = []
    metasheet['metadataArchive'] = []
    metasheet['targetMetadataArchive'] = []
    metasheet['siteMetadataArchive'] = []

    if notate_body.targetClass is None:
        raise HTTPException(
            status_code=400,
            detail="Must include a targetClass")
    metasheet["targetClass"] = notate_body.targetClass
    meta_target = get_meta_target(notate_body.targetClass)()
    metasheet['targetMetadata'] = meta_target.validate_target_metadata(
        notate_body, user_info)

    if notate_body.siteClass is None:
        raise HTTPException(
            status_code=400,
            detail="Must include a siteClass")
    metasheet["siteClass"] = notate_body.siteClass
    meta_site = get_meta_site(notate_body.siteClass)()
    metasheet['siteMetadata'] = meta_site.validate_site_metadata(
        notate_body, user_info)

    repo = get_repo()()
    repo.notate(metasheet)

    ret_val = {'docId': doc_id}

    return ret_val


def force_notate(metasheet, user_info):
    """Add a document to the repo with no validation"""

    # Check the user group -- only admins allowed
    groups = get_groups(user_info)
    groups = [group['idmGroupId'] for group in groups]
    admin_group = config.get("ADMIN", "admin_group", fallback=None)
    if admin_group is None or admin_group not in groups:
        raise HTTPException(
            status_code=401,
            detail="Only members of the admin group may use the forceNotate query")

    # If a docId isn't supplied, we have to make one
    doc_id = metasheet.get('docId', str(uuid.uuid4()))
    metasheet['docId'] = doc_id

    repo = get_repo()()
    repo.notate(metasheet)

    ret_val = {'docId': doc_id}

    return ret_val


def update_doc(notate_body, user_info):
    """Given a docId of a previously created document, update it
    # Note that if metadata fields are updated, they're saved in the archive

    # First, make sure the document exists and is available to the user"""
    doc_id = notate_body.docId
    repo = get_repo()()
    find_filters = {"docId": doc_id}
    doc = find(find_filters, user_info)
    if not doc:
        raise HTTPException(
            status_code=404,
            detail="No matching document found")
    # find returns a list, so extract the doc
    doc = doc[0]

    # We found a document, so initialize and construct the query, validating
    # as we go
    timestamp = time.time()
    archive_format = {"timestamp": timestamp,
                      "userId": user_info["username"],
                      "comment": notate_body.archiveComment or '',
                      "previous": {}}
    update_query = {}

    # There are two framework level fields that might be changed, update them
    # and then the archive if needed
    old_framework = {}
    if notate_body.displayName is not None:
        update_query["displayName"] = notate_body.displayName
        old_framework["displayName"] = doc["displayName"]

    if notate_body.docSetId is not None:
        update_query["docSetId"] = notate_body.docSetId
        old_framework["docSetId"] = doc["docSetId"]

    # oldFramework keeps track of framework level metadata changes--if it has
    # anything, we need to add it to the archive
    if old_framework:
        archive_format["previous"] = old_framework
        framework_archive = doc["frameworkArchive"]
        framework_archive.append(archive_format)
        update_query["frameworkArchive"] = framework_archive

    # If we update any metadata, save it in the archive
    if notate_body.userMetadata is not None:
        update_query["userMetadata"] = notate_body.userMetadata
        archive_format["previous"] = doc["userMetadata"]
        metadata_archive = doc["metadataArchive"]
        metadata_archive.append(archive_format)
        update_query["metadataArchive"] = metadata_archive

    meta_target = get_meta_target(doc["targetClass"])()
    update_query = meta_target.update_target_metadata(
        doc, notate_body, update_query, archive_format)

    meta_site = get_meta_site(doc["siteClass"])()
    update_query = meta_site.update_site_metadata(
        doc, notate_body, update_query, archive_format)

    repo.update(doc_id, update_query)

    return ''
