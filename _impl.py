""" Implementation code for metarepo2. See the API and functions for details"""

import configparser
import time
import uuid

from enum import Enum
from elasticsearch import Elasticsearch
from fastapi import HTTPException

from _resolver import get_meta_site, get_meta_target
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

def connect_elasticsearch():
    """Perform the connection to elasticsearch, using details from the config"""
    els = Elasticsearch(
         config.get(
            "ELASTICSEARCH", "elastic_url"), ssl_assert_fingerprint=config.get(
            "ELASTICSEARCH", "cert_fingerprint"), basic_auth=(
                config.get(
                    "ELASTICSEARCH", "elastic_user"), config.get(
                        "ELASTICSEARCH", "elastic_password")))
    return els


def list_elasticsearch(page, user_info):
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
    query = {"match_all": {}}
    els = connect_elasticsearch()
    results = els.search(index="meta", query=query, size=1000, from_=page*1000)

    # We only want to return the actual sheets, not the elasticsearch cruft
    results = results["hits"]["hits"]
    results = [doc["_source"] for doc in results]

    return results


def find_elasticsearch(filters, user_info):
    """Construct a search using the elasticsearch DSL
    # For now, we're just doing a filter--"and" join all search parameters"""
    query = {"bool": {"must": []}}
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
        query_field = {"match": {tag: val}}
        query["query"]["bool"]["must"].append(query_field)

    # user can only see docs if they match the group
    groups = get_groups(user_info)
    groups = [group['idmGroupId'] for group in groups]
    groups.append(user_info["username"])  # sso is a valid "group"
    group_query = {"bool": {"should": []}}
    for group in groups:
        group_query["bool"]["should"].append(
            {"term": {"siteMetadata.tenant": group}})
    query["query"]["bool"]["must"].append(group_query)

    # We can provide this query as is. It'll get sanitized when it gets
    # converted from dict to json
    els = connect_elasticsearch()
    results = els.search(index="meta", query=query, size=100)

    # We only want to return the actual sheets, not the elasticsearch cruft
    results = results["hits"]["hits"]
    results = [doc["_source"] for doc in results]

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
    metasheet['metadata'] = notate_body.metadata or {}
    notate_body.targetMetadata = notate_body.targetMetadata or {}
    notate_body.siteMetadata = notate_body.siteMetadata or {}
    metasheet['frameworkArchive'] = []
    metasheet['metadataArchive'] = []
    metasheet['targetMetadataArchive'] = []
    metasheet['siteMetadataArchive'] = []

    if 'targetType' not in notate_body:
        raise HTTPException(
            status_code=400,
            detail="Must include a targetType")
    meta_target = get_meta_target(notate_body)()
    metasheet['targetMetadata'] = meta_target.validate_target_metadata(
        notate_body, user_info)

    if 'siteType' not in notate_body:
        raise HTTPException(status_code=400, detail="Must include a siteType")
    meta_site = get_meta_site(notate_body)()
    metasheet['siteMetadata'] = meta_site.validate_site_metadata(
        notate_body, user_info)

    els = connect_elasticsearch()
    els.create(index="meta", id=doc_id, document=metasheet)

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

    els = connect_elasticsearch()
    els.create(index="meta", id=doc_id, document=metasheet)

    ret_val = {'docId': doc_id}

    return ret_val


def update_doc(notate_body, user_info):
    """Given a docId of a previously created document, update it
    # Note that if metadata fields are updated, they're saved in the archive

    # First, make sure the document exists and is available to the user"""
    doc_id = notate_body.docId
    timestamp = time.time()
    archive_format = {"timestamp": timestamp,
                      "userId": user_info["username"],
                      "comment": notate_body.archiveComment or '',
                      "previous": {}}
    find_filters = {"docId": doc_id}
    doc = find_elasticsearch(find_filters, user_info)
    if not doc:
        raise HTTPException(
            status_code=404,
            detail="No matching document found")
    # findElasticsearch for a docId returns a list, so extract the doc
    doc = doc[0]

    # We found a document, so initialize and construct the query, validating
    # as we go
    update_query = {}

    # There are two framework level fields that might be changed, update them
    # and then the archive if needed
    old_framework = {}
    if notate_body.docSetId is not None:
        update_query["displayName"] = notate_body.displayName
        old_framework["displayName"] = doc["displayName"]

    if notate_body.displayName is not None:
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
    if notate_body.metadata is not None:
        update_query["metadata"] = notate_body.metadata
        archive_format["previous"] = doc["metadata"]
        metadata_archive = doc["metadataArchive"]
        metadata_archive.append(archive_format)
        update_query["metadataArchive"] = metadata_archive

    meta_target = get_meta_target(doc['targetType'])()
    update_query = meta_target.update_target_metadata(
        doc, notate_body, update_query, archive_format)

    meta_site = get_meta_site(doc['siteType'])()
    update_query = meta_site.update_site_metadata(
        doc, notate_body, update_query, archive_format)

    els = connect_elasticsearch()
    update = els.update(index="meta", id=doc_id, doc=update_query, refresh=True)
    if update["result"] not in ['successful', 'updated', 'noop']:
        raise HTTPException(status_code=500,
                            detail="Update failed for unknown reason")
    return ''
