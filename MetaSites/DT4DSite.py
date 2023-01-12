""" Validator for the DT4D Site """

# This file does a lot of weird things that pylint doesn't like
# pylint: skip-file

from fastapi import HTTPException

# Can't do an "import ..auth" in importlib, so we need to add '..' to the path
import sys
sys.path.append('..')
from auth import in_group

from .MetaSiteBase import MetaSiteBase

class DT4DSite(MetaSiteBase):
    name = 'DT4DSite'

    def validate_site_metadata(self, doc : dict, user_info : dict) -> dict:
        doc = doc.siteMetadata # We don't need the rest of the document
        # We need to make sure the user hasn't provided any extra siteMetadata fields
        siteMetadataFields = ['type', 'versionMajor', 'versionMinor', 'versionPatch',
                              'tenant', 'workflowId',
                              'parentWorkflowId', 'originatorWorkflowId']
        for key in doc:
            if key not in siteMetadataFields:
                raise HTTPException(status_code=400,
                                    detail=f"Incorrect targetMetadata field {key}!")

        # Users can do whatever 'type' they want, but for now we typically use SIM and TOOL
        if 'type' not in doc:
            raise HTTPException(status_code=400, detail="siteMetadata must include a type!")

        # If Version is not specified, just assume 1.0.0. Instead, we should check resource
        # name and dynamically determine the next version if none is provided
        if 'versionMajor' not in doc:
            doc['versionMajor'] = 1
        if 'versionMinor' not in doc:
            doc['versionMinor'] = 0
        if 'versionPatch' not in doc:
            doc['versionPatch'] = 0

        # Tenancy is a type of permission, so we need to verify the user belongs
        if 'tenant' not in doc:
            raise HTTPException(status_code=400, detail="siteMetadata must include a tenant!")
        if not in_group(user_info, doc['tenant'], True):
            raise HTTPException(status_code=401, detail="user does not belong to this tenant!")

        if 'workflowId' not in doc:
            raise HTTPException(status_code=400, detail="siteMetadata must include a workflowId!")
        if 'parentWorkflowId' not in doc:
            raise HTTPException(status_code=400,
                            detail="siteMetadata must include a parentWorkflowId!")
        if 'originatorWorkflowId' not in doc:
            raise HTTPException(status_code=400,
                                detail="siteMetadata must include a originatorWorkflowId!")

        doc['userId'] = user_info["username"]

        return doc


    def update_site_metadata(self, doc : dict, update_body: dict,
                           update_query : dict, archive_format : dict) -> dict:
        if update_body.site_metadata is not None:
            # Make sure siteMetadata doesn't have any fields it's not supposed to
            site_metadata_fields = ['type', 'versionMajor', 'versionMinor', 'versionPatch',
                                  'tenant', 'workflowId',
                                  'parentWorkflowId', 'originatorWorkflowId']
            for key in update_body.siteMetadata:
                if key not in site_metadata_fields:
                    raise HTTPException(status_code=400,
                                        detail="Incorrect targetMetadata field {key}!")

            update_query["siteMetadata"] = update_body.siteMetadata
            archive_format["previous"] = doc["siteMetadata"]
            site_metadata_archive = doc["siteMetadataArchive"]
            site_metadata_archive.append(archive_format)
            update_query["siteMetadataArchive"] = site_metadata_archive

        return update_query
