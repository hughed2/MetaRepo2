# This file does a lot of weird things that pylint doesn't like
# pylint: skip-file

from fastapi import HTTPException

from .MetaTargetBase import MetaTargetBase

class DT4DTarget(MetaTargetBase):
    name = 'DT4DTarget'
    
    def validate_target_metadata(self, doc : dict, user_info : dict) -> dict:
        doc = doc.targetMetadata # We don't need the rest of the document

        # DT4D Target Metadata is entirely supplied by the user. So make sure every key
        # has been provided, and no extra keys have been provided
        targetMetadataFields = ['fileName', 'filePath', 'fileSize', 'storageKey', 'bucketName']
        for key in doc:
            if key not in targetMetadataFields:
                raise HTTPException(status_code=400, detail="Incorrect targetMetadata field {key}!")
    
        # Finalize targetMetadata
        for key in targetMetadataFields:
            if key not in doc:
                raise HTTPException(status_code=400, detail=f"targetMetadata must include a {key}!")

        return doc

    
    def update_target_metadata(self, doc : dict, update_body: dict,
                               update_query : dict, archive_format : dict) -> dict:
        if update_body.targetMetadata is not None:
            # Make sure targetMetadata doesn't have any fields it's not supposed to    
            target_metadata_fields = ['fileName', 'filePath', 'fileSize']
            for key in update_body.targetMetadata:
                if key not in target_metadata_fields:
                    raise HTTPException(status_code=400, detail=f"Incorrect targetMetadata field {key}!")
    
            update_query["targetMetadata"] = update_body.target_metadata
            archive_format["previous"] = doc["targetMetadata"]
            target_metadata_archive = doc["targetMetadataArchive"]
            target_metadata_archive.append(archive_format)
            update_query["targetMetadataArchive"] = target_metadata_archive

        return update_query
