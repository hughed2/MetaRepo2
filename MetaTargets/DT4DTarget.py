from fastapi import HTTPException

from MetaTargetBase import MetaTargetBase

class DT4DTarget(MetaTargetBase):
    name = 'DT4DTarget'
    
    def validateTargetMetadata(self, doc : dict, userInfo : dict) -> dict:
        doc = doc.targetMetadata # We don't need the rest of the document

        # DT4D Target Metadata is entirely supplied by the user. So make sure every key
        # has been provided, and no extra keys have been provided
        targetMetadataFields = ['fileName', 'filePath', 'fileSize', 'storageKey', 'bucketName']
        for key in doc:
            if key not in targetMetadataFields:
                raise HTTPException(status_code=400, detail="Incorrect targetMetadata field %s!" % key)
    
        # Finalize targetMetadata
        for key in targetMetadataFields:
            if key not in doc:
                raise HTTPException(status_code=400, detail="targetMetadata must include a %s!" % key)

        return doc

    
    def updateTargetMetadata(self, doc : dict, updateBody: dict, updateQuery : dict, archiveFormat : dict) -> dict:
        if updateBody.targetMetadata is not None:
            # Make sure targetMetadata doesn't have any fields it's not supposed to    
            targetMetadataFields = ['fileName', 'filePath', 'fileSize']
            for key in updateBody.targetMetadata:
                if key not in targetMetadataFields:
                    raise HTTPException(status_code=400, detail="Incorrect targetMetadata field %s!" % key)
    
            updateQuery["doc"]["targetMetadata"] = updateBody.targetMetadata
            archiveFormat["previous"] = doc["targetMetadata"]
            targetMetadataArchive = doc["targetMetadataArchive"]
            targetMetadataArchive.append(archiveFormat)
            updateQuery["doc"]["targetMetadataArchive"] = targetMetadataArchive

        return updateQuery