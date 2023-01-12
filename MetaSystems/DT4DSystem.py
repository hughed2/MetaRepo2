from fastapi import HTTPException

from ..auth import inGroup 
from MetaSystemBase import MetaSystemBase

class DT4DSystem(MetaSystemBase):
    name = 'DT4DSystem'
    
    def validateSystemMetadata(self, doc : dict, userInfo : dict) -> dict:
        doc = doc.systemMetadata # We don't need the rest of the document
        # We need to make sure the user hasn't provided any systemMetadata fields they aren't supposed to
        systemMetadataFields = ['type', 'versionMajor', 'versionMinor', 'versionPatch', 'tenant', 'workflowId', 'parentWorkflowId', 'originatorWorkflowId']
        for key in doc:
            if key not in systemMetadataFields:
                raise HTTPException(status_code=400, detail="Incorrect targetMetadata field %s!" % key)
    
        # Users can do whatever 'type' they want, but for now we typically use SIM and TOOL
        if 'type' not in doc:
            raise HTTPException(status_code=400, detail="systemMetadata must include a type!")

        # If Version is not specified, just assume 1.0.0
        # TODO: Check resource name and dynamically determine a version if none is provided
        if 'versionMajor' not in doc:
            doc['versionMajor'] = 1
        if 'versionMinor' not in doc:
            doc['versionMinor'] = 0
        if 'versionPatch' not in doc:
            doc['versionPatch'] = 0

        # Tenancy is a type of permission, so we need to verify the user belongs
        if 'tenant' not in doc:
            raise HTTPException(status_code=400, detail="systemMetadata must include a tenant!")
        if not inGroup(userInfo, doc['tenant'], True):
            raise HTTPException(status_code=401, detail="user does not belong to this tenant!")
            
        if 'workflowId' not in doc:
            raise HTTPException(status_code=400, detail="systemMetadata must include a workflowId!")
        if 'parentWorkflowId' not in doc:
            raise HTTPException(status_code=400, detail="systemMetadata must include a parentWorkflowId!")
        if 'originatorWorkflowId' not in doc:
            raise HTTPException(status_code=400, detail="systemMetadata must include a originatorWorkflowId!")
            
        doc['userId'] = userInfo["username"]
    
        return doc

    
    def updateSystemMetadata(self, doc : dict, updateBody: dict, updateQuery : dict, archiveFormat : dict) -> dict:
        if updateBody.systemMetadata is not None:
            # Make sure systemMetadata doesn't have any fields it's not supposed to    
            systemMetadataFields = ['type', 'versionMajor', 'versionMinor', 'versionPatch', 'tenant', 'workflowId', 'parentWorkflowId', 'originatorWorkflowId']
            for key in updateBody.systemMetadata:
                if key not in systemMetadataFields:
                    raise HTTPException(status_code=400, detail="Incorrect targetMetadata field %s!" % key)
    
            updateQuery["doc"]["systemMetadata"] = updateBody.systemMetadata
            archiveFormat["previous"] = doc["systemMetadata"]
            systemMetadataArchive = doc["systemMetadataArchive"]
            systemMetadataArchive.append(archiveFormat)
            updateQuery["doc"]["systemMetadataArchive"] = systemMetadataArchive
    
        return updateQuery