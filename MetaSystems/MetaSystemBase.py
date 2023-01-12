class MetaSystemBase:
    name = ''
    
    def validateSystemMetadata(self, doc : dict, userInfo : dict) -> dict:
        pass
    
    def updateSystemMetadata(self, doc : dict, updateBody: dict, updateQuery : dict, userInfo : dict) -> dict:
        pass