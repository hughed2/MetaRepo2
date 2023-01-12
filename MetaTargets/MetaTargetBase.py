class MetaTargetBase:
    name = ''
    
    def validateTargetMetadata(self, doc : dict, userInfo : dict) -> dict:
        pass
    
    def updateTargetMetadata(self, doc : dict, updateBody: dict, updateQuery : dict, userInfo : dict) -> dict:
        pass