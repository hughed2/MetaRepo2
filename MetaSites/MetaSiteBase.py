class MetaSiteBase:
    name = 'MetaSiteBase'
    
    def validateSiteMetadata(self, doc : dict, userInfo : dict) -> dict:
        pass
    
    def updateSiteMetadata(self, doc : dict, updateBody: dict, updateQuery : dict, userInfo : dict) -> dict:
        pass