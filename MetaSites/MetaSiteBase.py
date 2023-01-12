# This file does a lot of weird things that pylint doesn't like
# pylint: skip-file

class MetaSiteBase:
    name = 'MetaSiteBase'
    
    def validate_site_metadata(self, doc : dict, user_info : dict) -> dict:
        pass
    
    def update_site_metadata(self, doc : dict, update_body: dict, update_query : dict, user_info : dict) -> dict:
        pass
