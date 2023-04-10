# This file does a lot of weird things that pylint doesn't like
# pylint: skip-file

class MetaTargetBase:
    name = ''
    
    def validate_target_metadata(self, doc : dict, user_info : dict) -> dict:
        pass
    
    def update_target_metadata(self, doc : dict, update_body: dict, update_query : dict, user_info : dict) -> dict:
        pass
