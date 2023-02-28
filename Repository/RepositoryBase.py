from abc import ABC, abstractmethod

class RepoBase(ABC):
    
    @abstractmethod
    def find(self, filters: dict=None, groups: list=None, page: int=0) -> list[dict]:
        """A find should do a hard match on every filter
        If groups are provided, we should match one
        page allows for pagination if we're doing multiple searches
        
        Return a list of db results. This should JUST be the notation we care about, no db metadata"""
        pass
    
    @abstractmethod
    def notate(self, doc: dict) -> None:
        """ Add a document to the repo. Note that validation must be done beforehand """
        pass

    @abstractmethod
    def update(self, doc_id: str, update_fields: dict) -> None:
        """ Update a preexisting document.
        doc_id is the document to be updated
        update_fields is a dict of values to be updated. Any key not included in this parameter will not be updated """
        pass
    