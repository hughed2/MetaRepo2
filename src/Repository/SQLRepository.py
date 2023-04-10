import configparser
import sqlite3
import time

from fastapi import HTTPException

from .RepositoryBase import RepoBase

config = configparser.ConfigParser()
config.read('metarepo.conf')

class SQLRepository(RepoBase):
    """
    Implementation of a SQL based Metarepo. Because we can't store an arbitrary dict
    and still have performant searches, we arrange things a bit differently. All metadata
    goes into a Metadata table regardless of type, and can get organized into dicts
    when extracting. DocSets go into a dict as well. Everything has a primary key
    timestamp, so the archives just a matter of just organizing by timestamp and metadata type
    """

    def _connect_sql(self):
        """ Perform the connection to sqlite, and initialize tables if necessary """
        fn = config.get("SQL", "db_filename")
        con = sqlite3.connect(fn)
        cur = con.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS
                    Metasheets (docID CHAR, timestamp INT, displayName CHAR, targetClass CHAR, siteClass CHAR, status INT,
                                PRIMARY KEY (docID, timestamp))
                    """)
        cur.execute("""CREATE TABLE IF NOT EXISTS
                    Metadata (docID CHAR, timestamp INT, key CHAR, val CHAR, type CHAR,
                              PRIMARY KEY (docID, timestamp, key, type),
                              FOREIGN KEY(docID) REFERENCES Metasheets(docID))
                    """)
        cur.execute("""CREATE TABLE IF NOT EXISTS
                    DocSets (docID CHAR, timestamp INT, docSetId CHAR,
                             FOREIGN KEY(docID) REFERENCES Metasheets(docID))
                    """)
        
        con.commit() # Save new tables, if they were created
        return con
    
    def _get_metasheet(self, docId, con):
        """ Given a docId, get the entire document including metasheet and archives """
        cur = con.cursor()
        res = cur.execute(f"SELECT * FROM Metasheets WHERE docID = '{docId}' ORDER BY timestamp DESC")
        metasheets_sql = res.fetchall()
        
        res = cur.execute(f"SELECT * FROM Metadata WHERE docID = '{docId}' ORDER BY timestamp DESC")
        metadata = res.fetchall()

        res = cur.execute(f"SELECT * FROM DocSets WHERE docID = '{docId}'")
        docsets = res.fetchall()
        
        # Turn the metasheet data into a list of dicts
        metasheets = []
        for meta_sql in metasheets_sql:
            meta_dict = {}
            meta_dict["docId"] = meta_sql[0]
            meta_dict["timestamp"] = meta_sql[1]
            meta_dict["displayName"] = meta_sql[2]
            meta_dict["targetClass"] = meta_sql[3]
            meta_dict["siteClass"] = meta_sql[4]
            meta_dict["status"] = meta_sql[5]
            metasheets.append(meta_dict)
        
        metasheet = metasheets[0] # Since we ordered by descending timestamp, this will be the most recent
        metasheet["userMetadata"] = {}
        metasheet["siteMetadata"] = {}
        metasheet["targetMetadata"] = {}
        metasheet["frameworkArchive"] = []
        metasheet["metadataArchive"] = []
        metasheet["targetMetadataArchive"] = []
        metasheet["siteMetadataArchive"] = []
        
        # We get the framework metadata archive by looking at old records from the metasheet table
        for idx in range(len(metasheets)-1):
            if metasheets[idx]["displayName"] != metasheets[idx+1]["displayName"]:
                metasheet["frameworkArchive"].append({"timestamp" : metasheets[idx]["timestamp"],
                                         "userId" : 'UNIDENTIFIED_USER', # We need to find a way to get the user in here
                                         "comment" : '', # Should probably use a new table or something for comment archives
                                         "previous" : metasheets[idx+1]["displayName"]
                                         })
                
        # We get the three other metadata types from the metadata table. First, organize them
        metadata_dict = {"userMetadata" : {}, "siteMetadata" : {}, "targetMetadata" : {}}
        for datum in metadata:
            timestamp = datum[1]
            key = datum[2]
            val = datum[3]
            mType = datum[4]
            if timestamp not in metadata_dict[mType]:
                metadata_dict[mType][timestamp] = {}
            metadata_dict[mType][timestamp][key] = val
            
        # Now create the archives. We can just throw everything in rather than chopping down
        # Note that this may not work right pre python 3.7--before, dict key is order is probably preserved but not guaranteed
        timestamps = list(metadata_dict["userMetadata"].keys())
        for idx in range(len(timestamps)-1):
            metasheet["metadataArchive"].append({"timestamp" : timestamps[idx],
                                     "userId" : 'UNIDENTIFIED_USER', # We need to find a way to get the user in here
                                     "comment" : '', # Should probably use a new table or something for comment archives
                                     "previous" : metadata_dict["userMetadata"][timestamps[idx+1]]
                                     })
        timestamps = list(metadata_dict["siteMetadata"].keys())
        for idx in range(len(timestamps)-1):
            metasheet["siteMetadataArchive"].append({"timestamp" : timestamps[idx],
                                     "userId" : 'UNIDENTIFIED_USER', # We need to find a way to get the user in here
                                     "comment" : '', # Should probably use a new table or something for comment archives
                                     "previous" : metadata_dict["siteMetadata"][timestamps[idx+1]]
                                     })
        timestamps = list(metadata_dict["targetMetadata"].keys())
        for idx in range(len(timestamps)-1):
            metasheet["targetMetadataArchive"].append({"timestamp" : timestamps[idx],
                                     "userId" : 'UNIDENTIFIED_USER', # We need to find a way to get the user in here
                                     "comment" : '', # Should probably use a new table or something for comment archives
                                     "previous" : metadata_dict["targetMetadata"][timestamps[idx+1]]
                                     })
            
        # Lastly, we need docsets
        metasheet["docSetId"] = []
        for docset in docsets:
            metasheet["docSetId"].append(docset[2])
        
        return metasheet
    
    def find(self, filters: dict=None, groups: list=None, page: int=0):
        if filters is None: filters = {}

        con = self._connect_sql()
        cur = con.cursor()

        docIds = None
        for tag in filters:
            if '.' in tag: # This is metadata
                val = filters[tag]
                mType, tag = tag.split('.')
                res = cur.execute(f"""
                              SELECT docID FROM Metadata WHERE key='{tag}' and val='{val}' and type='{mType}'
                              """)
            else:
                val = filters[tag]
                res = cur.execute(f"""
                                  SELECT docID from Metasheets WHERE {tag}='{val}'""")
            res = res.fetchall()
            filter_docIds = set()
            for doc in res: # Store docIds as a set, so they don't get added in multiple times
                filter_docIds.add(doc[0])
                
            # We ONLY want docIds that fit every single filter, so take a union as we go
            if docIds is None:
                docIds = filter_docIds
            else:
                docIds = docIds.union(filter_docIds)
                
        metasheets = []
        for docId in docIds:
            metasheets.append(self._get_metasheet(docId, con))
        return metasheets
    
    def notate(self, doc: dict) -> None:
        try:
            con = self._connect_sql()
            cur = con.cursor()
            
            # Add in the main document, with no metadata or docsets
            docID = doc["docId"]
            timestamp = time.time()
            displayName = doc["displayName"]
            targetClass = doc["targetClass"]
            siteClass = doc["siteClass"]
            status = doc["status"]
            cur.execute(f"""
                        INSERT INTO Metasheets VALUES
                        ('{docID}', {timestamp}, '{displayName}', '{targetClass}', '{siteClass}', {status})
                        """)
            
            # Add in metadata, compressing to a single insert. We should always have at least some, thanks to site/target
            metadataQuery = "INSERT INTO Metadata VALUES"
            metadataTypes = ['userMetadata', 'siteMetadata', 'targetMetadata']
            for mType in metadataTypes:
                for key in doc[mType]:
                    val = str(doc[mType][key]) # It should be a string, but double check
                    metadataQuery += f" ('{docID}', {timestamp}, '{key}', '{val}', '{mType}'),"
            metadataQuery = metadataQuery[:-1]
            cur.execute(metadataQuery)

            # Add in docsets, compressing to a single insert
            if doc["docSetId"]:
               docSetQuery = "INSERT INTO DocSets VALUES"
               for docSet in doc["docSetId"]:
                   docSetQuery += f" ('{docID}', {timestamp}, '{docSet}'),"
               docSetQuery = docSetQuery[:-1] # Cut off trailing comma
               cur.execute(docSetQuery)
            con.commit()
            
        except Exception as ex:
            print(f"Notate failed: {ex}")
            raise HTTPException(status_code=500,
                                detail=f"Notate failed: {ex}")

    def update(self, doc_id: str, update_fields: dict) -> None:
        try:
            con = self._connect_sql()
            metasheet = self._get_metasheet(doc_id, con)
            for key in update_fields:
                metasheet[key] = update_fields[key]
            self.notate(metasheet)
        except Exception as ex:
            print(f"Update failed: {ex}")
            raise HTTPException(status_code=500,
                                detail=f"Update failed: {ex}")
