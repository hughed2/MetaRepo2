import requests
import sys

def exportValidation(metasheet):
    # This is meant to be overridden by users
    # For example, if you want to edit an s3 url for each metasheet, or change the target type
    # By default, do nothing
    return metasheet

def importExport(import_url, import_token, export_url, export_token, validation=exportValidation):
    # We need URLs and tokens for two separate Metarepos
    # Then, use the admin endpoints to get each metasheet from the import Metarepo, and add to the export Metarepo
    if import_url[-1] == '/': import_url = import_url[:-1] # Strip possible trailing slashes
    if export_url[-1] == '/': export_url = export_url[:-1]
    
    
    finished = False
    page = 0
    while not finished:
        # This should return a list of up to 1000 metasheets
        res_im = requests.get("%s/metarepo/admin/list" % (import_url),
                          headers={"Authorization" : "Bearer %s" % import_token},
                          data={"page" : page})
        
        # We should get a list of metasheets--check the status code, process the sheets, then possible continue
        if res_im.status_code != 200:
            sys.exit("Recieved status code %d from import with message: %s" % (res_im.status_code, res_im.text))
        
        metasheets = res_im.json()
        for metasheet in metasheets:
            metasheet = exportValidation(metasheet)
            
            res_ex = requests.post("%s/metarepo/admin/forceNotate" % (export_url),
                              headers={"Authorization" : "Bearer %s" % export_token},
                              data={"metasheet" : metasheet})
            
            if res_ex.status_code != 200:
                sys.exit("Recieved status code %d from export with message: %s" % (res_ex.status_code, res_ex.text))
        
        if len(metasheets) < 1000 : finished = True


if __name__ == "__main__":
    if sys.argv != 5:
        sys.exit("Usage: ImportExport.py <import_url> <import_token> <export_url> <export_token>")

    import_url = sys.argv[1]
    import_token = sys.argv[2]
    export_url = sys.argv[3]
    export_token = sys.argv[4]
    importExport(import_url, import_token, export_url, export_token)