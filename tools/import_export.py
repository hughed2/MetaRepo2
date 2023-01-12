""" A script to import all of the metasheets from one MetaRepo, and export to another """
import sys
import requests

def export_validation(metasheet):
    """ This is meant to be overridden by users
    For example, if you want to edit an s3 url for each metasheet, or change the target type
    By default, do nothing """
    return metasheet

def import_export(import_url, import_token, export_url, export_token, validation=export_validation):
    """ Perform an import from one MetaRepo and export to another """
    if import_url[-1] == '/': # Strip possible trailing slashes
        import_url = import_url[:-1]
    if export_url[-1] == '/':
        export_url = export_url[:-1]


    finished = False
    page = 0
    while not finished:
        # This should return a list of up to 1000 metasheets
        res_im = requests.get(f"{import_url}/metarepo/admin/list",
                          headers={"Authorization" : f"Bearer {import_token}"},
                          data={"page" : page},
                          timeout=10)

        # We should get a list of metasheets
        # check the status code, process the sheets, then possibly continue
        if res_im.status_code != 200:
            sys.exit(f"Recieved status code {res_im.status_code} from import with message: "
                     f"{res_im.text}")

        metasheets = res_im.json()
        for metasheet in metasheets:
            metasheet =  validation(metasheet)

            res_ex = requests.post(f"{export_url}/metarepo/admin/forceNotate",
                              headers={"Authorization" : f"Bearer {export_token}"},
                              data={"metasheet" : metasheet},
                              timeout=10)

            if res_ex.status_code != 200:
                sys.exit(f"Recieved status code {res_ex.status_code} from export with message: "
                         f"{res_ex.text}")

        if len(metasheets) < 1000 :
            finished = True

def main():
    """ By default, we take in arguments from command line and pass them in to import_export """
    if sys.argv != 5:
        sys.exit("Usage: import_export.py <import_url> <import_token> <export_url> <export_token>")

    import_url = sys.argv[1]
    import_token = sys.argv[2]
    export_url = sys.argv[3]
    export_token = sys.argv[4]
    import_export(import_url, import_token, export_url, export_token)

if __name__ == "__main__":
    main()
