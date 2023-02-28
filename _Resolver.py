"""The resolver provides two external functions--getMetaSite and getMetaTarget
Given the name of a Site or Target type, provide a corresponding class
These modules are in a separate module so that a user can easily subsitute their own"""

import configparser
import importlib

from fastapi import HTTPException

config = configparser.ConfigParser()
config.read('repo.conf')

def _get_meta(meta_type_cls, meta_type_str, name):
    """ Perform a dynamic import from one of our subdirectories """
    try:
        meta_module = importlib.import_module(f"{meta_type_cls}.{name}")
        meta_class = getattr(meta_module, name)
    except Exception as exc:
        print(exc)
        raise HTTPException(status_code=400,
                            detail=f"Nonexistent {meta_type_str} type: {name}") from exc
    return meta_class

def get_meta_site(metadata):
    """ Dynamically import a site given a string name """
    name = metadata.siteClass
    meta_class = _get_meta('MetaSites', 'site', name)
    return meta_class

def get_meta_target(metadata):
    """ Dynamically import a target given a string name """
    name = metadata.targetClass
    meta_class = _get_meta('MetaTargets', 'target', name)
    return meta_class


def get_repo():
    """ Dynamically get the appropriate repo """
    db_type = config.get("BASE", "repotype", fallback=None)
    meta_class = _get_meta('Repository', 'repo', db_type)
    return meta_class
