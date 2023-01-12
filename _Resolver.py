### The resolver provides two external functions--getMetaSite and getMetaTarget
### Given the name of a Site or Target type, provide a corresponding class
### These modules are in a separate module so that a user can easily subsitute their own

import importlib

from fastapi import HTTPException

def _getMeta(metaTypeCls, metaTypeStr, name):
    try:
        metaModule = importlib.import_module(("%s.%s") % (metaTypeCls, name))
        metaClass = getattr(metaModule, name)
    except:
        raise HTTPException(status_code=400, detail="Nonexistent %s type: %s" % (metaTypeStr, name))
    return metaClass

def getMetaSite(name):
    metaClass = _getMeta('MetaSites', 'site', name)
    return metaClass

def getMetaTarget(name):
    metaClass = _getMeta('MetaTargets', 'target', name)
    return metaClass
