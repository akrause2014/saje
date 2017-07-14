import os.path

def get(*path_elems):
    """Like os.path.join, but always relative to the resource directory"""
    res_dir = os.path.dirname(__file__)
    pargs = (res_dir,) + path_elems
    return os.path.join(*pargs)

    
