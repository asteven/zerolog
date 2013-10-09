# python 2 <-> 3 backwards compatibility helpers

import os

# python2 back-compat
def makedirs(path, mode=0o777, exist_ok=False):
    try:
        os.makedirs(path, mode=mode, exist_ok=exist_ok)
    except TypeError:
        try:
            os.makedirs(path, mode=mode)
        except OSError as e:
            if exist_ok and e.errno == 17: # File exists
                pass
            else:
                raise
