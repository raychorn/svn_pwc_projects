"""A patched version of the entry-point file to the IBM DB2 module.

This file is needed because the default version provided by IBM makes
assumptions about where the IBM DB2 CLI Driver is on the local system.
These assumptions are not true when running the standalone executable
on a client machine.

To fix this, we use environment variables to point to the actual
location of the files, and raise an ImportError if that environment
variable is not found.
"""

import os

if 'PWC_IBM_DLL' not in os.environ:
    raise ImportError('Environment variable "PWC_IBM_DLL" must be set '
                      'in order to import this module.')

if 'clidriver' not in os.environ['PATH']:
    os.environ['PATH'] = os.environ['PATH'] + ";" + os.path.join(os.environ['PWC_IBM_DLL'], 'clidriver', 'bin')

def __bootstrap__():
    global __bootstrap__, __loader__, __file__
    import sys, pkg_resources, imp

    __file__ = os.path.join(os.environ['PWC_IBM_DLL'], 'ibm_db_dlls', 'ibm_db.dll')

    __loader__ = None; del __bootstrap__, __loader__
    imp.load_dynamic(__name__, __file__)

__bootstrap__()
