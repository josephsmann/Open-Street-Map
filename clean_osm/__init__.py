# the existance of this __init__.py file makes this directory into a package
from . cl import *
#  __all__ will import the modules in the list from the command
# from clean_osm import *
# but you still need to call them
#  by name eg. cl.get_element
__all__ = ['cl']

# from .cl import *
# "." necessary to find cl
