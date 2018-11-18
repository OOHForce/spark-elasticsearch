# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 11:41:51 2018

@author: beersoccer
"""

from pkg_resources import get_distribution

# Import a few utility methods that we'll use later on.
from IPython.display import Image, HTML, display
from time import time

__all__ = ['Image', 
           'HTML', 
           'display', 
           'time', 
        ]

__version__ = get_distribution('recooh').version
