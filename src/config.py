import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import ConfigParser

config = ConfigParser() 
config.read('config.ini')
