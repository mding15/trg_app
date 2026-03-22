import os
import sys

# add trg_app root to path so process2.* imports resolve
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
