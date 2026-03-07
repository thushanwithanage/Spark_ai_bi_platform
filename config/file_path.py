import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SEMANTIC_LAYER_DIR = os.path.join(BASE_DIR, "semantic_layer")
DATA_PATH = os.path.join(BASE_DIR, "data", "gold")
ERROR_MSGS_PATH = os.path.join(BASE_DIR, "error_msgs.json")