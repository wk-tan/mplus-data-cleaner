import json
import os

import yaml


def load_yaml(file):
    """import yaml config file"""
    with open(file) as f:
        output = yaml.load(f, Loader=yaml.FullLoader)
    return output
