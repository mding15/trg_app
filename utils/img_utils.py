# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 11:06:05 2025

@author: mgdin

pip install pillow

"""

from PIL import Image
from trg_config import config

# convert png image to ico image
def convert_png_to_ico(input_png, output_ico):
    img = Image.open(input_png)
    img.save(output_ico, format='ICO', sizes=[(32, 32), (16, 16)])

# Example usage
def example_convert_png_to_ico():
    png_file = config['TEMPLATES_DIR'] / 'logo.png'
    ico_file = config['TEMPLATES_DIR'] / 'favicon.ico'
    convert_png_to_ico(png_file, ico_file)
    print("Conversion completed: favicon.ico created!")
