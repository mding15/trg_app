# -*- coding: utf-8 -*-
"""
Created on Thu May 23 21:18:54 2024

@author: mgdin
"""
import datetime

from trg_config import config

config['DATA_DIR']


def my_decorator(func):
    def wrapper(*args, **kwargs):
        value = 5
        print(f"Something is happening before the function is called with value: {value}.")
        func(*args, value, **kwargs)
        print("Something is happening after the function is called.")
    return wrapper

@my_decorator
def say_hello(value):
    print(f"Hello! value: {value}")

# Calling the function
say_hello()

def repeat(num_times):
    def decorator_repeat(func):
        def wrapper(*args, **kwargs):
            for _ in range(num_times):
                func(*args, **kwargs)
        return wrapper
    return decorator_repeat

@repeat(num_times=3)
def greet(name):
    print(f"Hello, {name}!")

greet("Alice")


import pandas as pd
import numpy as np

# Sample DataFrame
df = pd.DataFrame({
    'A': [1, 2, np.nan, 4],
    'B': [np.nan, 2, 3, 4],
    'C': [1, np.nan, np.nan, 4]
})

# Replace NaN with None
df = df.applymap(lambda x: None if pd.isna(x) else x)
df = df.replace({np.nan: None})

print(df)
