import numpy as np

# Define a function to check if a string is a valid hexadecimal
def is_hex(x):
    try:
        # Try to convert the string to an integer
        int(x, 16)
        return True
    except ValueError:
        # If conversion fails, return False
        return False


