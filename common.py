import os

def conditionalMkdir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
