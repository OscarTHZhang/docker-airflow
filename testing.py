"""
This file is here for testing any unclear libraries, APIs, and syntax of Python as this is the only Python file
in this project that can be run locally and has console output (not in Docker nor in Airflow web server). You
can freely use this file for any local testing.
"""

__author__ = "Oscar Zhang"
__email__ = "tzhang383@wisc.edu"
__version__ = '0.1'
__status__ = 'Development'

import os


def walking(directory):
    """
    Walking through the directory and retrieve sub-directory with dates
    :param directory: some directory with /test/larson/feedwatch
    :return: a list of absolute directories with dates
    """
    res = []
    for dirs in os.listdir(directory):
        res.append(os.path.join(directory, dirs))
    return res


a = walking('test/larson/feedwatch')
for i in range(len(a)):
    a[i] = os.path.basename(a[i])

print(a)