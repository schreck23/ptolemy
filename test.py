#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 20:13:48 2023

@author: wfschrec
"""

import os

def split_file(filename, chunk_size=1024*1024*1024):
    with open(filename, 'rb') as infile:
        index = 1
        while True:
            chunk = infile.read(chunk_size)
            if not chunk:
                break
            #with open(f"{filename}.part{index}", 'wb') as outfile:
            #    outfile.write(chunk)
            print("%s.part%i" % (filename, index))
            print(len(chunk))
            index += 1

if __name__ == '__main__':
    split_file("/data/raw/large/file1")
