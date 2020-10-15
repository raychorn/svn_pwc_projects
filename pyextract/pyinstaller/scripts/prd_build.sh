#!/bin/bash

rm -f pyextract/config.py
cp configs/prd_config.py pyextract/config.py
rm -r build
rm -r dist
pyinstaller build.spec
