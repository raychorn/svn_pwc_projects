#!/bin/bash

rm -f pyextract/config.py
cp configs/qa_config.py pyextract/config.py
rm -r build
rm -r dist
pyinstaller build.spec
