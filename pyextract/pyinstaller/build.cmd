@echo off

REM rm -f pyextract/config.py
REM cp configs/prd_config.py pyextract/config.py
REM rm -r build
REM rm -r dist
pyinstaller build.spec
