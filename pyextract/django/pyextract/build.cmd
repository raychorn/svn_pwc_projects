@echo off

RMDIR /S /Q build
RMDIR /S /Q dist
REM pyinstaller --name=pyextract manage.py
pyinstaller pyextract.spec

