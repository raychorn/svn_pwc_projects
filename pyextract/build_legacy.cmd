@echo off

RMDIR /S /Q build
RMDIR /S /Q dist
pyinstaller build.spec
