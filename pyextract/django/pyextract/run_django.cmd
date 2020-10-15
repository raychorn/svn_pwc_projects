@echo off

REM Assumes you have activated the correct virtualenv before proceeding.

REM Adjust as-required for your environment.

echo %~dp0
echo %APPDATA%

SET PYTHONPATH=%~dp0..\..\pyextract;%~dp0..\..\pyextract\lib;%APPDATA%\Python\Python35\site-packages;

echo %PYTHONPATH%

REM python show_pythonpath.py

python manage.py runserver
