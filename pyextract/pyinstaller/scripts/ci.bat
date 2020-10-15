:: Batch file to run unit tests on the Continuous Integration server
CALL C:\Users\aroche009\Documents\extract-env.bat
python -m unittest tests.test_utils
