#!/bin/bash

curl -X POST http://$USERNAME:$JENKINS_API_TOKEN@cd-jenkins.pwcinternal.com/job/EIT/job/EIT-PyExtract/buildWithParameters\
    -H "Jenkins-Crumb:$JENKINS_CRUMB"\
    --form SourceRepo='matlkatp2app025:8080/tfs/ECAP/PyTech/_git/pyextract'\
    --form GitBranch='develop'\
    --form BuildLabel='PYTHON35'\
    --form LicenseKey=''
