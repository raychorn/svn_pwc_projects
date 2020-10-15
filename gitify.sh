#!/bin/bash

git init
find * -size +4M -type f -print >> .gitignore
git add -A
git commit -m "first commit"
git branch -M main
git remote add origin https://raychorn:53457c9bd48ffe004ca09cbcebe9502fd6f9fec6@github.com/raychorn/svn_pwc_projects.git
git push -u origin main
