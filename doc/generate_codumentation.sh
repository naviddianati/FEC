#! /bin/bash

epydoc -v -c style.css --parse-only -o scripts/ ../src/*.py
epydoc -v -c style.css --parse-only -o disambiguation/ ../src/disambiguation
