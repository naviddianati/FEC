#! /bin/bash

epydoc -c style.css --parse-only -o scripts/ ../src/*.py
epydoc -c style.css --parse-only -o disambiguation/ ../src/disambiguation
