#! /bin/bash

#epydoc -v -c style.css --parse-only -o scripts/ ../src/*.py
epydoc --no-frames -v -c style.css --parse-only -o FEC-disambiguation/ ../src/disambiguation ../src/*.py
