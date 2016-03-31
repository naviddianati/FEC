#! /bin/bash

# Load configurations
source config.sh 




cd $URL_WORKING/FEC-raw-reports

# Download .zip files. Don't dl existing files
wget  -N ftp://ftp.fec.gov/FEC/electronic/*.zip

# unzip all zip files
for zipfile in *.zip
do
    unzip -o  $zipfile
done
cd $URL_WORKING
