#! /bin/bash

# Load configurations
source config.sh



# Download all individual campaign contribution datases

cd $URL_DLS


for year in 1980 1982 1984 1986 1988 1990 1992 1994 1996 1998 2000 2002 20004 2006 2008 2010 2012 2014 2016
do
    two_digits=`echo $year | sed -r 's/[0-9]{2}([0-9]{2})/\1/'`
    echo $year
    file="indiv$two_digits.zip"
    #file="oppexp$two_digits.zip"
    # if year's data doesn't exist, download it
    if [ ! -f "$file" ]
    then
        wget "ftp://ftp.fec.gov/FEC/$year/$file" &
    fi        
done 



# Unzip the data files. Output is files like indiv04.txt
for file in indiv*.zip
do
    filename=`basename $file .zip`
    extractedfile="$filename.txt"
    if [ ! -f "$extractedfile" ]
    then
        unzip $file -d $URL_TMP 
        mv $URL_TMP/itcont.txt "$URL_DATA/$filename.txt"
    fi
done

cd $URL_WORKING


exit




