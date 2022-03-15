http://stat-computing.org/dataexpo/2009/1987.csv.bz2

echo Downloding file
curl -o /Users/ajaygoel/Documents/Neu/BigData/Project "http://msis.neu.edu/nyse/nyse.zip"
if [ "$?" -gt 0 ]
then
echo "Error downloading file a. Exiting"
exit
fi
echo "Unzipping the file"
unzip nyse_zip.zip 
if [ "$?" -gt 0 ]
then
echo "Error unzipping file"
exit
fi
echo "Unzipping the file"

for filename in `ls NYSE | grep "NYSE_daily_prices_\([A-Z]\).csv"` ;
do
#	echo $filename;
mongoimport -d bigdata_2 -c nyse --type csv --file NYSE/$filename --headerline
        if [ $? -eq 0 ]; then
        	echo "$filename imported successfully!!!!"
        else
        	echo "Error while importing $filename!!!"
        	exit 1
        fi
done
echo "All files imported sucessfully!!


chars=( {a..z} )
i=0
for filename in `hadoop fs -ls /Assignment3/NYSE2 | awk '{print $NF}' | grep .csv$ | tr '\n' ' '`
do 
	echo $filename;
	hadoop jar /Users/ajaygoel/.m2/repository/com/ajay/mr/Assignment3_6/1.0-SNAPSHOT/Assignment3_6-1.0-SNAPSHOT.jar com.ajay.mr.assignment3_6.NYSEMR $filename /Assignment3_6/"${chars[i++]}"
done



------------------------------------------------------------------------------------------------------------
echo Downloding file
max = 2008
for (( i=1987; i <= 2008; ++i ))
do
    curl -o /Users/ajaygoel/Documents/Neu/BigData/Project/Data/$i.csv.bz2 "http://stat-computing.org/dataexpo/2009/$i.csv.bz2"
done

echo "Unzipping the file"
for (( i=1987; i <= 2008; ++i ))
do
    bzip2 -d /Users/ajaygoel/Documents/Neu/BigData/Project/Data/$i.csv.bz2
done
hadoop fs -copyFromLocal /Users/ajaygoel/Documents/Neu/BigData/Project/Data /project/data
------------------------------------------------------------------------------------------------------------

fi
echo "Unzipping the file"
bzip2 -d /Users/ajaygoel/Documents/Neu/BigData/Project/Data/1987.csv.bz2
if [ "$?" -gt 0 ]
then
echo "Error unzipping file"
exit

bzip2 -d /Users/ajaygoel/Documents/Neu/BigData/Project/Data/1987.csv.bz2
