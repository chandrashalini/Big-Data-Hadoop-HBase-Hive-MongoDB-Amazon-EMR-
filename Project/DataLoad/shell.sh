echo Downloding file
max = 2008
for (( i=1987; i <= 2008; ++i ))
do
    curl -o /Users/ajaygoel/Documents/Neu/BigData/Project/Data/$i.csv.bz2 "http://stat-computing.org/dataexpo/2009/$i.csv.bz2"
done