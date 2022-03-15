flightData = LOAD '/project/data/1987.csv' USING PigStorage(',') AS (
Year:int,
Month:int,
DayofMonth:int,
DayOfWeek:int,
DepTime :int,
CRSDepTime:int,
ArrTime:int,
CRSArrTime:int,
UniqueCarrier:chararray,
FlightNum:int,
TailNum:chararray,
ActualElapsedTime:int,
CRSElapsedTime:int,
AirTime:int,
ArrDelay:int,
DepDelay:int,
Origin:chararray,
Dest:chararray,
Distance:int,
TaxiIn:int,
TaxiOut:int,
Cancelled:int,
CancellationCode :chararray,
Diverted:chararray,
CarrierDelay:int,
WeatherDelay:int,
NASDelay:int,
SecurityDelay:int,
LateAircraftDelay:int);

carrierData = LOAD '/project/data2/carriers.csv' USING PigStorage(',') AS
(code:chararray, description:chararray);


airportData = LOAD '/project/data2/airports.csv' USING PigStorage(',') AS
(Iata:chararray,airport:chararray, city:chararray, state:chararray,
country:chararray,lat:chararray, longi:chararray);
