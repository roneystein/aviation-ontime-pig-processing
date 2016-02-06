-- Question 2.4
-- as mean arrival delay only positive delays will be used
-- delayed flight considered as any delay > 0 minutes
define CqlNativeStorage org.apache.cassandra.hadoop.pig.CqlNativeStorage();
SET DEFAULT_PARALLEL 10;
A = LOAD '/on_input_bz2/' USING PigStorage(' ') AS (date:chararray,
                                                               dweek:chararray,
                                                               carrier:chararray, mcarrier:chararray,
                                                               flight:chararray,
                                                               origin:chararray,
                                                               destination:chararray, deptime:chararray,
                                                               depdelayed:int, arrtime:chararray,
                                                               arrdelaymin:int, arrdelayed:int);
-- no need to filter when using the ArrDelayMinutes field (negative are zeroed out)
A = FOREACH A GENERATE origin, destination, arrdelaymin;
B = GROUP A BY (origin, destination);
-- B: {group: (origin: chararray,destination: chararray),A: {(origin: chararray,destination: chararray,arrdelaymin: int)}}
C = FOREACH B GENERATE TOTUPLE(TOTUPLE('origin',group.origin),TOTUPLE('destination',group.destination)),TOTUPLE((int) AVG(A.arrdelaymin));
STORE C INTO 'cql://capstone/xymeandelay?output_query=UPDATE+xymeandelay+SET+mean_delay+%3D+%3F' USING CqlNativeStorage();
