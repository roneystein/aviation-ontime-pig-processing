-- Question 2.2
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
B = FOREACH A GENERATE origin, destination, depdelayed;
-- group by origin-destination pairs
C = GROUP B BY (origin, destination);
-- for each pair calculate the average of departures delayed > 15 minutes
D = FOREACH C GENERATE group.origin, group.destination , AVG(B.depdelayed) * 100 as depdelayed;
-- for each originating airport generate the tuples containing
-- origin - detination - average departure delay
-- ranked by least delayed first
-- just the first 10 entries
E = GROUP D BY origin;
-- E: {group: chararray,D: {(origin: chararray,destination: chararray,double)}}
F = FOREACH E {
    F1 = ORDER D BY depdelayed;
    F2 = LIMIT F1 10;
    GENERATE TOTUPLE(TOTUPLE('origin', group)), TOTUPLE(BagToString(F2.destination, ',')) ;
} ;
-- F: {org.apache.pig.builtin.totuple_org.apache.pig.builtin.totuple_group_34_35: 
--      (org.apache.pig.builtin.totuple_group_34: 
--         (chararray,group: chararray)),org.apache.pig.builtin.totuple_36: (chararray)}
STORE F INTO 'cql://capstone/top10air?output_query=UPDATE+top10air+SET+least_delayed+%3D+%3F' USING CqlNativeStorage();