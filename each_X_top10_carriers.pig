-- Question 2.1 : same as question 2.2 but using airports
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
B = FOREACH A GENERATE origin, carrier, depdelayed;
C = GROUP B BY (origin, carrier);
-- C: {group: (origin: chararray,carrier: chararray),B: {(origin: chararray,carrier: chararray,depdelayed: int)}}
D = FOREACH C GENERATE group.origin, group.carrier , AVG(B.depdelayed) * 100 as depdelayed;
-- D: {origin: chararray,carrier: chararray,double}
E = GROUP D BY origin;
--E: {group: chararray,D: {(origin: chararray,carrier: chararray,double)}}
F = FOREACH E {
    F1 = ORDER D BY depdelayed;
    F2 = LIMIT F1 10;
    GENERATE TOTUPLE(TOTUPLE('origin', group)), TOTUPLE(BagToString(F2.carrier, ',')) ;
} ;
STORE F INTO 'cql://capstone/top10carriers?output_query=UPDATE+top10carriers+SET+least_delayed+%3D+%3F' USING CqlNativeStorage();