-- Question 2.3
-- Considers as delayed => 15 min. delayed
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
B = FOREACH A GENERATE origin, destination, carrier, arrdelayed;
-- Groups all entries by origin-destination-carrier to calculate average delay by carrier
C = GROUP B BY (origin, destination, carrier);
D = FOREACH C GENERATE group.origin, group.destination, group.carrier, AVG(B.arrdelayed) * 100 AS avgdelayed;
-- group by origin-destination to sort the carriers
E = GROUP D BY (origin, destination);
-- E: {group: (origin: chararray,destination: chararray),D: {(origin: chararray,destination: chararray,carrier: chararray,avgdelayed: double)}}
-- For each route sorts the carrier in the bag
F = FOREACH E {
    F1 = ORDER D BY avgdelayed;
    F2 = LIMIT F1 10;
    GENERATE group, F2;
} ;
-- F: {group: (origin: chararray,destination: chararray),F2: {(origin: chararray,destination: chararray,carrier: chararray,avgdelayed: double)}}
-- Generates the tuples to store in cassandra
G = FOREACH F GENERATE TOTUPLE(TOTUPLE('origin',group.origin),TOTUPLE('destination', group.destination)),TOTUPLE(BagToString(F2.carrier, ','));
STORE G INTO 'cql://capstone/xytop10arrival?output_query=UPDATE+xytop10arrival+SET+least_delayed+%3D+%3F' USING CqlNativeStorage();
