SET DEFAULT_PARALLEL 10;
A = LOAD '/ontime_input_bz2/' USING PigStorage(' ') AS (date:chararray,
                                                        dweek:chararray,
                                                        carrier:chararray, mcarrier:chararray,
                                                        flight:chararray,
                                                        origin:chararray,
                                                        destination:chararray, deptime:chararray,
                                                        depdelayed:int, arrtime:chararray,
                                                        arrdelaymin:int, arrdelayed:int);
B = FOREACH A GENERATE carrier, arrdelayed;
C = GROUP B BY carrier;
D = FOREACH C GENERATE group, AVG(B.arrdelayed) * 100 AS avgdelayed;
E = ORDER D BY avgdelayed;
F = LIMIT E 10;
store F into '/on_output/quest1.2';