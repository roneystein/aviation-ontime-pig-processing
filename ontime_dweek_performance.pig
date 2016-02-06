SET DEFAULT_PARALLEL 10;
A = LOAD '/on_input_bz2/' USING PigStorage(' ') AS (date:chararray,
                                                    dweek:chararray,
                                                    carrier:chararray, mcarrier:chararray,
                                                    flight:chararray,
                                                    origin:chararray,
                                                    destination:chararray, deptime:chararray,
                                                    depdelayed:int, arrtime:chararray,
                                                    arrdelaymin:int, arrdelayed:int);
B = FOREACH A GENERATE dweek, arrdelayed;
C = GROUP B BY dweek;
D = FOREACH C GENERATE group, AVG(B.arrdelayed) * 100 AS avgdelayed;
E = ORDER D BY avgdelayed;
store E into '/on_output/quest1.3';