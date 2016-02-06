SET DEFAULT_PARALLEL 10;
A = LOAD '/od_input_bz2/';
B = FOREACH A GENERATE flatten(TOKENIZE($0)) as airport;
C = GROUP  B BY airport;
D = FOREACH  C GENERATE COUNT(B), group;
E = ORDER D by $0 DESC;
F = LIMIT E 10;
store F into '/od_output/quest1.1';
