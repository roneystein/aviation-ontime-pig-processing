REGISTER /opt/pig/lib/piggybank.jar;
define CqlNativeStorage org.apache.cassandra.hadoop.pig.CqlNativeStorage();
SET DEFAULT_PARALLEL 20;
A = LOAD '/on_input_bz2/On_Time_On_Time_Performance_2008_*' USING PigStorage(' ') AS (date:chararray,
                                                               dweek:chararray,
                                                               carrier:chararray, mcarrier:chararray,
                                                               flight:chararray,
                                                               origin:chararray,
                                                               destination:chararray, deptime:chararray,
                                                               depdelayed:int, arrtime:chararray,
                                                               arrdelaymin:int, arrdelayed:int);
A1 = FOREACH A GENERATE ToDate(date,'yyyy-MM-dd') AS date, CONCAT(mcarrier, ' ', flight) AS mflight,
     origin, destination, deptime, arrdelaymin, AddDuration(ToDate(date,'yyyy-MM-dd'), 'P2D') as nextdate;
-- First leg departure before 12:00h, only least delayed flight is selected
X1 = FILTER A1 BY (int)deptime < 1200;
-- X1: {date: datetime,mflight: chararray,origin: chararray,destination: chararray,deptime: chararray,arrdelaymin: int}
X2 = GROUP X1 BY (date, origin, destination);
-- X2: {group: (date: datetime,origin: chararray,destination: chararray),
--      X1: {(date: datetime,mflight: chararray,origin: chararray,destination: chararray,deptime: chararray,arrdelaymin: int)}}
-- For each day, origin and destination we pick the most on-time flight
X3 = FOREACH X2 {
    XF2 = ORDER X1 BY arrdelaymin;
    XF3 = LIMIT XF2 1;
    GENERATE FLATTEN (XF3); } ;
-- X3: {XF3::date: datetime,XF3::origin: chararray,XF3::destination: chararray,XF3::mflight: chararray,XF3::deptime: chararray,XF3::arrdelaymin: int}

-- Second leg departure after 12:00h, only least delayed flight is selected
Y1 = FILTER A1 BY (int)deptime > 1200;
Y2 = GROUP Y1 BY (date, origin, destination);
Y3 = FOREACH Y2 {
    YF2 = ORDER Y1 BY arrdelaymin;
    YF3 = LIMIT YF2 1;
    GENERATE FLATTEN (YF3); } ;
-- Y3: {YF3::date: datetime,YF3::origin: chararray,YF3::destination: chararray,YF3::mflight: chararray,YF3::deptime: chararray,YF3::arrdelaymin: int}

X4 = ORDER X3 BY XF3::nextdate, XF3::destination;
Y4 = ORDER Y3 BY YF3::date, YF3::origin;

J1 = JOIN X4 BY (XF3::nextdate, XF3::destination), Y4 BY (YF3::date, YF3::origin) USING 'merge';
-- J1: {X3::XF3::date: datetime,X3::XF3::mflight: chararray,X3::XF3::origin: chararray,X3::XF3::destination: chararray,X3::XF3::deptime: chararray,X3::XF3::arrdelaymin: int,X3::XF3::nextdate: datetime,
--      Y3::YF3::date: datetime,Y3::YF3::mflight: chararray,Y3::YF3::origin: chararray,Y3::YF3::destination: chararray,Y3::YF3::deptime: chararray,Y3::YF3::arrdelaymin: int,Y3::YF3::nextdate: datetime}

S1 = FOREACH J1 GENERATE TOTUPLE(TOTUPLE('xdepdate',ToString(X3::XF3::date,'dd/MM/yyyy')),TOTUPLE('origin',X3::XF3::origin),TOTUPLE('intermediate',Y3::YF3::origin),TOTUPLE('destination',Y3::YF3::destination)),
                         TOTUPLE(X3::XF3::deptime, X3::XF3::mflight, ToString(Y3::YF3::date,'dd/MM/yyyy'), Y3::YF3::deptime, Y3::YF3::mflight, (X3::XF3::arrdelaymin + Y3::YF3::arrdelaymin));

STORE S1 INTO 'cql://capstone/xyz?output_query=UPDATE+xyz+SET+xdeptime+%3D+%3F+%2C+xflight+%3D+%3F+%2C+ydepdate+%3D+%3F+%2C+ydeptime+%3D+%3F+%2C+yflight+%3D+%3F+%2C+delay+%3D+%3F' USING CqlNativeStorage();