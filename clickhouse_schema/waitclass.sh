DATABASE=oradb
USERNAME=oracle
PASSWORD=elcaro
DBHOST=
TABLENAME=waitclass

#echo -ne "CREATE TABLE IF NOT EXISTS ${DATABASE}.${TABLENAME} ( classnum UInt8, class String )  ENGINE = Memory;" | clickhouse-client  -h $DBHOST -u $USERNAME --password $PASSWORD -d $DATABASE

cat << EOF | clickhouse-client  -h $DBHOST -u $USERNAME --password $PASSWORD -d $DATABASE --query="INSERT INTO ${DATABASE}.${TABLENAME} FORMAT TSV";
127	CPU
0	Other
1	Application
2	Configuration
3	Administrative
4	Concurrency
5	Commit
6	Idle
7	Network
8	User I/O
9	System I/O
10	Scheduler
12	Queueing
EOF

echo -ne "CREATE VIEW default.waitclass ( classnum UInt8,  class String) AS SELECT classnum, class FROM oradb.waitclass;" | clickhouse-client  -h $DBHOST -u $USERNAME --password $PASSWORD -d $DATABASE
