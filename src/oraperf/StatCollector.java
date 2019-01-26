package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class StatCollector
        extends Thread {

    SL4JLogger lg;
    private final int SECONDSBETWEENSESSWAITSSNAPS = 10;
    private final int SECONDSBETWEENSESSSTATSSNAPS = 30;
    private final int SECONDSBETWEENSYSSTATSSNAPS = 120;
    private final int SECONDSBETWEENSQLSNAPS = 600;
    private final int RSSESSIONWAIT = 0;
    private final int RSSESSIONSTAT = 1;
    private final int RSSYSTEMSTAT = 2;
    private final int RSSQLSTAT = 3;
    private final int RSSEGMENTSTAT = 4;
    private final int RSSQLPHV = 5;
    private final int RSSQLTEXT = 6;
    private final int RSSTATNAME = 7;
    private final int RSIOFILESTAT = 8;
    private final int RSIOFUNCTIONSTAT = 9;
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private final int threadType;
    private final String dbUserName;
    private final String dbPassword;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private Connection con;
    private PreparedStatement oraWaitsPreparedStatement;
    private PreparedStatement oraSysStatsPreparedStatement;
    private PreparedStatement oraSesStatsPreparedStatement;
    private PreparedStatement oraSQLTextsPreparedStatement;
    private PreparedStatement oraSQLPlansPreparedStatement;
    private PreparedStatement oraSQLStatsPreparedStatement;
    private PreparedStatement oraStatNamesPreparedStatement;
    private PreparedStatement oraIOFileStatsPreparedStatement;
    private PreparedStatement oraIOFunctionStatsPreparedStatement;
    private long currentDateTime;
    private boolean shutdown = false;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private static final String ORASYSSTATSQUERY = "select statistic#,value from v$sysstat where value<>0";
    private static final String ORASESSWAITSQUERY = "select   sid,  serial#,  decode(taddr,null,'N','Y'),  status,  nvl(username,schemaname),  nvl(osuser,'-'),  nvl(machine,'-'),  nvl(program,'-'),  type,  nvl(module,'-'),  nvl(blocking_session,0),  decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event),   decode(state,'WAITED KNOWN TIME',127,'WAITED SHORT TIME',127,wait_class#),  round(wait_time_micro/1000000,3),  nvl(sql_id,'-'),  nvl(sql_exec_start,to_date('19700101','YYYYMMDD')),  sql_exec_id,  logon_time,  seq#,  nvl(p1,0),  nvl(p2,0)  from gv$session a  where sid<>sys_context('USERENV','SID') and (  wait_class#<>6 or exists  ( select 1 from gv$session b where a.inst_id=b.inst_id and (a.sid = b.blocking_session or a.sid = b.final_blocking_session) )  )";
    private static final String ORASESSTATSQUERY = "select sid,sserial,statistic#,value from (select sid,serial# sserial from v$session where type='USER' and sid<>sys_context('USERENV','SID') and ( wait_class#<>6 or (wait_class#=6 and seconds_in_wait < 10) )) join v$sesstat using(sid) join v$statname using(statistic#) where name in ( 'Requests to/from client','user commits','user rollbacks','user calls','recursive calls','recursive cpu usage','DB time','session pga memory','physical read total bytes','physical write total bytes','db block changes','redo size','redo size for direct writes','table fetch by rowid','table fetch continued row','lob reads','lob writes','index fetch by key','sql area evicted','session cursor cache hits','session cursor cache count','queries parallelized','Parallel operations not downgraded','Parallel operations downgraded to serial','parse time cpu','parse count (total)','parse count (hard)','parse count (failures)','sorts (memory)','sorts (disk)' )  and value<>0";
    private static final String ORASQLTEXTSQUERY = "select sql_id,sql_text from v$sqlarea";
    private static final String ORASQLPLANSQUERY = "select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0";
    private static final String ORASQLSTATSQUERY = "";
    private static final String ORASTATNAMESQUERY = "select statistic#,name from v$statname";
    private static final String ORAIOFILESTATSQUERY = "select filetype_name,coalesce(b.name,c.name,'-'),small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,small_sync_read_reqs,small_read_servicetime,small_write_servicetime,small_sync_read_latency,large_read_servicetime,large_write_servicetime from V$IOSTAT_FILE a left join v$datafile b on (b.file#=a.file_no and a.filetype_id=2) left join v$tempfile c on (c.file#=a.file_no and a.filetype_id=6)";
    private static final String ORAIOFUNCTIONSTATSQUERY = "select function_name,filetype_name,small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,number_of_waits,wait_time from V$IOSTAT_FUNCTION_DETAIL";

    public StatCollector(String inputString, String dbUSN, String dbPWD, ComboPooledDataSource ckhDS, int runTType, BlockingQueue<OraCkhMsg> queue) {
        this.dbConnectionString = inputString;
        this.dbUniqueName = inputString.split("/")[1];
        this.dbHostName = inputString.split(":")[0];
        this.dbUserName = dbUSN;
        this.dbPassword = dbPWD;
        this.threadType = runTType;
        this.ckhQueue = queue;
    }

    private List getSessionWaitsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while ((rs != null) && (rs.next())) {
                List rowList = new ArrayList();
                rowList.add(Integer.valueOf(rs.getInt(1)));
                rowList.add(Integer.valueOf(rs.getInt(2)));
                rowList.add(rs.getString(3));
                rowList.add(rs.getString(4).substring(0, 1));
                rowList.add(rs.getString(5));
                rowList.add(rs.getString(6));
                rowList.add(rs.getString(7));
                rowList.add(rs.getString(8));
                rowList.add(rs.getString(9).substring(0, 1));
                rowList.add(rs.getString(10));
                rowList.add(Integer.valueOf(rs.getInt(11)));
                rowList.add(rs.getString(12));
                rowList.add(Long.valueOf(rs.getLong(13)));
                rowList.add(Float.valueOf(rs.getFloat(14)));
                rowList.add(rs.getString(15));
                rowList.add(Long.valueOf(rs.getTimestamp(16).getTime() / 1000L));
                rowList.add(Integer.valueOf(rs.getInt(17)));
                rowList.add(Long.valueOf(rs.getTimestamp(18).getTime() / 1000L));
                rowList.add(Integer.valueOf(rs.getInt(19)));
                rowList.add((long) new BigDecimal(rs.getDouble(20)).setScale(0, RoundingMode.HALF_UP).doubleValue() );
        rowList.add(Long.valueOf(rs.getLong(21)));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting data from resultset " + this.dbConnectionString);
        }
        return outList;
    }

    private List getStatNamesListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while ((rs != null) && (rs.next())) {
                List rowList = new ArrayList();
                rowList.add(Integer.valueOf(rs.getInt(1)));
                rowList.add(rs.getString(2));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting data from resultset " + this.dbConnectionString);
        }
        return outList;
    }

    private List getSQlTextsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while ((rs != null) && (rs.next())) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getString(2));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting data from resultset " + this.dbConnectionString);
        }
        return outList;
    }

    private List getSQlPlansListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while ((rs != null) && (rs.next())) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(Long.valueOf(rs.getLong(2)));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting data from resultset " + this.dbConnectionString);
        }
        return outList;
    }

    private List getSysStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while ((rs != null) && (rs.next())) {
                List rowList = new ArrayList();
                rowList.add(Integer.valueOf(rs.getInt(1)));
                rowList.add((long) new BigDecimal(rs.getDouble(2)).setScale(0, RoundingMode.HALF_UP).doubleValue());
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting data from resultset " + this.dbConnectionString);
        }
        return outList;
    }

    private List getSesStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while ((rs != null) && (rs.next())) {
                List rowList = new ArrayList();
                rowList.add(Integer.valueOf(rs.getInt(1)));
                rowList.add(Integer.valueOf(rs.getInt(2)));
                rowList.add(Integer.valueOf(rs.getInt(3)));
                rowList.add((long) new BigDecimal(rs.getDouble(4)).setScale(0, RoundingMode.HALF_UP).doubleValue());
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting data from resultset " + this.dbConnectionString);
        }
        return outList;
    }

    private List getIOFileStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while ((rs != null) && (rs.next())) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getString(2));
                rowList.add(new BigDecimal(rs.getDouble(3)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(4)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(5)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(6)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(7)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(8)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(9)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(10)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(11)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(12)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(13)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(14)).longValue());
                rowList.add(new BigDecimal(rs.getDouble(15)).longValue());
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(16)).longValue()));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting data from resultset " + this.dbConnectionString);
        }
        return outList;
    }

    private List getIOFunctionStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while ((rs != null) && (rs.next())) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getString(2));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(3)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(4)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(5)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(6)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(7)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(8)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(9)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(10)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(11)).longValue()));
                rowList.add(Long.valueOf(new BigDecimal(rs.getDouble(12)).longValue()));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting data from resultset " + this.dbConnectionString);
        }
        return outList;
    }

    private void setDateTimeVars() {
        this.currentDateTime = Instant.now().getEpochSecond();
    }

    private void runSessAndIOStatsRoutines()
            throws InterruptedException {
        while (!this.shutdown) {
            setDateTimeVars();
            try {
                this.oraIOFileStatsPreparedStatement.execute();
                this.ckhQueue.put(new OraCkhMsg(8, this.currentDateTime, this.dbUniqueName, this.dbHostName,
                        getIOFileStatsListFromRS(this.oraIOFileStatsPreparedStatement.getResultSet())));

                this.oraIOFileStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting io file stats from database " + this.dbConnectionString);

                this.shutdown = true;
                e.printStackTrace();
            }
            if (!this.shutdown) {
                try {
                    this.oraIOFunctionStatsPreparedStatement.execute();
                    this.ckhQueue.put(new OraCkhMsg(9, this.currentDateTime, this.dbUniqueName, this.dbHostName,
                            getIOFunctionStatsListFromRS(this.oraIOFunctionStatsPreparedStatement.getResultSet())));

                    this.oraIOFunctionStatsPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting io function stats from database " + this.dbConnectionString);

                    this.shutdown = true;
                    e.printStackTrace();
                }
            }
            if (!this.shutdown) {
                try {
                    this.oraWaitsPreparedStatement.execute();
                    this.ckhQueue.put(new OraCkhMsg(0, this.currentDateTime, this.dbUniqueName, this.dbHostName,
                            getSessionWaitsListFromRS(this.oraWaitsPreparedStatement.getResultSet())));

                    this.oraWaitsPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tError getting sessions from database " + this.dbConnectionString);

                    this.shutdown = true;
                    e.printStackTrace();
                }
            }
            TimeUnit.SECONDS.sleep(10L);
        }
    }

    private void runSessStatsRoutines()
            throws InterruptedException {
        while (!this.shutdown) {
            try {
                setDateTimeVars();
                this.oraSesStatsPreparedStatement.execute();
                this.ckhQueue.put(new OraCkhMsg(1, this.currentDateTime, this.dbUniqueName, this.dbHostName,
                        getSesStatsListFromRS(this.oraSesStatsPreparedStatement.getResultSet())));

                this.oraSesStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbConnectionString + "\tError processing session statistics!");

                this.shutdown = true;
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(30L);
        }
    }

    private void runSysRoutines()
            throws InterruptedException {
        int snapcounter = 0;
        try {
            this.oraStatNamesPreparedStatement.execute();
            this.ckhQueue.put(new OraCkhMsg(7, 0L, null, null,
                    getStatNamesListFromRS(this.oraStatNamesPreparedStatement.getResultSet())));

            this.oraStatNamesPreparedStatement.close();
        } catch (Exception e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbConnectionString + "\tError processing statistics names!");

            this.shutdown = true;
        }
        while (!this.shutdown) {
            try {
                setDateTimeVars();
                this.oraSysStatsPreparedStatement.execute();
                this.ckhQueue.put(new OraCkhMsg(2, this.currentDateTime, this.dbUniqueName, this.dbHostName,
                        getSysStatsListFromRS(this.oraSysStatsPreparedStatement.getResultSet())));

                this.oraSysStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbConnectionString + "\tError processing system statistics!");

                this.shutdown = true;
                e.printStackTrace();
            }
            if (snapcounter == 30) {
                snapcounter = 0;
                long begints = System.currentTimeMillis();
                if (!this.shutdown) {
                    try {
                        this.oraSQLPlansPreparedStatement.execute();
                        this.ckhQueue.put(new OraCkhMsg(5, 0L, null, null,
                                getSQlPlansListFromRS(this.oraSQLPlansPreparedStatement.getResultSet())));

                        this.oraSQLPlansPreparedStatement.clearWarnings();
                    } catch (SQLException e) {
                        this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbConnectionString + "\tError processing sql plan hash values!");

                        this.shutdown = true;
                        e.printStackTrace();
                    }
                }
                if (!this.shutdown) {
                    try {
                        this.oraSQLTextsPreparedStatement.execute();
                        this.ckhQueue.put(new OraCkhMsg(6, 0L, null, null,
                                getSQlTextsListFromRS(this.oraSQLTextsPreparedStatement.getResultSet())));

                        this.oraSQLTextsPreparedStatement.clearWarnings();
                    } catch (SQLException e) {
                        this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbConnectionString + "\tError processing sql texts!");

                        this.shutdown = true;
                        e.printStackTrace();
                    }
                }
                long endts = System.currentTimeMillis();
                if (endts - begints < 120000L) {
                    TimeUnit.SECONDS.sleep(120 - (int) ((endts - begints) / 1000L));
                }
            } else {
                TimeUnit.SECONDS.sleep(120L);
                snapcounter++;
            }
        }
    }

    private void runSQLRoutines()
            throws InterruptedException {
        while (!this.shutdown) {
            try {
                setDateTimeVars();
                this.oraSQLStatsPreparedStatement.execute();
                this.ckhQueue.put(new OraCkhMsg(3, this.currentDateTime, this.dbUniqueName, this.dbHostName, null));

                this.oraSQLStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbConnectionString + "\tError processing SQL statistics!");

                this.shutdown = true;
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(600L);
        }
    }

    private void cleanup() {
        try {
            if ((this.oraWaitsPreparedStatement != null) && (!this.oraWaitsPreparedStatement.isClosed())) {
                this.oraWaitsPreparedStatement.close();
            }
            if ((this.oraIOFileStatsPreparedStatement != null) && (!this.oraIOFileStatsPreparedStatement.isClosed())) {
                this.oraIOFileStatsPreparedStatement.close();
            }
            if ((this.oraIOFunctionStatsPreparedStatement != null) && (!this.oraIOFunctionStatsPreparedStatement.isClosed())) {
                this.oraIOFunctionStatsPreparedStatement.close();
            }
            if ((this.oraSysStatsPreparedStatement != null) && (!this.oraSysStatsPreparedStatement.isClosed())) {
                this.oraSysStatsPreparedStatement.close();
            }
            if ((this.oraSesStatsPreparedStatement != null) && (!this.oraSesStatsPreparedStatement.isClosed())) {
                this.oraSesStatsPreparedStatement.close();
            }
            if ((this.oraSQLTextsPreparedStatement != null) && (!this.oraSQLTextsPreparedStatement.isClosed())) {
                this.oraSQLTextsPreparedStatement.close();
            }
            if ((this.oraSQLPlansPreparedStatement != null) && (!this.oraSQLPlansPreparedStatement.isClosed())) {
                this.oraSQLPlansPreparedStatement.close();
            }
            if ((this.oraSQLStatsPreparedStatement != null) && (!this.oraSQLStatsPreparedStatement.isClosed())) {
                this.oraSQLStatsPreparedStatement.close();
            }
            if ((this.con != null) && (!this.con.isClosed())) {
                this.con.close();
            }
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbConnectionString + "\tError durring ORADB resource cleanups!");

            e.printStackTrace();
        }
    }

    private void openConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            this.con = DriverManager.getConnection("jdbc:oracle:thin:@" + this.dbConnectionString, this.dbUserName, this.dbPassword);
            this.con.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tCannot load Oracle driver!");
            this.shutdown = true;
        } catch (SQLException e) {
            this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tCannot initiate connection to target Oracle database: " + this.dbConnectionString);

            this.shutdown = true;
        }
    }

    public void run() {
        this.lg = new SL4JLogger();

        openConnection();
        if (!this.shutdown) {
            try {
                switch (this.threadType) {
                    case 0:
                        this.oraWaitsPreparedStatement = this.con.prepareStatement("select   sid,  serial#,  decode(taddr,null,'N','Y'),  status,  nvl(username,schemaname),  nvl(osuser,'-'),  nvl(machine,'-'),  nvl(program,'-'),  type,  nvl(module,'-'),  nvl(blocking_session,0),  decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event),   decode(state,'WAITED KNOWN TIME',127,'WAITED SHORT TIME',127,wait_class#),  round(wait_time_micro/1000000,3),  nvl(sql_id,'-'),  nvl(sql_exec_start,to_date('19700101','YYYYMMDD')),  sql_exec_id,  logon_time,  seq#,  nvl(p1,0),  nvl(p2,0)  from gv$session a  where sid<>sys_context('USERENV','SID') and (  wait_class#<>6 or exists  ( select 1 from gv$session b where a.inst_id=b.inst_id and (a.sid = b.blocking_session or a.sid = b.final_blocking_session) )  )");
                        this.oraWaitsPreparedStatement.setFetchSize(1000);
                        this.oraIOFileStatsPreparedStatement = this.con.prepareStatement("select filetype_name,coalesce(b.name,c.name,'-'),small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,small_sync_read_reqs,small_read_servicetime,small_write_servicetime,small_sync_read_latency,large_read_servicetime,large_write_servicetime from V$IOSTAT_FILE a left join v$datafile b on (b.file#=a.file_no and a.filetype_id=2) left join v$tempfile c on (c.file#=a.file_no and a.filetype_id=6)");
                        this.oraIOFileStatsPreparedStatement.setFetchSize(100);
                        this.oraIOFunctionStatsPreparedStatement = this.con.prepareStatement("select function_name,filetype_name,small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,number_of_waits,wait_time from V$IOSTAT_FUNCTION_DETAIL");
                        this.oraIOFunctionStatsPreparedStatement.setFetchSize(100);
                        runSessAndIOStatsRoutines();
                        break;
                    case 1:
                        this.oraSesStatsPreparedStatement = this.con.prepareStatement("select sid,sserial,statistic#,value from (select sid,serial# sserial from v$session where type='USER' and sid<>sys_context('USERENV','SID') and ( wait_class#<>6 or (wait_class#=6 and seconds_in_wait < 10) )) join v$sesstat using(sid) join v$statname using(statistic#) where name in ( 'Requests to/from client','user commits','user rollbacks','user calls','recursive calls','recursive cpu usage','DB time','session pga memory','physical read total bytes','physical write total bytes','db block changes','redo size','redo size for direct writes','table fetch by rowid','table fetch continued row','lob reads','lob writes','index fetch by key','sql area evicted','session cursor cache hits','session cursor cache count','queries parallelized','Parallel operations not downgraded','Parallel operations downgraded to serial','parse time cpu','parse count (total)','parse count (hard)','parse count (failures)','sorts (memory)','sorts (disk)' )  and value<>0");
                        this.oraSesStatsPreparedStatement.setFetchSize(1000);
                        runSessStatsRoutines();
                        break;
                    case 2:
                        this.oraSysStatsPreparedStatement = this.con.prepareStatement("select statistic#,value from v$sysstat where value<>0");
                        this.oraSysStatsPreparedStatement.setFetchSize(500);
                        this.oraSQLTextsPreparedStatement = this.con.prepareStatement("select sql_id,sql_text from v$sqlarea");
                        this.oraSQLTextsPreparedStatement.setFetchSize(1000);
                        this.oraStatNamesPreparedStatement = this.con.prepareStatement("select statistic#,name from v$statname");
                        this.oraStatNamesPreparedStatement.setFetchSize(1000);
                        this.oraSQLPlansPreparedStatement = this.con.prepareStatement("select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0");
                        this.oraSQLPlansPreparedStatement.setFetchSize(1000);
                        runSysRoutines();
                        break;
                    case 3:
                        this.oraSQLStatsPreparedStatement = this.con.prepareStatement("");
                        this.oraSQLStatsPreparedStatement.setFetchSize(500);
                        runSQLRoutines();
                        break;
                    default:
                        this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbConnectionString + "\tUnknown thread type provided!");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tCannot prepare statements for  Oracle database: " + this.dbConnectionString);
            }
            cleanup();
        }
    }
}
