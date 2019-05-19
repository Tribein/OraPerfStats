package oraperf;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WaitsCollector {

    SL4JLogger lg;
    private final int RSSESSIONWAIT = 0;
    private final int RSIOFILESTAT = 8;
    private final int RSIOFUNCTIONSTAT = 9;
    private final int SECONDSBETWEENSESSWAITSSNAPS = 10;
    private final int dbVersion;
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private final Connection con;
    private PreparedStatement oraWaitsPreparedStatement;
    private PreparedStatement oraIOFileStatsPreparedStatement;
    private PreparedStatement oraIOFunctionStatsPreparedStatement;
    private long currentDateTime;
    private boolean shutdown = false;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private static final String ORASESSWAITSQUERY
            = "with t as ( "
            + "SELECT /*+ m0aterialize */ "
            + "        * "
            + "        FROM   "
            + "        v$session  "
            + ") "
            + "select  "
            + "        a.sid,   "
            + "        a.serial#,   "
            + "        DECODE(a.taddr,NULL,'N','Y') trn,   "
            + "        a.status,   "
            + "        nvl(a.username,a.schemaname),   "
            + "        nvl(a.osuser,'-'),   "
            + "        nvl(a.machine,'-'),   "
            + "        nvl(a.program,'-'),   "
            + "        a.type,   "
            + "        nvl(a.module,'-'),   "
            + "        nvl(a.blocking_session,0),   "
            + "        DECODE(a.state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',a.event)  event, "
            + "        DECODE(a.state,'WAITED KNOWN TIME',127,'WAITED SHORT TIME',127,a.wait_class#) waitclass, "
            + "        round(a.wait_time_micro / 1000000,3) wittime,   "
            + "        nvl(a.sql_id,'-') sqlid,   "
            + "        nvl(a.sql_exec_start,TO_DATE('19700101','YYYYMMDD')) sqlexecstart,   "
            + "        a.sql_exec_id,   "
            + "        a.logon_time,   "
            + "        a.seq#,   "
            + "        nvl(a.p1,0),   "
            + "        nvl(a.p2,0),   "
            + "        a.row_wait_obj#,  "
            + "        nvl(a.PLSQL_ENTRY_OBJECT_ID,0) penobj,   "
            + "        nvl(a.PLSQL_ENTRY_SUBPROGRAM_ID,0) pensubp,   "
            + "        nvl(a.PLSQL_OBJECT_ID,0) pobj,  "
            + "        nvl(a.PLSQL_SUBPROGRAM_ID,0) psubp,   "
            + "        a.LAST_CALL_ET,  "
            + "        substr(a.PDML_STATUS,1,1) pdml,  "
            + "        substr(a.PDDL_STATUS,1,1) pddl,  "
            + "        substr(a.PQ_STATUS,1,1) pq,  "
            + "        nvl(a.ECID,'-') ecid "
            + "from t a "
            + "join t b on ( "
            + "    (a.wait_class#<>6 and a.sid=b.sid) "
            + "    or "
            + "    (a.wait_class#=6 and a.sid in (nvl(b.blocking_session,-1),nvl(b.final_blocking_session,-1))) "
            + ") "
            + "where  "
            + "a.sid <> sys_context('USERENV','SID')";
    private static final String ORASESSWAITSQUERYCDB
            = "with t as ( "
            + "SELECT /*+ m0aterialize */ "
            + "        * "
            + "        FROM   "
            + "        v$session  "
            + "         where con_id=sys_context('USERENV','CON_ID') "
            + ") "
            + "select  "
            + "        a.sid,   "
            + "        a.serial#,   "
            + "        DECODE(a.taddr,NULL,'N','Y') trn,   "
            + "        a.status,   "
            + "        nvl(a.username,a.schemaname),   "
            + "        nvl(a.osuser,'-'),   "
            + "        nvl(a.machine,'-'),   "
            + "        nvl(a.program,'-'),   "
            + "        a.type,   "
            + "        nvl(a.module,'-'),   "
            + "        nvl(a.blocking_session,0),   "
            + "        DECODE(a.state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',a.event)  event, "
            + "        DECODE(a.state,'WAITED KNOWN TIME',127,'WAITED SHORT TIME',127,a.wait_class#) waitclass, "
            + "        round(a.wait_time_micro / 1000000,3) wittime,   "
            + "        nvl(a.sql_id,'-') sqlid,   "
            + "        nvl(a.sql_exec_start,TO_DATE('19700101','YYYYMMDD')) sqlexecstart,   "
            + "        a.sql_exec_id,   "
            + "        a.logon_time,   "
            + "        a.seq#,   "
            + "        nvl(a.p1,0),   "
            + "        nvl(a.p2,0),   "
            + "        a.row_wait_obj#,  "
            + "        nvl(a.PLSQL_ENTRY_OBJECT_ID,0) penobj,   "
            + "        nvl(a.PLSQL_ENTRY_SUBPROGRAM_ID,0) pensubp,   "
            + "        nvl(a.PLSQL_OBJECT_ID,0) pobj,  "
            + "        nvl(a.PLSQL_SUBPROGRAM_ID,0) psubp,   "
            + "        a.LAST_CALL_ET,  "
            + "        substr(a.PDML_STATUS,1,1) pdml,  "
            + "        substr(a.PDDL_STATUS,1,1) pddl,  "
            + "        substr(a.PQ_STATUS,1,1) pq,  "
            + "        nvl(a.ECID,'-') ecid "
            + "from t a "
            + "join t b on ( "
            + "    (a.wait_class#<>6 and a.sid=b.sid) "
            + "    or "
            + "    (a.wait_class#=6 and a.sid in (nvl(b.blocking_session,-1),nvl(b.final_blocking_session,-1))) "
            + ") "
            + "where  "
            + "a.sid <> sys_context('USERENV','SID')";    
    private static final String ORAIOFILESTATSQUERY = "select /*+ rule */filetype_name,coalesce(b.name,c.name,'-'),small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,small_sync_read_reqs,small_read_servicetime,small_write_servicetime,small_sync_read_latency,large_read_servicetime,large_write_servicetime from v$iostat_file a left join v$datafile b on (b.file#=a.file_no and a.filetype_id=2) left join v$tempfile c on (c.file#=a.file_no and a.filetype_id=6)";
    private static final String ORAIOFILESTATSQUERYCDB = "select /*+ rule */filetype_name,coalesce(b.name,c.name,'-'),small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,small_sync_read_reqs,small_read_servicetime,small_write_servicetime,small_sync_read_latency,large_read_servicetime,large_write_servicetime from v$iostat_file a left join v$datafile b on (b.file#=a.file_no and a.filetype_id=2) left join v$tempfile c on (c.file#=a.file_no and a.filetype_id=6) where a.con_id=sys_context('USERENV','CON_ID')";
    private static final String ORAIOFUNCTIONSTATSQUERY = "select function_name,filetype_name,small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,number_of_waits,wait_time from v$iostat_function_detail";

    public WaitsCollector(Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr, int version) {
        ckhQueue            = queue;
        con                 = conn;
        dbConnectionString  = connstr;
        dbUniqueName        = dbname;
        dbHostName          = dbhost;
        dbVersion           = version;
    }

    private List getIOFileStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
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
                rowList.add(new BigDecimal(rs.getDouble(16)).longValue());
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Error getting data from resultset " + dbConnectionString
                    + "\t" + e.getMessage()
            );
        }
        return outList;
    }

    private List getIOFunctionStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
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
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Error getting data from resultset " + dbConnectionString
                    + "\t" + e.getMessage()
            );
        }
        return outList;
    }

    private List getSessionWaitsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getInt(1));
                rowList.add(rs.getInt(2));
                rowList.add(rs.getString(3));
                rowList.add(rs.getString(4).substring(0, 1));
                rowList.add(rs.getString(5));
                rowList.add(rs.getString(6));
                rowList.add(rs.getString(7));
                rowList.add(rs.getString(8));
                rowList.add(rs.getString(9).substring(0, 1));
                rowList.add(rs.getString(10));
                rowList.add(rs.getInt(11));
                rowList.add(rs.getString(12));
                rowList.add(rs.getLong(13));
                rowList.add(rs.getFloat(14));
                rowList.add(rs.getString(15));
                rowList.add(rs.getTimestamp(16).getTime() / 1000L);
                rowList.add(rs.getInt(17));
                rowList.add(rs.getTimestamp(18).getTime() / 1000L);
                rowList.add(rs.getInt(19));
                rowList.add((long) new BigDecimal(rs.getDouble(20)).setScale(0, RoundingMode.HALF_UP).doubleValue());
                rowList.add(rs.getLong(21));
                rowList.add(((rs.getLong(22) >= 0) ? rs.getLong(22) : 0));
                rowList.add(rs.getLong(23));
                rowList.add(rs.getLong(24));
                rowList.add(rs.getLong(25));
                rowList.add(rs.getLong(26));
                rowList.add(rs.getLong(27));
                rowList.add(rs.getString(28));
                rowList.add(rs.getString(29));
                rowList.add(rs.getString(30));
                rowList.add(rs.getString(31));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Error getting data from resultset " + dbConnectionString
                    + "\t" + e.getMessage()
            );
        }
        return outList;
    }

    private void cleanup() {
        try {
            if ((oraWaitsPreparedStatement != null) && (!oraWaitsPreparedStatement.isClosed())) {
                oraWaitsPreparedStatement.close();
            }
            if ((oraIOFileStatsPreparedStatement != null) && (!oraIOFileStatsPreparedStatement.isClosed())) {
                oraIOFileStatsPreparedStatement.close();
            }
            if ((oraIOFunctionStatsPreparedStatement != null) && (!oraIOFunctionStatsPreparedStatement.isClosed())) {
                oraIOFunctionStatsPreparedStatement.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + dbConnectionString + "\t" + "Error durring ORADB resource cleanups!" 
                    + "\t" + e.getMessage()
            );
            //e.printStackTrace();
        }
    }

    public void RunCollection() throws InterruptedException {
        lg = new SL4JLogger();
        try {
            //oraWaitsPreparedStatement = con.prepareStatement((dbVersion>=12)? ORASESSWAITSQUERYCDB : ORASESSWAITSQUERY);
            oraWaitsPreparedStatement = con.prepareStatement(ORASESSWAITSQUERY);
            oraWaitsPreparedStatement.setFetchSize(1000);
            //oraIOFileStatsPreparedStatement = con.prepareStatement((dbVersion>=12)? ORAIOFILESTATSQUERYCDB: ORAIOFILESTATSQUERY);
            oraIOFileStatsPreparedStatement = con.prepareStatement(ORAIOFILESTATSQUERY);
            oraIOFileStatsPreparedStatement.setFetchSize(100);
            oraIOFunctionStatsPreparedStatement = con.prepareStatement(ORAIOFUNCTIONSTATSQUERY);
            oraIOFunctionStatsPreparedStatement.setFetchSize(100);
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Cannot prepare statements for  Oracle database: " + dbConnectionString
            );
            shutdown = true;
        }
        while (!shutdown) {
            currentDateTime = Instant.now().getEpochSecond();
            try {
                oraIOFileStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSIOFILESTAT, currentDateTime, dbUniqueName, dbHostName,
                        getIOFileStatsListFromRS(oraIOFileStatsPreparedStatement.getResultSet())));

                oraIOFileStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                        + "Error getting io file stats from database " + dbConnectionString
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
            if (!shutdown) {
                currentDateTime = Instant.now().getEpochSecond();
                try {
                    oraIOFunctionStatsPreparedStatement.execute();
                    ckhQueue.put(new OraCkhMsg(RSIOFUNCTIONSTAT, currentDateTime, dbUniqueName, dbHostName,
                            getIOFunctionStatsListFromRS(oraIOFunctionStatsPreparedStatement.getResultSet())));

                    oraIOFunctionStatsPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                            + "Error getting io function stats from database " + dbConnectionString
                            + "\t" + e.getMessage()
                    );

                    shutdown = true;
                    //e.printStackTrace();
                }
            }
            if (!shutdown) {
                currentDateTime = Instant.now().getEpochSecond();
                try {
                    oraWaitsPreparedStatement.execute();
                    ckhQueue.put(new OraCkhMsg(RSSESSIONWAIT, currentDateTime, dbUniqueName, dbHostName,
                            getSessionWaitsListFromRS(oraWaitsPreparedStatement.getResultSet())));

                    oraWaitsPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                            + "Error getting sessions from database " + dbConnectionString
                            + "\t" + e.getMessage()
                    );

                    shutdown = true;
                    //e.printStackTrace();
                }
            }
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSESSWAITSSNAPS);
        }
        cleanup();
    }
}
