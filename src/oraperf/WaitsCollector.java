package oraperf;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WaitsCollector implements Configurable {

    SL4JLogger lg;
    private final int dbVersion;
    private final Connection con;
    private PreparedStatement oraWaitsPreparedStatement;
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
            + "        nvl(a.ECID,'-') ecid, "
            + "        nvl(a.SQL_CHILD_NUMBER,-1) sql_child_number "
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

    public WaitsCollector(Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr, int version) {
        ckhQueue                = queue;
        con                     = conn;
        dbConnectionString      = connstr;
        dbUniqueName            = dbname;
        dbHostName              = dbhost;
        dbVersion               = version;
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
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(20)).setScale(0, RoundingMode.HALF_UP));
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
                rowList.add(rs.getInt(32)); 
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from resultset"
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
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error durring ORADB resource cleanups"
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
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "cannot prepare statements"
            );
            shutdown = true;
        }
        while (!shutdown) {
            currentDateTime = Instant.now().getEpochSecond();
            try {
                oraWaitsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSESSIONWAIT, currentDateTime, dbUniqueName, dbHostName,
                        getSessionWaitsListFromRS(oraWaitsPreparedStatement.getResultSet())));

                oraWaitsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error getting sessions from database"
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSESSWAITSSNAPS);
        }
        cleanup();
    }
}
