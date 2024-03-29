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

public class SesCollector implements Configurable {
    private final SL4JLogger lg;
    private final int dbVersion;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private final Connection con;
    private static final String ORASESSTATSQUERY = 
            "SELECT " +
            "    sid, " +
            "    sserial, " +
            "    statistic#, " +
            "    value " +
            "FROM " +
            "    ( " +
            "        SELECT " +
            "            sid, " +
            "            serial# sserial " +
            "        FROM " +
            "            v$session " +
            "        WHERE " +
            "            /*type = 'USER' AND*/ " +
            "            sid <> sys_context('USERENV','SID') " +
            "            AND ( " +
            "                wait_class# <> 6 " +
            "                OR ( " +
            "                    wait_class# = 6 " +
            "                    AND seconds_in_wait < 10 " +
            "                ) " +
            "            ) " +
            "    ) a " +
            "    JOIN v$sesstat b USING ( sid ) " +
            "    JOIN v$statname c USING ( statistic# ) " +
            "WHERE" +
            "    name IN (" +
            "        'Requests to/from client'," +
            "        'user commits'," +
            "        'user rollbacks'," +
            "        'user calls'," +
            "        'recursive calls'," +
            "        'recursive cpu usage'," +
            "        'DB time'," +
            "        'session pga memory'," +
            "        'physical read total bytes'," +
            "        'physical write total bytes'," +
            "        'db block changes'," +
            "        'redo size'," +
            "        'redo size for direct writes'," +
            "        'table fetch by rowid'," +
            "        'table fetch continued row'," +
            "        'lob reads'," +
            "        'lob writes'," +
            "        'index fetch by key'," +
            "        'sql area evicted'," +
            "        'session cursor cache hits'," +
            "        'session cursor cache count'," +
            "        'opened cursors cumulative'," +
            "        'opened cursors current'," +
            "        'pinned cursors current'," +
            "        'queries parallelized'," +
            "        'Parallel operations not downgraded'," +
            "        'Parallel operations downgraded to serial'," +
            "        'parse time cpu'," +
            "        'parse count (total)'," +
            "        'parse count (hard)'," +
            "        'parse count (failures)'," +
            "        'sorts (memory)'," +
            "        'sorts (disk)'" +
            "    ) " +
            "    AND b.value <> 0";
    private static final String ORASESSTATSQUERYCDB = 
            "SELECT " +
            "    sid, " +
            "    sserial, " +
            "    statistic#, " +
            "    value " +
            "FROM " +
            "    ( " +
            "        SELECT " +
            "            sid, " +
            "            serial# sserial " +
            "        FROM " +
            "            v$session " +
            "        WHERE " +
            "            /*type = 'USER' AND*/ " +
            "            sid <> sys_context('USERENV','SID') " +
            "            AND ( " +
            "                wait_class# <> 6 " +
            "                OR ( " +
            "                    wait_class# = 6 " +
            "                    AND seconds_in_wait < 10 " +
            "                ) " +
            "            ) " +
            "    ) a " +
            "    JOIN v$sesstat b USING ( sid ) " +
            "    JOIN v$statname c USING ( statistic# ) " +
            "WHERE" +
            "    name IN (" +
            "        'Requests to/from client'," +
            "        'user commits'," +
            "        'user rollbacks'," +
            "        'user calls'," +
            "        'recursive calls'," +
            "        'recursive cpu usage'," +
            "        'DB time'," +
            "        'session pga memory'," +
            "        'physical read total bytes'," +
            "        'physical write total bytes'," +
            "        'db block changes'," +
            "        'redo size'," +
            "        'redo size for direct writes'," +
            "        'table fetch by rowid'," +
            "        'table fetch continued row'," +
            "        'lob reads'," +
            "        'lob writes'," +
            "        'index fetch by key'," +
            "        'sql area evicted'," +
            "        'session cursor cache hits'," +
            "        'session cursor cache count'," +
            "        'opened cursors cumulative'," +
            "        'opened cursors current'," +
            "        'pinned cursors current'," +
            "        'queries parallelized'," +
            "        'Parallel operations not downgraded'," +
            "        'Parallel operations downgraded to serial'," +
            "        'parse time cpu'," +
            "        'parse count (total)'," +
            "        'parse count (hard)'," +
            "        'parse count (failures)'," +
            "        'sorts (memory)'," +
            "        'sorts (disk)'" +
            "    ) " +
            "    AND b.value <> 0 and a.con_id=sys_context('USERENV','CON_ID')";    
    private final BlockingQueue<OraCkhMsg> ckhQueue;    

    public SesCollector(Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr, int version){
        ckhQueue                = queue;
        con                     = conn;
        dbConnectionString      = connstr;
        dbUniqueName            = dbname;
        dbHostName              = dbhost;
        dbVersion               = version;
        lg                      = new SL4JLogger();
    }
    
    private List getSesStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getInt(1));
                rowList.add(rs.getInt(2));
                rowList.add(rs.getInt(3));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(4)).setScale(0, RoundingMode.HALF_UP));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from session stats resultset" 
                    + "\t" + e.getMessage()
            );
        }
        return outList;
    }    
    
    private void cleanup(PreparedStatement oraSesStatsPreparedStatement) {
        try {
            if(this.con.isClosed()){
                oraSesStatsPreparedStatement=null;
                return;
            }
            if ((oraSesStatsPreparedStatement != null) && (!oraSesStatsPreparedStatement.isClosed())) {
                oraSesStatsPreparedStatement.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error during ORADB resource cleanups"
                    + "\t" + e.getMessage()
            );

            //e.printStackTrace();
        }
    }
    
    public void RunCollection() throws InterruptedException{
        PreparedStatement oraSesStatsPreparedStatement=null;
        boolean shutdown = false;
        try{
            //oraSesStatsPreparedStatement = con.prepareStatement((dbVersion>=12)? ORASESSTATSQUERYCDB : ORASESSTATSQUERY);
            oraSesStatsPreparedStatement = con.prepareStatement(ORASESSTATSQUERY);
            oraSesStatsPreparedStatement.setFetchSize(1000);      
        }catch(SQLException e){
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "cannot prepare statements"
                        + "\t" + e.getMessage()
                ); 
                shutdown = true;
        }  
        while (!shutdown) {
            try {
                oraSesStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSESSIONSTAT, Instant.now().getEpochSecond(), dbUniqueName, dbHostName,
                        getSesStatsListFromRS(oraSesStatsPreparedStatement.getResultSet())));

                oraSesStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error processing session statistics" 
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSESSSTATSSNAPS);
        }  
        cleanup(oraSesStatsPreparedStatement);
    }
    
}
