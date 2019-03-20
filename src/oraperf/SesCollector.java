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

public class SesCollector {
    SL4JLogger lg;
    private final int SECONDSBETWEENSESSSTATSSNAPS = 30;
    private final int RSSESSIONSTAT = 1;
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private Connection con;
    private PreparedStatement oraSesStatsPreparedStatement;
    private long currentDateTime;
    private boolean shutdown = false;
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
            "            type = 'USER' " +
            "            AND sid <> sys_context('USERENV','SID') " +
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
    private final BlockingQueue<OraCkhMsg> ckhQueue;    

    public SesCollector(Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr){
        ckhQueue                = queue;
        con                     = conn;
        dbConnectionString      = connstr;
        dbUniqueName            = dbname;
        dbHostName              = dbhost;
        
    }
    
    private List getSesStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getInt(1));
                rowList.add(rs.getInt(2));
                rowList.add(rs.getInt(3));
                rowList.add((long) new BigDecimal(rs.getDouble(4)).setScale(0, RoundingMode.HALF_UP).doubleValue());
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }
        return outList;
    }    
    
    private void cleanup() {
        try {
            if ((oraSesStatsPreparedStatement != null) && (!oraSesStatsPreparedStatement.isClosed())) {
                oraSesStatsPreparedStatement.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                    dbConnectionString + "\t"+"Error durring ORADB resource cleanups!"
            );

            e.printStackTrace();
        }
    }
    
    public void RunCollection() throws InterruptedException{
        lg = new SL4JLogger();
        try{
            oraSesStatsPreparedStatement = con.prepareStatement(ORASESSTATSQUERY);
            oraSesStatsPreparedStatement.setFetchSize(1000);      
        }catch(SQLException e){
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                        "Cannot prepare statements for  Oracle database: " + dbConnectionString
                ); 
                shutdown = true;
        }  
        while (!shutdown) {
            try {
                currentDateTime = Instant.now().getEpochSecond();
                oraSesStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSESSIONSTAT, currentDateTime, dbUniqueName, dbHostName,
                        getSesStatsListFromRS(oraSesStatsPreparedStatement.getResultSet())));

                oraSesStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        dbConnectionString + "\t"+"Error processing session statistics!"
                );

                shutdown = true;
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSESSSTATSSNAPS);
        }        
        cleanup();
    }
    
}
