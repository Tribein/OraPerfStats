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

public class SQLCollector {
    SL4JLogger lg;
    private final int SECONDSBETWEENSQLSNAPS = 600;
    private final int RSSQLSTAT = 3;
    private final int RSSQLPHV = 5;
    private final int RSSQLTEXT = 6;
    private final int dbVersion;
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private final Connection con;
    private PreparedStatement oraSQLPlansPreparedStatement;
    private PreparedStatement oraSQLStatsPreparedStatement;

    private long currentDateTime;
    private boolean shutdown = false;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private static final String ORASQLTEXTSQUERY = "select sql_id,sql_text from v$sqlarea";
    private static final String ORASQLTEXTSQUERYCDB = "select sql_id,sql_text from v$sqlarea where con_id=sys_context('USERENV','CON_ID')";
    private static final String ORASQLPLANSQUERY = "select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0";
    private static final String ORASQLPLANSQUERYCDB = "select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0 and con_id=sys_context('USERENV','CON_ID')";
    private static final String ORASQLSTATSQUERY = "";
    private static final String ORASQLSTATSQUERYCDB = "";
    
    public SQLCollector (Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr, int version){
        ckhQueue                = queue;
        con                     = conn;
        dbConnectionString      = connstr;
        dbUniqueName            = dbname;
        dbHostName              = dbhost; 
        dbVersion               = version;
    }
    private void cleanup() {
        try {
            if ((oraSQLPlansPreparedStatement != null) && (!oraSQLPlansPreparedStatement.isClosed())) {
                oraSQLPlansPreparedStatement.close();
            }
            if ((oraSQLStatsPreparedStatement != null) && (!oraSQLStatsPreparedStatement.isClosed())) {
                oraSQLStatsPreparedStatement.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                    dbConnectionString + "\t"+"Error durring ORADB resource cleanups!" + "\t" + e.getMessage()
            );

            //e.printStackTrace();
        }
    }
    public void RunCollection() throws InterruptedException{
        lg = new SL4JLogger();
        
        try{
            oraSQLStatsPreparedStatement = con.prepareStatement("");
            oraSQLStatsPreparedStatement.setFetchSize(500);        
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Cannot prepare statements for  Oracle database: " + dbConnectionString
            );
            shutdown = true;
        }
        while (!shutdown) {
            try {
                currentDateTime = Instant.now().getEpochSecond();
                oraSQLStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSQLSTAT, currentDateTime, dbUniqueName, dbHostName, null));

                oraSQLStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        dbConnectionString + "\t"+"Error processing SQL statistics!" + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSQLSNAPS);
        }
        cleanup();
    }
        
}
