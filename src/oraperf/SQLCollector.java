package oraperf;

import java.math.BigDecimal;
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

public class SQLCollector implements Configurable {

    private final SL4JLogger lg;
    private final int dbVersion;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private final Connection con;
    private final BlockingQueue<OraCkhMsg> ckhQueue;
    private static final String ORASQLTEXTSQUERY = "select sql_id,sql_text from v$sqlarea";
    private static final String ORASQLTEXTSQUERYCDB = "select sql_id,sql_text from v$sqlarea where con_id=sys_context('USERENV','CON_ID')";
    private static final String ORASQLPLANSQUERY = "select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0";
    private static final String ORASQLPLANSQUERYCDB = "select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0 and con_id=sys_context('USERENV','CON_ID')";
    private static final String ORASQLSTATSQUERY = "select " +
    "sql_id , " +
    "plan_hash_value , " +
    "version_count , " +
    "sharable_mem , " +
    "persistent_mem , " +
    "runtime_mem , " +
    "sorts , " +
    "loaded_versions , " +
    "open_versions , " +
    "users_opening , " +
    "users_executing , " +
    "fetches , " +
    "executions , " +
    "px_servers_executions , " +
    "end_of_fetch_count , " +
    "loads , " +
    "nvl(first_load_time,to_date('19700101010000','YYYYMMDDHH24MISS')) , " +
    "nvl(last_load_time,to_date('19700101010000','YYYYMMDDHH24MISS')) , " +
    "greatest(nvl(last_active_time,to_date('19700101010000','YYYYMMDDHH24MISS')),to_date('19710101010000','YYYYMMDDHH24MISS')) , " +
    "invalidations , " +
    "parse_calls , " +
    "disk_reads , " +
    "direct_writes , " +
    "buffer_gets , " +
    "cpu_time , " +
    "elapsed_time , " +
    "application_wait_time , " +
    "concurrency_wait_time , " +
    "cluster_wait_time , " +
    "user_io_wait_time , " +
    "plsql_exec_time ," +
    "java_exec_time , " +
    "rows_processed , " +
    "command_type , " +
    "to_char(nvl(optimizer_cost,0)) , " +
    "nvl(parsing_schema_name,'-') , " +
    "kept_versions , " +
    "nvl(object_status,'-') , " +
    "nvl(sql_profile,'-') , " +
    "program_id , " +
    "program_line# , " +
    "round(io_cell_offload_eligible_bytes/1024/1024) , " +
    "round(io_interconnect_bytes/1024/1024) , " +
    "physical_read_requests , " +
    "round(physical_read_bytes/1024/1024) , " +
    "physical_write_requests , " +
    "round(physical_write_bytes/1024/1024) , " +
    "optimized_phy_read_requests , " +
    "round(io_cell_uncompressed_bytes/1024/1024), " +
    "round(io_cell_offload_returned_bytes/1024/1024) " +
    "from v$sqlarea_plan_hash";
    private static final String ORASQLSTATSQUERYCDB = "";

    public SQLCollector(Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr, int version) {
        ckhQueue                = queue;
        con                     = conn;
        dbConnectionString      = connstr;
        dbUniqueName            = dbname;
        dbHostName              = dbhost;
        dbVersion               = version;
        lg                      = new SL4JLogger();
    }

    private void cleanup(
            PreparedStatement oraSQLPlansPreparedStatement,
            PreparedStatement oraSQLStatsPreparedStatement,
            PreparedStatement oraSQLTextsPreparedStatement
    ) {
        try {
            if ((oraSQLPlansPreparedStatement != null) && (!oraSQLPlansPreparedStatement.isClosed())) {
                oraSQLPlansPreparedStatement.close();
            }
            if ((oraSQLStatsPreparedStatement != null) && (!oraSQLStatsPreparedStatement.isClosed())) {
                oraSQLStatsPreparedStatement.close();
            }
            if ((oraSQLTextsPreparedStatement != null) && (!oraSQLTextsPreparedStatement.isClosed())) {
                oraSQLTextsPreparedStatement.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + dbConnectionString + "\t" + "Error durring ORADB resource cleanups!" + "\t" + e.getMessage()
            );

            //e.printStackTrace();
        }
    }

    private List getSQlPlansListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getLong(2));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from sql plans resultset"
                    + "\t" + e.getMessage()
            );
        }
        return outList;
    }

    private List getSQlTextsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getString(2));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from  sql texts resultset"
                    + "\t" + e.getMessage()
            );
        }
        return outList;
    }

    private List getSQlStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getString(1));
                rowList.add(rs.getLong(2));
                rowList.add(rs.getInt(3));
                rowList.add(rs.getLong(4));
                rowList.add(rs.getLong(5));
                rowList.add(rs.getLong(6));
                rowList.add(rs.getLong(7));
                rowList.add(rs.getInt(8));
                rowList.add(rs.getInt(9));
                rowList.add(rs.getInt(10));
                rowList.add(rs.getInt(11));
                rowList.add(rs.getLong(12));
                rowList.add(rs.getLong(13));
                rowList.add(rs.getLong(14));
                rowList.add(rs.getLong(15));
                rowList.add(rs.getInt(16));
                rowList.add(rs.getTimestamp(17).getTime() / 1000L); //--first load time
                rowList.add(rs.getTimestamp(18).getTime() / 1000L);
                rowList.add(rs.getTimestamp(19).getTime() / 1000L);
                rowList.add(rs.getLong(20));
                rowList.add(rs.getLong(21));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(22)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(23)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(24)).setScale(0));
                rowList.add(rs.getLong(25));
                rowList.add(rs.getLong(26));
                rowList.add(rs.getLong(27));
                rowList.add(rs.getLong(28));
                rowList.add(rs.getLong(29));
                rowList.add(rs.getLong(30));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(31)).setScale(0));
                rowList.add(rs.getLong(32));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(33)).setScale(0)); //-- rows processed
                rowList.add(rs.getInt(34));
                rowList.add(rs.getString(35));
                rowList.add(rs.getString(36));
                rowList.add(rs.getInt(37));
                rowList.add(rs.getString(38));
                rowList.add(rs.getString(39));
                rowList.add(rs.getLong(40));
                rowList.add(rs.getInt(41)); //program line
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(42)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(43)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(44)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(45)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(46)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(47)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(48)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(49)).setScale(0));
                rowList.add((BigDecimal) new BigDecimal(rs.getDouble(50)).setScale(0));
                outList.add(rowList);
            }
            rs.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                    + "\t" + "error getting data from sql stats resultset"
                    + "\t" + e.getMessage()
            );
            e.printStackTrace();
        }
        return outList;
    }    
    
    private boolean collectSQLStats(boolean shutdown,PreparedStatement oraSQLStatsPreparedStatement) throws InterruptedException {
        if (!shutdown) {
            
            try {
                oraSQLStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSQLSTAT, Instant.now().getEpochSecond(), dbUniqueName, dbHostName,
                        getSQlStatsListFromRS(oraSQLStatsPreparedStatement.getResultSet())));

                oraSQLStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error processing sql stats!"
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }        
        }
        return shutdown;
    }
        
    private boolean collectSQLTexts(boolean shutdown,PreparedStatement oraSQLTextsPreparedStatement) throws InterruptedException {
        if (!shutdown) {
            try {
                oraSQLTextsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSQLTEXT, 0, null, null,
                        getSQlTextsListFromRS(oraSQLTextsPreparedStatement.getResultSet())));

                oraSQLTextsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error processing sql texts!"
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
        }
        return shutdown;
    }

    private boolean collectSQLPlans(boolean shutdown,PreparedStatement oraSQLPlansPreparedStatement) throws InterruptedException {
        if (!shutdown) {
            try {
                oraSQLPlansPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSQLPHV, 0, null, null,
                        getSQlPlansListFromRS(oraSQLPlansPreparedStatement.getResultSet())));

                oraSQLPlansPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbConnectionString
                        + "\t" + "error processing sql plan hash values"
                        + "\t" + e.getMessage()
                );

                shutdown = true;
                //e.printStackTrace();
            }
        }
        return shutdown;
    }

    public void RunCollection() throws InterruptedException {
        PreparedStatement oraSQLPlansPreparedStatement=null;
        PreparedStatement oraSQLStatsPreparedStatement=null;
        PreparedStatement oraSQLTextsPreparedStatement=null;
        boolean shutdown = false;
        try {
            oraSQLStatsPreparedStatement = con.prepareStatement(ORASQLSTATSQUERY);
            oraSQLStatsPreparedStatement.setFetchSize(10000);
            oraSQLPlansPreparedStatement = con.prepareStatement((dbVersion >= 12) ? ORASQLPLANSQUERYCDB : ORASQLPLANSQUERY);
            oraSQLPlansPreparedStatement.setFetchSize(10000);
            oraSQLTextsPreparedStatement = con.prepareStatement((dbVersion >= 12) ? ORASQLTEXTSQUERYCDB : ORASQLTEXTSQUERY);
            oraSQLTextsPreparedStatement.setFetchSize(10000);
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Cannot prepare statements for  Oracle database: " + dbConnectionString
            );
            shutdown = true;
        }
        int counter = 0;
        long begints,endts;
        while (!shutdown) {
            shutdown = collectSQLStats(shutdown,oraSQLStatsPreparedStatement);
            if(counter==6){
                begints = System.currentTimeMillis();
                shutdown = collectSQLTexts(shutdown,oraSQLTextsPreparedStatement);
                shutdown = collectSQLPlans(shutdown,oraSQLPlansPreparedStatement);
                endts = System.currentTimeMillis();
                counter = 0;
                if (endts - begints < SECONDSBETWEENSQLSNAPS * 1000L) {
                    TimeUnit.SECONDS.sleep(SECONDSBETWEENSQLSNAPS - (int) ((endts - begints) / 1000L));
                }                
            }else{
                TimeUnit.SECONDS.sleep(SECONDSBETWEENSQLSNAPS);
            }
            counter++;
        }
        shutdown = true;
        cleanup(oraSQLPlansPreparedStatement,oraSQLStatsPreparedStatement,oraSQLTextsPreparedStatement);
    }
}
