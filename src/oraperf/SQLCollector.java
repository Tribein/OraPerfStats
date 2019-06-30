package oraperf;

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

    SL4JLogger lg;
    private final int dbVersion;
    private final String dbConnectionString;
    private final String dbUniqueName;
    private final String dbHostName;
    private final Connection con;
    private PreparedStatement oraSQLPlansPreparedStatement;
    private PreparedStatement oraSQLStatsPreparedStatement;
    private PreparedStatement oraSQLTextsPreparedStatement;

    private long currentDateTime;
    private boolean shutdown = false;
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
    "first_load_time , " +
    "last_load_time , " +
    "last_active_time , " +
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
    "optimizer_cost , " +
    "parsing_schema_name , " +
    "kept_versions , " +
    "object_status , " +
    "sql_profile , " +
    "program_id , " +
    "program_line# , " +
    "io_cell_offload_eligible_bytes , " +
    "io_interconnect_bytes , " +
    "physical_read_requests , " +
    "physical_read_bytes , " +
    "physical_write_requests , " +
    "physical_write_bytes , " +
    "optimized_phy_read_requests , " +
    "io_cell_uncompressed_bytes , " +
    "io_cell_offload_returned_bytes " +
    "from v$sqlarea_plan_hash";
    private static final String ORASQLSTATSQUERYCDB = "";

    public SQLCollector(Connection conn, BlockingQueue<OraCkhMsg> queue, String dbname, String dbhost, String connstr, int version) {
        ckhQueue = queue;
        con = conn;
        dbConnectionString = connstr;
        dbUniqueName = dbname;
        dbHostName = dbhost;
        dbVersion = version;
    }

    private void cleanup() {
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
                    + "\t" + "error getting data from resultset"
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
                    + "\t" + "error getting data from resultset"
                    + "\t" + e.getMessage()
            );
        }
        return outList;
    }

    private void collectSQLTexts() throws InterruptedException {
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
    }

    private void collectSQLPlans() throws InterruptedException {
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
    }

    public void RunCollection() throws InterruptedException {
        lg = new SL4JLogger();

        try {
            oraSQLStatsPreparedStatement = con.prepareStatement(ORASQLSTATSQUERY);
            oraSQLStatsPreparedStatement.setFetchSize(500);
            oraSQLPlansPreparedStatement = con.prepareStatement((dbVersion >= 12) ? ORASQLPLANSQUERYCDB : ORASQLPLANSQUERY);
            oraSQLPlansPreparedStatement.setFetchSize(1000);
            oraSQLTextsPreparedStatement = con.prepareStatement((dbVersion >= 12) ? ORASQLTEXTSQUERYCDB : ORASQLTEXTSQUERY);
            oraSQLTextsPreparedStatement.setFetchSize(1000);
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"
                    + "Cannot prepare statements for  Oracle database: " + dbConnectionString
            );
            shutdown = true;
        }
        while (!shutdown) {
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSQLSNAPS/2);
            currentDateTime = Instant.now().getEpochSecond();
            collectSQLTexts();
            collectSQLPlans();
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSQLSNAPS/2);
        }
        cleanup();
    }
}
