/* 
 * Copyright (C) 2017 Tribein
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
    private static final String ORAIOFILESTATSQUERY = "select filetype_name,coalesce(b.name,c.name,'-'),small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,small_sync_read_reqs,small_read_servicetime,small_write_servicetime,small_sync_read_latency,large_read_servicetime,large_write_servicetime from v$iostat_file a left join v$datafile b on (b.file#=a.file_no and a.filetype_id=2) left join v$tempfile c on (c.file#=a.file_no and a.filetype_id=6)";
    private static final String ORAIOFUNCTIONSTATSQUERY = "select function_name,filetype_name,small_read_megabytes,small_write_megabytes,large_read_megabytes,large_write_megabytes,small_read_reqs,small_write_reqs,large_read_reqs,large_write_reqs,number_of_waits,wait_time from v$iostat_function_detail";
    private static final String ORAIOSESSIONSTATSQUERY= "select sid,block_gets,consistent_gets,physical_reads,block_changes,consistent_changes  from v$sess_Io";

    public StatCollector(String inputString, String dbUSN, String dbPWD, ComboPooledDataSource ckhDS, int runTType, BlockingQueue<OraCkhMsg> queue) {
        dbConnectionString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
        dbUserName = dbUSN;
        dbPassword = dbPWD;
        threadType = runTType;
        ckhQueue = queue;
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
                rowList.add((long) new BigDecimal(rs.getDouble(20)).setScale(0, RoundingMode.HALF_UP).doubleValue() );
                rowList.add(rs.getLong(21));
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

    private List getStatNamesListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(rs.getInt(1));
                rowList.add(rs.getString(2));
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
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }
        return outList;
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
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }
        return outList;
    }

    private List getSysStatsListFromRS(ResultSet rs) {
        List<List> outList = new ArrayList();
        try {
            while (rs != null && rs.next()) {
                List rowList = new ArrayList();
                rowList.add(Integer.valueOf(rs.getInt(1)));
                rowList.add((long) new BigDecimal(rs.getDouble(2)).setScale(0, RoundingMode.HALF_UP).doubleValue());
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
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
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
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Error getting data from resultset " + dbConnectionString
            );
        }
        return outList;
    }

    private void setDateTimeVars() {
        currentDateTime = Instant.now().getEpochSecond();
    }

    private void runSessAndIOStatsRoutines()
            throws InterruptedException {
        while (!shutdown) {
            setDateTimeVars();
            try {
                oraIOFileStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSIOFILESTAT, currentDateTime, dbUniqueName, dbHostName,
                        getIOFileStatsListFromRS(oraIOFileStatsPreparedStatement.getResultSet())));

                oraIOFileStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                        "Error getting io file stats from database " + dbConnectionString
                );

                shutdown = true;
                e.printStackTrace();
            }
            if (!shutdown) {
                setDateTimeVars();
                try {
                    oraIOFunctionStatsPreparedStatement.execute();
                    ckhQueue.put(new OraCkhMsg(RSIOFUNCTIONSTAT, currentDateTime, dbUniqueName, dbHostName,
                            getIOFunctionStatsListFromRS(oraIOFunctionStatsPreparedStatement.getResultSet())));

                    oraIOFunctionStatsPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                            "Error getting io function stats from database " + dbConnectionString
                    );

                    shutdown = true;
                    e.printStackTrace();
                }
            }
            if (!shutdown) {
                setDateTimeVars();
                try {
                    oraWaitsPreparedStatement.execute();
                    ckhQueue.put(new OraCkhMsg(RSSESSIONWAIT, currentDateTime, dbUniqueName, dbHostName,
                            getSessionWaitsListFromRS(oraWaitsPreparedStatement.getResultSet())));

                    oraWaitsPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                            "Error getting sessions from database " + dbConnectionString
                    );

                    shutdown = true;
                    e.printStackTrace();
                }
            }
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSESSWAITSSNAPS);
        }
    }

    private void runSessStatsRoutines()
            throws InterruptedException {
        while (!shutdown) {
            try {
                setDateTimeVars();
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
    }

    private void runSysRoutines()
            throws InterruptedException {
        int snapcounter = 0;
        try {
            oraStatNamesPreparedStatement.execute();
            ckhQueue.put(new OraCkhMsg(RSSTATNAME, 0, null, null,
                    getStatNamesListFromRS(oraStatNamesPreparedStatement.getResultSet())));

            oraStatNamesPreparedStatement.close();
        } catch (Exception e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                    dbConnectionString + "\t"+"Error processing statistics names!"
            );

            shutdown = true;
        }
        while (!shutdown) {
            try {
                setDateTimeVars();
                oraSysStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSYSTEMSTAT, currentDateTime, dbUniqueName, dbHostName,
                        getSysStatsListFromRS(oraSysStatsPreparedStatement.getResultSet())));

                oraSysStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        dbConnectionString + "\t"+"Error processing system statistics!"
                );

                shutdown = true;
                e.printStackTrace();
            }
            if (snapcounter == 30) {
                snapcounter = 0;
                long begints = System.currentTimeMillis();
                if (!shutdown) {
                    try {
                        oraSQLPlansPreparedStatement.execute();
                        ckhQueue.put(new OraCkhMsg(RSSQLPHV, 0, null, null,
                                getSQlPlansListFromRS(oraSQLPlansPreparedStatement.getResultSet())));

                        oraSQLPlansPreparedStatement.clearWarnings();
                    } catch (SQLException e) {
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                                dbConnectionString + "\t"+"Error processing sql plan hash values!"
                        );

                        shutdown = true;
                        e.printStackTrace();
                    }
                }
                if (!shutdown) {
                    try {
                        oraSQLTextsPreparedStatement.execute();
                        ckhQueue.put(new OraCkhMsg(RSSQLTEXT, 0, null, null,
                                getSQlTextsListFromRS(oraSQLTextsPreparedStatement.getResultSet())));

                        oraSQLTextsPreparedStatement.clearWarnings();
                    } catch (SQLException e) {
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                                dbConnectionString + "\t"+"Error processing sql texts!"
                        );

                        shutdown = true;
                        e.printStackTrace();
                    }
                }
                long endts = System.currentTimeMillis();
                if (endts - begints < SECONDSBETWEENSYSSTATSSNAPS*1000L) {
                    TimeUnit.SECONDS.sleep(SECONDSBETWEENSYSSTATSSNAPS - (int) ((endts - begints) / 1000L));
                }
            } else {
                TimeUnit.SECONDS.sleep(SECONDSBETWEENSYSSTATSSNAPS);
                snapcounter++;
            }
        }
    }

    private void runSQLRoutines()
            throws InterruptedException {
        while (!shutdown) {
            try {
                setDateTimeVars();
                oraSQLStatsPreparedStatement.execute();
                ckhQueue.put(new OraCkhMsg(RSSQLSTAT, currentDateTime, dbUniqueName, dbHostName, null));

                oraSQLStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        dbConnectionString + "\t"+"Error processing SQL statistics!"
                );

                shutdown = true;
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(SECONDSBETWEENSQLSNAPS);
        }
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
            if ((oraSysStatsPreparedStatement != null) && (!oraSysStatsPreparedStatement.isClosed())) {
                oraSysStatsPreparedStatement.close();
            }
            if ((oraSesStatsPreparedStatement != null) && (!oraSesStatsPreparedStatement.isClosed())) {
                oraSesStatsPreparedStatement.close();
            }
            if ((oraSQLTextsPreparedStatement != null) && (!oraSQLTextsPreparedStatement.isClosed())) {
                oraSQLTextsPreparedStatement.close();
            }
            if ((oraSQLPlansPreparedStatement != null) && (!oraSQLPlansPreparedStatement.isClosed())) {
                oraSQLPlansPreparedStatement.close();
            }
            if ((oraSQLStatsPreparedStatement != null) && (!oraSQLStatsPreparedStatement.isClosed())) {
                oraSQLStatsPreparedStatement.close();
            }
            if ((con != null) && (!con.isClosed())) {
                con.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                    dbConnectionString + "\t"+"Error durring ORADB resource cleanups!"
            );

            e.printStackTrace();
        }
    }

    private void openConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + dbConnectionString, dbUserName, dbPassword);
            con.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Cannot load Oracle driver!"
            );
            shutdown = true;
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                    "Cannot initiate connection to target Oracle database: " + dbConnectionString
            );

            shutdown = true;
        }
    }

    public void run() {
        lg = new SL4JLogger();

        openConnection();
        if (!shutdown) {
            try {
                switch (threadType) {
                    case 0:
                        oraWaitsPreparedStatement = con.prepareStatement(ORASESSWAITSQUERY);
                        oraWaitsPreparedStatement.setFetchSize(1000);
                        oraIOFileStatsPreparedStatement = con.prepareStatement(ORAIOFILESTATSQUERY);
                        oraIOFileStatsPreparedStatement.setFetchSize(100);
                        oraIOFunctionStatsPreparedStatement = con.prepareStatement(ORAIOFUNCTIONSTATSQUERY);
                        oraIOFunctionStatsPreparedStatement.setFetchSize(100);
                        runSessAndIOStatsRoutines();
                        break;
                    case 1:
                        oraSesStatsPreparedStatement = con.prepareStatement(ORASESSTATSQUERY);
                        oraSesStatsPreparedStatement.setFetchSize(1000);
                        runSessStatsRoutines();
                        break;
                    case 2:
                        oraSysStatsPreparedStatement = con.prepareStatement(ORASYSSTATSQUERY);
                        oraSysStatsPreparedStatement.setFetchSize(500);
                        oraSQLTextsPreparedStatement = con.prepareStatement(ORASQLTEXTSQUERY);
                        oraSQLTextsPreparedStatement.setFetchSize(1000);
                        oraStatNamesPreparedStatement = con.prepareStatement(ORASTATNAMESQUERY);
                        oraStatNamesPreparedStatement.setFetchSize(1000);
                        oraSQLPlansPreparedStatement = con.prepareStatement(ORASQLPLANSQUERY);
                        oraSQLPlansPreparedStatement.setFetchSize(1000);
                        runSysRoutines();
                        break;
                    case 3:
                        oraSQLStatsPreparedStatement = con.prepareStatement("");
                        oraSQLStatsPreparedStatement.setFetchSize(500);
                        runSQLRoutines();
                        break;
                    default:
                        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                                dbConnectionString + "\t"+"Unknown thread type provided!"
                        );
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                        "Cannot prepare statements for  Oracle database: " + dbConnectionString
                );
            }
            cleanup();
        }
    }
}
