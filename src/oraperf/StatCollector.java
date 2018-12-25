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
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class StatCollector extends Thread {

    SL4JLogger lg;

    private final int secondsBetweenSessSnaps = 10;
    private final int secondsBetweenSessStatsSnaps = 30;
    private final int secondsBetweenSysSnaps = 120;
    private final int secondsBetweenSQLSnaps = 600;
    private final DateTimeFormatter ckhDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private int threadType; //0 - waits, 1 - sess stats, 2 - sys stats & sql texts, 3 - sql plans and sql stats
    private String dbUserName;
    private String dbPassword;
    private String dbConnectionString;
    private String dbUniqueName;
    private String dbHostName;
    private ComboPooledDataSource ckhDataSource;
    private Connection con;
    private PreparedStatement oraWaitsPreparedStatement;
    private PreparedStatement oraSysStatsPreparedStatement;
    private PreparedStatement oraSesStatsPreparedStatement;
    private PreparedStatement oraSQLTextsPreparedStatement;
    private PreparedStatement oraSQLPlansPreparedStatement;
    private PreparedStatement oraSQLStatsPreparedStatement;
    private PreparedStatement oraStatNamesPreparedStatement;
    private DateTimeFormatter dateFormatData;
    private long currentDateTime;
    private String currentDate;
    private StatCollectorCKH processor;
    private boolean shutdown = false;

    private final String oraSysStatQuery = "select statistic#,value from v$sysstat where value<>0";
    private final String oraWaitsQuery
            = "select "
            + "  sid,"
            + "  serial#,"
            + "  decode(taddr,null,'N','Y'),"
            + "  status,"
            + "  nvl(username,schemaname),"
            + "  nvl(osuser,'-'),"
            + "  nvl(machine,'-'),"
            + "  nvl(program,'-'),"
            + "  type,"
            + "  nvl(module,'-'),"
            + "  nvl(blocking_session,0),"
            + "  decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event), "
            + "  decode(state,'WAITED KNOWN TIME',127,'WAITED SHORT TIME',127,wait_class#),"
            + "  round(wait_time_micro/1000000,3),"
            + "  nvl(sql_id,'-'),"
            + "  nvl(sql_exec_start,to_date('19700101','YYYYMMDD')),"
            + "  sql_exec_id,"
            + "  logon_time,"
            + "  seq#,"
            + "  nvl(p1,0),"
            + "  nvl(p2,0)"
            + "  from gv$session a"
            + "  where sid<>sys_context('USERENV','SID') and ("
            + "  wait_class#<>6 or exists"
            + "  ( select 1 from gv$session b where a.inst_id=b.inst_id and (a.sid = b.blocking_session or a.sid = b.final_blocking_session) )"
            + "  )";
    private final String oraSesStatQuery
            = "select sid,sserial,statistic#,value from"
            + " (select sid,serial# sserial from v$session where type='USER' and sid<>sys_context('USERENV','SID') and ( wait_class#<>6 or (wait_class#=6 and seconds_in_wait < 10) ))"
            + " join v$sesstat using(sid)"
            + " join v$statname using(statistic#)"
            + " where name in ( "
            + "'Requests to/from client','user commits','user rollbacks','user calls','recursive calls','recursive cpu usage','DB time','session pga memory','physical read total bytes','physical write total bytes','db block changes','redo size','redo size for direct writes','table fetch by rowid','table fetch continued row','lob reads','lob writes','index fetch by key','sql area evicted','session cursor cache hits','session cursor cache count','queries parallelized','Parallel operations not downgraded','Parallel operations downgraded to serial','parse time cpu','parse count (total)','parse count (hard)','parse count (failures)','sorts (memory)','sorts (disk)'"
            + " ) "
            + " and value<>0";
    private final String oraSQLTextsQuery = "select sql_id,sql_text from v$sqlarea";
    private final String oraSQLPlansQuery = "select distinct sql_id,plan_hash_value from v$sqlarea_plan_hash where plan_hash_value<>0";
    private final String oraSQLStatsQuery = "";
    private final String oraStatNamesQuery = "select statistic#,name from v$statname";

    public StatCollector(String inputString, String dbUSN, String dbPWD, ComboPooledDataSource ckhDS, DateTimeFormatter dtFMT, int runTType) {
        dbConnectionString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
        dbUserName = dbUSN;
        dbPassword = dbPWD;
        ckhDataSource = ckhDS;
        dateFormatData = dtFMT;
        threadType = runTType;
    }

    private void setDateTimeVars() {
        currentDateTime = Instant.now().getEpochSecond();
        currentDate = LocalDate.now().format(ckhDateTimeFormatter);
    }

    private void runSessionsRoutines() throws InterruptedException {
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                setDateTimeVars();
                oraWaitsPreparedStatement.execute();
                shutdown = ! processor.processSessions(
                        oraWaitsPreparedStatement.getResultSet(),
                        currentDateTime,
                        currentDate
                );
                oraWaitsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error getting result from database " + dbConnectionString);
                shutdown = true;
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(secondsBetweenSessSnaps);
        }
    }

    private void runSessStatsRoutines() throws InterruptedException {
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                setDateTimeVars();
                oraSesStatsPreparedStatement.execute();
                shutdown = ! processor.processSesStats(
                        oraSesStatsPreparedStatement.getResultSet(),
                        currentDateTime,
                        currentDate
                );
                oraSesStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error processing session statistics!");
                shutdown = true;
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(secondsBetweenSessStatsSnaps);
        }
    }

    private void runSysRoutines() throws InterruptedException {
        int snapcounter = 0;
        long begints, endts;
        /*gather stat names once */
        //setDateTimeVars();
        try {
            oraStatNamesPreparedStatement.execute();
            shutdown = !processor.processStatNames(oraStatNamesPreparedStatement.getResultSet());
            oraStatNamesPreparedStatement.close();
        } catch (Exception e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error processing statistics names!");
            shutdown = true;
        }
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                setDateTimeVars();
                oraSysStatsPreparedStatement.execute();
                shutdown = ! processor.processSysStats(
                        oraSysStatsPreparedStatement.getResultSet(),
                        currentDateTime,
                        currentDate
                );
                oraSysStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error processing system statistics!");
                shutdown = true;
                e.printStackTrace();
            }
            if (snapcounter == 30) {
                snapcounter = 0;
                begints = System.currentTimeMillis();
                try {
                    oraSQLPlansPreparedStatement.execute();
                    shutdown = ! processor.processSQLPlans(oraSQLPlansPreparedStatement.getResultSet());
                    oraSQLPlansPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error processing sql plan hash values!");
                    shutdown = true;
                    e.printStackTrace();
                }
                if (!shutdown) {
                    try {
                        oraSQLTextsPreparedStatement.execute();
                        shutdown = ! processor.processSQLTexts(oraSQLTextsPreparedStatement.getResultSet());
                        oraSQLTextsPreparedStatement.clearWarnings();
                    } catch (SQLException e) {
                        lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error processing sql texts!");
                        shutdown = true;
                        e.printStackTrace();
                    }
                }
                endts = System.currentTimeMillis();
                if (endts - begints < secondsBetweenSysSnaps * 1000) {
                    TimeUnit.SECONDS.sleep(secondsBetweenSysSnaps - (int) ((endts - begints) / 1000));
                }
            } else {
                TimeUnit.SECONDS.sleep(secondsBetweenSysSnaps);
                snapcounter++;
            }
        }
    }

    private void runSQLRoutines() throws InterruptedException {
        int snapcounter = 0;
        long begints, endts;
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                setDateTimeVars();
                oraSQLStatsPreparedStatement.execute();
                shutdown = ! processor.processSQLStats(
                        oraSQLStatsPreparedStatement.getResultSet(),
                        currentDateTime,
                        currentDate
                );
                oraSQLStatsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error processing SQL statistics!");
                shutdown = true;
                e.printStackTrace();
            }
            if (snapcounter == 6) {
                snapcounter = 0;
                begints = System.currentTimeMillis();
                try {
                    oraSQLPlansPreparedStatement.execute();
                    shutdown = ! processor.processSQLPlans( oraSQLPlansPreparedStatement.getResultSet() );
                    oraSQLPlansPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error processing sql plans!");
                    shutdown = true;
                    e.printStackTrace();
                }
                endts = System.currentTimeMillis();
                if (endts - begints < secondsBetweenSQLSnaps * 1000) {
                    TimeUnit.SECONDS.sleep(secondsBetweenSQLSnaps - (int) ((endts - begints) / 1000));
                }
            } else {
                TimeUnit.SECONDS.sleep(secondsBetweenSQLSnaps);
                snapcounter++;
            }
        }
    }

    private void cleanup() {
        try {
            if (processor != null && processor.isAlive()) {
                processor.cleanup();
            }
            if (oraWaitsPreparedStatement != null && ! oraWaitsPreparedStatement.isClosed()) {
                oraWaitsPreparedStatement.close();
            }
            if (oraSysStatsPreparedStatement != null && ! oraSysStatsPreparedStatement.isClosed()) {
                oraSysStatsPreparedStatement.close();
            }
            if (oraSesStatsPreparedStatement != null && ! oraSesStatsPreparedStatement.isClosed()) {
                oraSesStatsPreparedStatement.close();
            }
            if (oraSQLTextsPreparedStatement != null && ! oraSQLTextsPreparedStatement.isClosed()) {
                oraSQLTextsPreparedStatement.close();
            }
            if (oraSQLPlansPreparedStatement != null && ! oraSQLPlansPreparedStatement.isClosed()) {
                oraSQLPlansPreparedStatement.close();
            }
            if (oraSQLStatsPreparedStatement != null && ! oraSQLStatsPreparedStatement.isClosed()) {
                oraSQLStatsPreparedStatement.close();
            }
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error durring ORADB resource cleanups!");
            e.printStackTrace();
        }
    }

    private void openConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + dbConnectionString, dbUserName, dbPassword);
            con.setAutoCommit(false);
            processor = new StatCollectorCKH(dbUniqueName, dbHostName, ckhDataSource, dateFormatData, threadType);
        } catch (ClassNotFoundException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load Oracle driver!");
            shutdown = true;
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot initiate connection to target Oracle database: " + dbConnectionString);
            shutdown = true;
        }
    }

    @Override
    public void run() {

        lg = new SL4JLogger();

        openConnection();

        if (!shutdown) {
            try {
                switch (threadType) {
                    case 0:
                        oraWaitsPreparedStatement = con.prepareStatement(oraWaitsQuery);
                        oraWaitsPreparedStatement.setFetchSize(1000);
                        runSessionsRoutines();
                        break;
                    case 1:
                        oraSesStatsPreparedStatement = con.prepareStatement(oraSesStatQuery);
                        oraSesStatsPreparedStatement.setFetchSize(1000);
                        runSessStatsRoutines();
                        break;
                    case 2:
                        oraSysStatsPreparedStatement = con.prepareStatement(oraSysStatQuery);
                        oraSysStatsPreparedStatement.setFetchSize(500);
                        oraSQLTextsPreparedStatement = con.prepareStatement(oraSQLTextsQuery);
                        oraSQLTextsPreparedStatement.setFetchSize(1000);
                        oraStatNamesPreparedStatement = con.prepareStatement(oraStatNamesQuery);
                        oraStatNamesPreparedStatement.setFetchSize(1000);
                        oraSQLPlansPreparedStatement = con.prepareStatement(oraSQLPlansQuery);
                        oraSQLPlansPreparedStatement.setFetchSize(1000);
                        runSysRoutines();
                        break;
                    case 3:
                        oraSQLStatsPreparedStatement = con.prepareStatement(oraSQLStatsQuery);
                        oraSQLStatsPreparedStatement.setFetchSize(500);
                        runSQLRoutines();
                        break;
                    default:
                        lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Unknown thread type provided!");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot prepare statements for  Oracle database: " + dbConnectionString);
            }
            cleanup();
        }
    }
}
