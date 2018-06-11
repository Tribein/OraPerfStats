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

    private final int secondsBetweenSessSnaps = 10;
    private final int secondsBetweenSessStatsSnaps = 30;
    private final int secondsBetweenSysSnaps = 120;
    private int threadType; //0 -waits, 1 - sess stats, 2 - sys stats & sql texts
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
    private DateTimeFormatter dateFormatData;
    private final DateTimeFormatter ckhDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private long currentDateTime;
    private String currentDate;
    private StatCollectorCKH porcessor;
    private final String oraSysStatQuery = "select name,class,value from v$sysstat where value<>0";
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
            = "select sid,name,class,value, sserial from"
            + " (select sid,serial# sserial from v$session where type='USER' and sid<>sys_context('USERENV','SID') and ( wait_class#<>6 or (wait_class#=6 and seconds_in_wait < 10) ))"
            + " join v$sesstat using(sid)"
            + " join v$statname using(statistic#)"
            + " where name in ( "
            + "'Requests to/from client','user commits','user rollbacks','user calls','recursive calls','recursive cpu usage','DB time','session pga memory','physical read total bytes','physical write total bytes','db block changes','redo size','redo size for direct writes','table fetch by rowid','table fetch continued row','lob reads','lob writes','index fetch by key','sql area evicted','session cursor cache hits','session cursor cache count','queries parallelized','Parallel operations not downgraded','Parallel operations downgraded to serial','parse time cpu','parse count (total)','parse count (hard)','parse count (failures)','sorts (memory)','sorts (disk)'"
            + " ) "
            + " and value<>0";
    private final String oraSQLTextsQuery = "select sql_id,sql_text from v$sqlarea";
    boolean shutdown = false;
    SL4JLogger lg;

    public StatCollector(String inputString, String dbUSN, String dbPWD, ComboPooledDataSource ckhDS, DateTimeFormatter dtFMT, int runTType) {
        dbConnectionString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
        dbUserName = dbUSN;
        dbPassword = dbPWD;
        ckhDataSource = ckhDS;
        dateFormatData = dtFMT;
        threadType = runTType;

        lg = new SL4JLogger();

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load Oracle driver!");
            shutdown = true;
        }
        try {
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + dbConnectionString, dbUserName, dbPassword);
            con.setAutoCommit(false);
            oraWaitsPreparedStatement = con.prepareStatement(oraWaitsQuery);
            oraWaitsPreparedStatement.setFetchSize(1000);
            oraSysStatsPreparedStatement = con.prepareStatement(oraSysStatQuery);
            oraSysStatsPreparedStatement.setFetchSize(1000);
            oraSesStatsPreparedStatement = con.prepareStatement(oraSesStatQuery);
            oraSesStatsPreparedStatement.setFetchSize(1000);
            oraSQLTextsPreparedStatement = con.prepareStatement(oraSQLTextsQuery);
            oraSesStatsPreparedStatement.setFetchSize(2000);

            porcessor = new StatCollectorCKH(dbUniqueName, dbHostName, ckhDataSource, dateFormatData);
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot initiate connection to target Oracle database: " + dbConnectionString);
            shutdown = true;
        }
    }

    private void setDateTimeVars(){
                currentDateTime = Instant.now().getEpochSecond();
                currentDate = LocalDate.now().format(ckhDateTimeFormatter);        
    }
    private void runSessionsRoutines() throws InterruptedException {
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                setDateTimeVars();
                oraWaitsPreparedStatement.execute();
                shutdown = !porcessor.processSessions(
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
                shutdown = !porcessor.processSesStats(
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
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                setDateTimeVars();                
                oraSysStatsPreparedStatement.execute();
                shutdown = !porcessor.processSysStats(
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
            if (snapcounter == 15) {
                snapcounter = 0;
                begints = System.currentTimeMillis();
                try {
                    oraSQLTextsPreparedStatement.execute();
                    shutdown = !porcessor.processSQLTexts(
                            oraSQLTextsPreparedStatement.getResultSet(),
                            currentDateTime,
                            currentDate
                    );
                    oraSQLTextsPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error processing sql texts!");
                    shutdown = true;
                    e.printStackTrace();
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

    private void cleanup(){
         try {
            if (porcessor.isAlive()) {
                porcessor.cleanup();
            }
            if (oraWaitsPreparedStatement != null && !oraWaitsPreparedStatement.isClosed()) {
                oraWaitsPreparedStatement.close();
            }
            if (oraSysStatsPreparedStatement != null && !oraSysStatsPreparedStatement.isClosed()) {
                oraSysStatsPreparedStatement.close();
            }
            if (oraSesStatsPreparedStatement != null && !oraSesStatsPreparedStatement.isClosed()) {
                oraSesStatsPreparedStatement.close();
            }
            if (oraSQLTextsPreparedStatement != null && !oraSQLTextsPreparedStatement.isClosed()) {
                oraSQLTextsPreparedStatement.close();
            }
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Error durring ORADB resource cleanups!");
            e.printStackTrace();
        }       
    }
    
    @Override
    public void run() {
        try {
            switch (threadType) {
                case 0:
                    runSessionsRoutines();
                    break;
                case 1:
                    runSessStatsRoutines();
                    break;
                case 2:
                    runSysRoutines();
                    break;
                default:
                    lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + dbConnectionString + "\t" + "Unknown thread type provided!");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cleanup();
    }
}
