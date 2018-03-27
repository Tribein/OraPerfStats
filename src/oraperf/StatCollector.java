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

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class StatCollector extends Thread {

    private final int secondsBetweenSnaps = 10;
    private int snapcounter = 0;
    private int sqlcounter = 0;
    private final String dbUserName = "dbsnmp";
    private final String dbPassword = "dbsnmp";
    private String connString;
    private String dbUniqueName;
    private String dbHostName;
    private Connection con;
    private PreparedStatement oraWaitsPreparedStatement;
    private PreparedStatement oraSysStatsPreparedStatement;
    private PreparedStatement oraSesStatsPreparedStatement;
    private PreparedStatement oraSQLTextsPreparedStatement;
    private final DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private long currentDateTime;
    private LocalDate currentDate;
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
    long begints,endts;

    public StatCollector(String inputString) {
        connString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
    }

    @Override
    public void run() {
        SL4JLogger lg = new SL4JLogger();
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load Oracle driver!");
            shutdown = true;
        }
        try {
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + connString, dbUserName, dbPassword);
            con.setAutoCommit(false);
            oraWaitsPreparedStatement = con.prepareStatement(oraWaitsQuery);
            oraWaitsPreparedStatement.setFetchSize(1000);
            oraSysStatsPreparedStatement = con.prepareStatement(oraSysStatQuery);
            oraSysStatsPreparedStatement.setFetchSize(1000);
            oraSesStatsPreparedStatement = con.prepareStatement(oraSesStatQuery);
            oraSesStatsPreparedStatement.setFetchSize(1000);
            oraSQLTextsPreparedStatement = con.prepareStatement(oraSQLTextsQuery);
            oraSesStatsPreparedStatement.setFetchSize(2000);
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot initiate connection to target Oracle database: " + connString);
            shutdown = true;
        }
        StatCollectorCKH porcessor = new StatCollectorCKH(dbUniqueName, dbHostName);
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                currentDateTime = Instant.now().getEpochSecond();
                currentDate = LocalDate.now();
                oraWaitsPreparedStatement.execute();
                shutdown = !porcessor.processSessions(
                        oraWaitsPreparedStatement.getResultSet(),
                        currentDateTime,
                        currentDate
                );
                oraWaitsPreparedStatement.clearWarnings();
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error getting result from database " + connString);
                shutdown = true;
                e.printStackTrace();
            }
            begints = System.currentTimeMillis();
            if ((snapcounter == 6 || snapcounter == 12 || snapcounter == 18 || snapcounter == 24 || snapcounter == 30)
                    && !shutdown) {
                try {
                    oraSesStatsPreparedStatement.execute();
                    shutdown = !porcessor.processSesStats(
                            oraSesStatsPreparedStatement.getResultSet(),
                            currentDateTime,
                            currentDate
                    );
                    oraSesStatsPreparedStatement.clearWarnings();
                } catch (SQLException e) {
                    lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing session statistics!");
                    shutdown = true;
                    e.printStackTrace();
                }
            }
            if (snapcounter == 30 && !shutdown) {
                try {
                    oraSysStatsPreparedStatement.execute();
                    shutdown = !porcessor.processSysStats(
                            oraSysStatsPreparedStatement.getResultSet(),
                            currentDateTime,
                            currentDate
                    );
                    oraSysStatsPreparedStatement.clearWarnings();
                    snapcounter = 0;
                } catch (SQLException e) {
                    lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing system statistics!");
                    shutdown = true;
                    e.printStackTrace();
                }
            }
            endts = System.currentTimeMillis();
            if (!shutdown) {
                sqlcounter++;
                try {
                    if (sqlcounter == 180) {
                        sqlcounter = 0;
                        try {
                            oraSQLTextsPreparedStatement.execute();
                            shutdown = !porcessor.processSQLTexts(
                                    oraSQLTextsPreparedStatement.getResultSet(),
                                    currentDateTime,
                                    currentDate
                            );
                            oraSQLTextsPreparedStatement.clearWarnings();
                        } catch (SQLException e) {
                            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing sql texts!");
                            shutdown = true;
                            e.printStackTrace();
                        }

                    }else{
                        //if (endts-begints < 10000){
                            TimeUnit.SECONDS.sleep(secondsBetweenSnaps /*- Math.toIntExact((endts-begints)/1000) */);
                        //}
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }/*
                catch (ArithmeticException e){
                    e.printStackTrace();
                }*/
                snapcounter++;
            }

        }
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
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error durring resource cleanups!");
            e.printStackTrace();
        }
    }
}
