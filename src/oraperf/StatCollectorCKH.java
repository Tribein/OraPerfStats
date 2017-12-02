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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class StatCollectorCKH extends Thread {

    private final int secondsBetweenSnaps = 10;
    private int snapcounter = 0;
    private final String dbUserName = "dbsnmp";
    private final String dbPassword = "dbsnmp";
    private String connString;
    private String dbUniqueName;
    private String dbHostName;
    private Connection con;
    private PreparedStatement oraWaitsPreparedStatement;
    private PreparedStatement oraSysStatsPreparedStatement;
    private PreparedStatement oraSesStatsPreparedStatement;
    private final DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private long currentDateTime;
    private LocalDate currentDate;
    private ClickHousePreparedStatement ckhSessionsPreparedStatement;
    private ClickHousePreparedStatement ckhSysStatsPreparedStatement;
    private ClickHousePreparedStatement ckhSesStatsPreparedStatement;
    private ClickHouseConnection connClickHouse;
    private final ClickHouseProperties connClickHouseProperties;
    private final String connClickHouseString;
    private final String ckhInsertSessionsQuery = "insert into sessions_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private final String ckhInsertSysStatsQuery = "insert into sysstats_buffer values (?,?,?,?,?,?)";
    private final String ckhInsertSesStatsQuery = "insert into sesstats_buffer values (?,?,?,?,?,?,?)";
    private final String oraSysStatQuery = "select name,class,value from v$sysstat where value<>0";
    private final String oraWaitsQuery
            = "select "
            + "  sid,"
            + "  serial#,"
            + "  decode(taddr,null,'N','Y'),"
            + "  status,"
            + "  schemaname,"
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
            + "  nvl(sql_exec_start,to_date('19700101','YYYYMMDD'),"
            + "  sql_exec_id,"
            + "  logon_time,"
            + "  seq#,"
            + "  nvl(p1,0),"
            + "  nvl(p2,0)"
            + " from gv$session"
            + " where (wait_class#<>6 or taddr is not null) and sid<>sys_context('USERENV','SID')";
    private final String oraSesStatQuery 
            = "select sid,name,value from v$statname " +
            "join v$sesstat using(statistic#) " +
            "join v$session using(sid) " +
            "where name in ( " +
            "'Requests to/from client','user commits','user rollbacks','user calls','recursive calls','recursive cpu usage','DB time','session pga memory','physical read total bytes','physical write total bytes','db block changes','redo size','redo size for direct writes','table fetch by rowid','table fetch continued row','lob reads','lob writes','index fetch by key','sql area evicted','session cursor cache hits','session cursor cache count','queries parallelized','Parallel operations not downgraded','Parallel operations downgraded to serial','parse time cpu','parse count (total)','parse count (hard)','parse count (failures)','sorts (memory)','sorts (disk)'\n" +
            ") " +
            "and value<>0 " +
            "and type='USER' " +
            "and not ( wait_class#=6 and wait_time_micro>60*1000000) " +
            "and sid<>sys_context('USERENV','SID') ";
    boolean shutdown = false;

    public StatCollectorCKH(String inputString, String ckhConnectionString, String ckhUsername, String ckhPassword) {
        connString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
        connClickHouseString = ckhConnectionString;
        connClickHouseProperties = new ClickHouseProperties().withCredentials(ckhUsername, ckhPassword);
    }

    @Override
    public void run() {
        ResultSet queryResult;
        SL4JLogger lg = new SL4JLogger();
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load Oracle driver!");
            shutdown = true;
        }
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load ClickHouse driver!");
            shutdown = true;
        }
        try{
            connClickHouse = new ClickHouseDriver().connect(connClickHouseString, connClickHouseProperties);
            ckhSessionsPreparedStatement = (ClickHousePreparedStatement) connClickHouse.prepareStatement(ckhInsertSessionsQuery);
            ckhSysStatsPreparedStatement = (ClickHousePreparedStatement) connClickHouse.prepareStatement(ckhInsertSysStatsQuery);
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot connect to ClickHouse server!");
            shutdown = true;
            e.printStackTrace();
        }
        try {
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + connString, dbUserName, dbPassword);
            oraWaitsPreparedStatement = con.prepareStatement(oraWaitsQuery);
            oraSysStatsPreparedStatement = con.prepareStatement(oraSysStatQuery);
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot initiate connection to target Oracle database: " + connString);
            shutdown = true;
        }
        queryResult = null;
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                oraWaitsPreparedStatement.execute();
                queryResult = oraWaitsPreparedStatement.getResultSet();
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error getting result from database " + connString);
                shutdown = true;
                e.printStackTrace();
            }
            if (!shutdown) {
                currentDateTime = Instant.now().getEpochSecond();
                currentDate = LocalDate.now();
            }
            try {
                while (queryResult != null && queryResult.next() && !shutdown) {
                    //--
                    ckhSessionsPreparedStatement.setString(1, dbUniqueName);
                    ckhSessionsPreparedStatement.setString(2, dbHostName);
                    ckhSessionsPreparedStatement.setLong(3, currentDateTime);
                    ckhSessionsPreparedStatement.setInt(4, queryResult.getInt(1));
                    ckhSessionsPreparedStatement.setInt(5, queryResult.getInt(2));
                    ckhSessionsPreparedStatement.setString(6, queryResult.getString(3));
                    ckhSessionsPreparedStatement.setString(7, queryResult.getString(4).substring(0, 1));
                    ckhSessionsPreparedStatement.setString(8, queryResult.getString(5));
                    ckhSessionsPreparedStatement.setString(9, queryResult.getString(6));
                    ckhSessionsPreparedStatement.setString(10, queryResult.getString(7));
                    ckhSessionsPreparedStatement.setString(11, queryResult.getString(8));
                    ckhSessionsPreparedStatement.setString(12, queryResult.getString(9).substring(0, 1));
                    ckhSessionsPreparedStatement.setString(13, queryResult.getString(10));
                    ckhSessionsPreparedStatement.setInt(14, queryResult.getInt(11));
                    ckhSessionsPreparedStatement.setString(15, queryResult.getString(12));
                    ckhSessionsPreparedStatement.setLong(16, queryResult.getLong(13));
                    ckhSessionsPreparedStatement.setFloat(17, queryResult.getFloat(14));
                    ckhSessionsPreparedStatement.setString(18, queryResult.getString(15));
                    ckhSessionsPreparedStatement.setLong(19, ((java.util.Date) queryResult.getTimestamp(16)).getTime() / 1000);
                    ckhSessionsPreparedStatement.setInt(20, queryResult.getInt(17));
                    ckhSessionsPreparedStatement.setLong(21, ((java.util.Date) queryResult.getTimestamp(18)).getTime() / 1000);
                    ckhSessionsPreparedStatement.setInt(22, queryResult.getInt(19));
                    ckhSessionsPreparedStatement.setDate(23, java.sql.Date.valueOf(currentDate));
                    ckhSessionsPreparedStatement.setLong(24, queryResult.getLong(20));
                    ckhSessionsPreparedStatement.setLong(25, queryResult.getLong(21));
                    ckhSessionsPreparedStatement.addBatch();
                    //--
                }
                if (!shutdown) {
                    if (queryResult != null) {
                        queryResult.close();
                    }
                    oraWaitsPreparedStatement.clearWarnings();
                }
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing resultset from Database!");
                shutdown = true;
            }
            if (!shutdown) {
                try {
                    ckhSessionsPreparedStatement.executeBatch();
                    ckhSessionsPreparedStatement.clearBatch();
                    ckhSessionsPreparedStatement.clearWarnings();
                    
                } catch (SQLException e) {
                    lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error submitting sessions data to ClickHouse!");
                    shutdown = true;
                    e.printStackTrace();
                }
            }
            if (!shutdown) {
                try {
                    TimeUnit.SECONDS.sleep(secondsBetweenSnaps);
                    snapcounter++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (snapcounter == 1 ){
                    try {
                        oraSysStatsPreparedStatement.execute();
                        queryResult = oraSysStatsPreparedStatement.getResultSet();
                        currentDateTime = Instant.now().getEpochSecond();
                        currentDate = LocalDate.now();                                                
                        while (!shutdown && queryResult != null && queryResult.next() ) {
                            ckhSysStatsPreparedStatement.setString(1, dbUniqueName);
                            ckhSysStatsPreparedStatement.setLong(2, currentDateTime);
                            ckhSysStatsPreparedStatement.setDate(3, java.sql.Date.valueOf(currentDate));
                            ckhSysStatsPreparedStatement.setString(4, queryResult.getString(1));
                            ckhSysStatsPreparedStatement.setInt(5, queryResult.getInt(2));
                            ckhSysStatsPreparedStatement.setLong(6, (long) new BigDecimal(queryResult.getDouble(3)).setScale(0, RoundingMode.HALF_UP).doubleValue());
                            ckhSysStatsPreparedStatement.addBatch();
                        }
                        if (queryResult != null) {
                            queryResult.close();
                        }
                        ckhSysStatsPreparedStatement.executeBatch();
                        ckhSysStatsPreparedStatement.clearBatch();
                        ckhSysStatsPreparedStatement.clearWarnings();
                        oraSysStatsPreparedStatement.clearWarnings(); 
                        snapcounter = 0;
                    } catch (SQLException e) {
                        lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing system statistics!");
                        shutdown = true;
                        e.printStackTrace();
                    }
                }                                
            }

        }
        try {
            if (oraWaitsPreparedStatement != null && !oraWaitsPreparedStatement.isClosed()) {
                oraWaitsPreparedStatement.close();
            }
            if (oraSysStatsPreparedStatement != null && !oraSysStatsPreparedStatement.isClosed()) {
                oraSysStatsPreparedStatement.close();
            }            
            if (ckhSessionsPreparedStatement != null && !ckhSessionsPreparedStatement.isClosed()) {
                ckhSessionsPreparedStatement.close();
            }
            if (ckhSysStatsPreparedStatement != null && !ckhSysStatsPreparedStatement.isClosed()) {
                ckhSysStatsPreparedStatement.close();
            }            
            if (con != null && !con.isClosed()) {
                con.close();
            }
            if (connClickHouse != null && !connClickHouse.isClosed()) {
                connClickHouse.close();
            }
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString+ "\t" + "Error durring resource cleanups!");
            e.printStackTrace();
        }
    }
}
