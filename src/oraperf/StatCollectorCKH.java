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
    private PreparedStatement waitsPreparedStatement;
    private PreparedStatement statsPreparedStatement;
    private final DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private long currentDateTime;
    private LocalDate currentDate;
    private ClickHousePreparedStatement sessionsPreparedStatement;
    private ClickHousePreparedStatement sysstatsPreparedStatement;
    private ClickHouseConnection connClickHouse;
    private final ClickHouseProperties connClickHouseProperties;
    private final String connClickHouseString;
    private final String insertSessionsQuery = "insert into sessions_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private final String insertSysStatsQuery = "insert into sysstats_buffer values (?,?,?,?,?,?)";
    private final String sysstatQuery = "select name,class,value from v$sysstat where value<>0";
    private final String waitsQuery
            = "SELECT "
            + "  sid,"
            + "  serial#,"
            + "  decode(taddr,NULL,'N','Y'),"
            + "  status,"
            + "  schemaname,"
            + "  nvl(osuser,'-'),"
            + "  nvl(machine,'-'),"
            + "  nvl(program,'-'),"
            + "  TYPE,"
            + "  nvl(MODULE,'-'),"
            + "  nvl(blocking_session,0),"
            + "  Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event), "
            + "  Decode(state,'WAITED KNOWN TIME',127,'WAITED SHORT TIME',127,wait_class#),"
            + "  round(wait_time_micro/1000000,3),"
            + "  nvl(sql_id,'-'),"
            + "  nvl(sql_exec_start,sysdate - interval '360' month),"
            + "  sql_exec_id,"
            + "  logon_time,"
            + "  seq#,"
            + "  nvl(p1,0),"
            + "  nvl(p2,0)"
            + " FROM gv$session"
            + " WHERE wait_class#<>6 OR taddr IS NOT null";

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
            sessionsPreparedStatement = (ClickHousePreparedStatement) connClickHouse.prepareStatement(insertSessionsQuery);
            sysstatsPreparedStatement = (ClickHousePreparedStatement) connClickHouse.prepareStatement(insertSysStatsQuery);
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot connect to ClickHouse server!");
            shutdown = true;
            e.printStackTrace();
        }
        try {
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + connString, dbUserName, dbPassword);
            waitsPreparedStatement = con.prepareStatement(waitsQuery);
            statsPreparedStatement = con.prepareStatement(sysstatQuery);
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot initiate connection to target Oracle database: " + connString);
            shutdown = true;
        }
        queryResult = null;
        while (!shutdown) /*for (int i = 0; i < 1; i++)*/ {
            try {
                waitsPreparedStatement.execute();
                queryResult = waitsPreparedStatement.getResultSet();
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
                    sessionsPreparedStatement.setString(1, dbUniqueName);
                    sessionsPreparedStatement.setString(2, dbHostName);
                    sessionsPreparedStatement.setLong(3, currentDateTime);
                    sessionsPreparedStatement.setInt(4, queryResult.getInt(1));
                    sessionsPreparedStatement.setInt(5, queryResult.getInt(2));
                    sessionsPreparedStatement.setString(6, queryResult.getString(3));
                    sessionsPreparedStatement.setString(7, queryResult.getString(4).substring(0, 1));
                    sessionsPreparedStatement.setString(8, queryResult.getString(5));
                    sessionsPreparedStatement.setString(9, queryResult.getString(6));
                    sessionsPreparedStatement.setString(10, queryResult.getString(7));
                    sessionsPreparedStatement.setString(11, queryResult.getString(8));
                    sessionsPreparedStatement.setString(12, queryResult.getString(9).substring(0, 1));
                    sessionsPreparedStatement.setString(13, queryResult.getString(10));
                    sessionsPreparedStatement.setInt(14, queryResult.getInt(11));
                    sessionsPreparedStatement.setString(15, queryResult.getString(12));
                    sessionsPreparedStatement.setLong(16, queryResult.getLong(13));
                    sessionsPreparedStatement.setFloat(17, queryResult.getFloat(14));
                    sessionsPreparedStatement.setString(18, queryResult.getString(15));
                    sessionsPreparedStatement.setLong(19, ((java.util.Date) queryResult.getTimestamp(16)).getTime() / 1000);
                    sessionsPreparedStatement.setInt(20, queryResult.getInt(17));
                    sessionsPreparedStatement.setLong(21, ((java.util.Date) queryResult.getTimestamp(18)).getTime() / 1000);
                    sessionsPreparedStatement.setInt(22, queryResult.getInt(19));
                    sessionsPreparedStatement.setDate(23, java.sql.Date.valueOf(currentDate));
                    sessionsPreparedStatement.setLong(24, queryResult.getLong(20));
                    sessionsPreparedStatement.setLong(25, queryResult.getLong(21));
                    sessionsPreparedStatement.addBatch();
                    //--
                }
                if (!shutdown) {
                    if (queryResult != null) {
                        queryResult.close();
                    }
                    waitsPreparedStatement.clearWarnings();
                }
            } catch (SQLException e) {
                lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing resultset from Database!");
                shutdown = true;
            }
            if (!shutdown) {
                try {
                    sessionsPreparedStatement.executeBatch();
                    sessionsPreparedStatement.clearBatch();
                    sessionsPreparedStatement.clearWarnings();
                    
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
                if (snapcounter == 6 ){
                    try {
                        statsPreparedStatement.execute();
                        queryResult = statsPreparedStatement.getResultSet();
                        currentDateTime = Instant.now().getEpochSecond();
                        currentDate = LocalDate.now();                                                
                        while (!shutdown && queryResult != null && queryResult.next() ) {
                            sysstatsPreparedStatement.setString(1, dbUniqueName);
                            sysstatsPreparedStatement.setLong(2, currentDateTime);
                            sysstatsPreparedStatement.setDate(3, java.sql.Date.valueOf(currentDate));
                            sysstatsPreparedStatement.setString(4, queryResult.getString(1));
                            sysstatsPreparedStatement.setInt(5, queryResult.getInt(2));
                            sysstatsPreparedStatement.setLong(6, (long) new BigDecimal(queryResult.getDouble(3)).setScale(0, RoundingMode.HALF_UP).doubleValue());
                            sysstatsPreparedStatement.addBatch();
                        }
                        if (queryResult != null) {
                            queryResult.close();
                        }
                        sysstatsPreparedStatement.executeBatch();
                        sysstatsPreparedStatement.clearBatch();
                        sysstatsPreparedStatement.clearWarnings();
                        statsPreparedStatement.clearWarnings(); 
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
            if (waitsPreparedStatement != null && !waitsPreparedStatement.isClosed()) {
                waitsPreparedStatement.close();
            }
            if (statsPreparedStatement != null && !statsPreparedStatement.isClosed()) {
                statsPreparedStatement.close();
            }            
            if (sessionsPreparedStatement != null && !sessionsPreparedStatement.isClosed()) {
                sessionsPreparedStatement.close();
            }
            if (sysstatsPreparedStatement != null && !sysstatsPreparedStatement.isClosed()) {
                sysstatsPreparedStatement.close();
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
