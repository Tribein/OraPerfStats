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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class StatCollectorCKH {

    private static final String CKHUSERNAME = "oracle";
    private static final String CKHPASSWORD = "elcaro";
    private static final String CKHCONNECTIONSTRING = "jdbc:clickhouse://10.64.139.57:8123/oradb";
    private String connString;
    private String dbUniqueName;
    private String dbHostName;
    private final DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private ClickHousePreparedStatement ckhSessionsPreparedStatement;
    private ClickHousePreparedStatement ckhSysStatsPreparedStatement;
    private ClickHousePreparedStatement ckhSesStatsPreparedStatement;
    private ClickHouseConnection connClickHouse;
    private final ClickHouseProperties connClickHouseProperties;
    private final String connClickHouseString;
    private final String ckhInsertSessionsQuery = "insert into sessions_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private final String ckhInsertSysStatsQuery = "insert into sysstats_buffer values (?,?,?,?,?,?)";
    private final String ckhInsertSesStatsQuery = "insert into sesstats_buffer values (?,?,?,?,?,?,?)";
    SL4JLogger lg;

    public StatCollectorCKH(String inpDBUniqename, String inpDBHostName) {
        connClickHouseString = CKHCONNECTIONSTRING;
        connClickHouseProperties = new ClickHouseProperties().withCredentials(CKHUSERNAME, CKHPASSWORD);
        dbUniqueName = inpDBUniqename;
        dbHostName = inpDBHostName;
        lg = new SL4JLogger();
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load ClickHouse driver!");
        }
        try {
            connClickHouse = new ClickHouseDriver().connect(connClickHouseString, connClickHouseProperties);
            ckhSessionsPreparedStatement = (ClickHousePreparedStatement) connClickHouse.prepareStatement(ckhInsertSessionsQuery);
            ckhSysStatsPreparedStatement = (ClickHousePreparedStatement) connClickHouse.prepareStatement(ckhInsertSysStatsQuery);
            ckhSesStatsPreparedStatement = (ClickHousePreparedStatement) connClickHouse.prepareStatement(ckhInsertSesStatsQuery);
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot connect to ClickHouse server!");
            e.printStackTrace();
        }
    }

    public boolean processSessions(ResultSet queryResult, long currentDateTime, LocalDate currentDate) throws SQLException {
        try {
            while (queryResult != null && queryResult.next() ) {
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
            }
            if(queryResult != null){
                queryResult.close();
            }
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing resultset from Database!");
            return false;
        }
        try {
            ckhSessionsPreparedStatement.executeBatch();
            ckhSessionsPreparedStatement.clearBatch();
            ckhSessionsPreparedStatement.clearWarnings();
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error submitting sessions data to ClickHouse!");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean processSysStats(ResultSet queryResult, long currentDateTime, LocalDate currentDate) {
        try {
            while (queryResult != null && queryResult.next()) {
                ckhSysStatsPreparedStatement.setString(1, dbUniqueName);
                ckhSysStatsPreparedStatement.setLong(2, currentDateTime);
                ckhSysStatsPreparedStatement.setDate(3, java.sql.Date.valueOf(currentDate));
                ckhSysStatsPreparedStatement.setString(4, queryResult.getString(1));
                ckhSysStatsPreparedStatement.setInt(5, queryResult.getInt(2));
                ckhSysStatsPreparedStatement.setLong(6,
                        (long) new BigDecimal(queryResult.getDouble(3)).setScale(0, RoundingMode.HALF_UP).doubleValue()
                );
                ckhSysStatsPreparedStatement.addBatch();
            }
            if(queryResult != null){
                queryResult.close();
            }
            ckhSysStatsPreparedStatement.executeBatch();
            ckhSysStatsPreparedStatement.clearBatch();
            ckhSysStatsPreparedStatement.clearWarnings();
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing system statistics!");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean processSesStats(ResultSet queryResult, long currentDateTime, LocalDate currentDate) {
        try {

            while (queryResult != null && queryResult.next()) {
                ckhSesStatsPreparedStatement.setString(1, dbUniqueName);
                ckhSesStatsPreparedStatement.setLong(2, currentDateTime);
                ckhSesStatsPreparedStatement.setDate(3, java.sql.Date.valueOf(currentDate));
                ckhSesStatsPreparedStatement.setInt(4, queryResult.getInt(1));
                ckhSesStatsPreparedStatement.setString(5, queryResult.getString(2));
                ckhSesStatsPreparedStatement.setInt(6, queryResult.getInt(3));
                ckhSesStatsPreparedStatement.setLong(7,
                        (long) new BigDecimal(queryResult.getDouble(4)).setScale(0, RoundingMode.HALF_UP).doubleValue()
                );
                ckhSesStatsPreparedStatement.addBatch();
            }
            if(queryResult != null){
                queryResult.close();
            }
            ckhSesStatsPreparedStatement.executeBatch();
            ckhSesStatsPreparedStatement.clearBatch();
            ckhSesStatsPreparedStatement.clearWarnings();
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error processing session statistics!");
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public boolean isAlive(){
        return true;
    }
    public void cleanup() {
        try {
            if (ckhSessionsPreparedStatement != null && !ckhSessionsPreparedStatement.isClosed()) {
                ckhSessionsPreparedStatement.close();
            }
            if (ckhSysStatsPreparedStatement != null && !ckhSysStatsPreparedStatement.isClosed()) {
                ckhSysStatsPreparedStatement.close();
            }
            if (ckhSesStatsPreparedStatement != null && !ckhSesStatsPreparedStatement.isClosed()) {
                ckhSesStatsPreparedStatement.close();
            }
            if (connClickHouse != null && !connClickHouse.isClosed()) {
                connClickHouse.close();
            }
        } catch (SQLException e) {
            lg.LogError(dateFormatData.format(LocalDateTime.now()) + "\t" + connString + "\t" + "Error durring resource cleanups!");
            e.printStackTrace();
        }
    }
}
