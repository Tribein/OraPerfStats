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
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StatProcessorCKH {

    SL4JLogger lg;
    
    private String dbUniqueName;
    private String dbHostName;
    private PreparedStatement ckhSessionsPreparedStatement;
    private PreparedStatement ckhSysStatsPreparedStatement;
    private PreparedStatement ckhSesStatsPreparedStatement;
    private PreparedStatement ckhSQLTextsPreparedStatement;
    private PreparedStatement ckhSQLPlansPreparedStatement;
    private PreparedStatement ckhSQLStatsPreparedStatement;
    private PreparedStatement ckhStatNamesPreparedStatement;
    private PreparedStatement ckhIOFileStatsPreparedStatement;
    private PreparedStatement ckhIOFunctionStatsPreparedStatement;
    private Connection connClickHouse;
    private ComboPooledDataSource ckhDataSource;
    
    private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
    
    private final String ckhInsertSessionsQuery = "insert into sessions_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private final String ckhInsertSysStatsQuery = "insert into sysstats_buffer values (?,?,?,?)";
    private final String ckhInsertSesStatsQuery = "insert into sesstats_buffer values (?,?,?,?,?,?)";
    private final String ckhInsertSQLTextsQuery = "insert into sqltexts_buffer values (?,?)";
    private final String ckhInsertSQLPlansQuery = "insert into sqlplans_buffer values (?,?)";
    private final String ckhInsertSQLStatsQuery = "insert into sqlstats_buffer values ()";
    private final String ckhInsertStatNamesQuery = "insert into statnames_buffer values (?,?,?)";
    private final String ckhInsertIOFileStatsQuery = "insert into iofilestats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private final String ckhInsertIOFunctionStatsQuery = "insert into iofunctionstats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    
    

    public StatProcessorCKH(String inpDBUniqename, String inpDBHostName, ComboPooledDataSource ckhDS, int threadType) {
        dbUniqueName                = inpDBUniqename;
        dbHostName                  = inpDBHostName;
        ckhDataSource               = ckhDS;
        
        lg = new SL4JLogger();
        
        try {
            connClickHouse = ckhDataSource.getConnection();
            switch(threadType){
                case 0:
                    ckhSessionsPreparedStatement = connClickHouse.prepareStatement(ckhInsertSessionsQuery);
                    ckhIOFileStatsPreparedStatement = connClickHouse.prepareStatement(ckhInsertIOFileStatsQuery);
                    ckhIOFunctionStatsPreparedStatement = connClickHouse.prepareStatement(ckhInsertIOFunctionStatsQuery);
                break;
                case 1:
                    ckhSesStatsPreparedStatement = connClickHouse.prepareStatement(ckhInsertSesStatsQuery);
                break;
                case 2:
                    ckhSysStatsPreparedStatement = connClickHouse.prepareStatement(ckhInsertSysStatsQuery);
                    ckhSQLTextsPreparedStatement = connClickHouse.prepareStatement(ckhInsertSQLTextsQuery);
                    ckhStatNamesPreparedStatement = connClickHouse.prepareStatement(ckhInsertStatNamesQuery);
                    ckhSQLPlansPreparedStatement = connClickHouse.prepareStatement(ckhInsertSQLPlansQuery);
                break;
                case 3:
                    ckhSQLStatsPreparedStatement = connClickHouse.prepareStatement(ckhInsertSQLStatsQuery);                    
                break;
                default:
                    lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                            "Unsupported run type!"
                    );
                    
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                    "Cannot connect to ClickHouse server!"
            );
            e.printStackTrace();
        }
    }
    private boolean handleSysStatsConnection (){
        try{
            if (connClickHouse == null || connClickHouse.isClosed()){
                    connClickHouse = ckhDataSource.getConnection();
                    ckhSysStatsPreparedStatement = connClickHouse.prepareStatement(ckhInsertSysStatsQuery);
            }
            return true;
        }catch(Exception e){
            return false;
        }
    }
    private boolean handleSQLTextsConnection (){
        try{
            if (connClickHouse == null || connClickHouse.isClosed()){
                    connClickHouse = ckhDataSource.getConnection();
                    ckhSQLTextsPreparedStatement = connClickHouse.prepareStatement(ckhInsertSQLTextsQuery);
            }
            return true;
        }catch(Exception e){
            return false;
        }
    }  
    private boolean handleSQLPlansConnection (){
        try{
            if (connClickHouse == null || connClickHouse.isClosed()){
                    connClickHouse = ckhDataSource.getConnection();
                    ckhSQLPlansPreparedStatement = connClickHouse.prepareStatement(ckhInsertSQLPlansQuery);
            }
            return true;
        }catch(Exception e){
            return false;
        }
    }      
    private boolean handleSesStatsConnection (){
        try{
            if (connClickHouse == null || connClickHouse.isClosed()){
                    connClickHouse = ckhDataSource.getConnection();
                    ckhSesStatsPreparedStatement = connClickHouse.prepareStatement(ckhInsertSesStatsQuery);
            }
            return true;
        }catch(Exception e){
            return false;
        }
    }
    public boolean processIOFileStats (ResultSet queryResult, long currentDateTime) throws SQLException {
        try{
            while (queryResult != null && queryResult.next()){
                ckhIOFileStatsPreparedStatement.setString(1, dbUniqueName);
                ckhIOFileStatsPreparedStatement.setString(2, dbHostName);
                ckhIOFileStatsPreparedStatement.setLong(3, currentDateTime);
                ckhIOFileStatsPreparedStatement.setString(4,queryResult.getString(1) );
                ckhIOFileStatsPreparedStatement.setString(5, queryResult.getString(2) );
                ckhIOFileStatsPreparedStatement.setLong(6, new BigDecimal(queryResult.getDouble(3)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(7, new BigDecimal(queryResult.getDouble(4)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(8, new BigDecimal(queryResult.getDouble(5)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(9, new BigDecimal(queryResult.getDouble(6)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(10, new BigDecimal(queryResult.getDouble(7)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(11, new BigDecimal(queryResult.getDouble(8)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(12, new BigDecimal(queryResult.getDouble(9)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(13, new BigDecimal(queryResult.getDouble(10)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(14, new BigDecimal(queryResult.getDouble(11)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(15, new BigDecimal(queryResult.getDouble(12)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(16, new BigDecimal(queryResult.getDouble(13)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(17, new BigDecimal(queryResult.getDouble(14)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(18, new BigDecimal(queryResult.getDouble(15)).longValue());
                ckhIOFileStatsPreparedStatement.setLong(19, new BigDecimal(queryResult.getDouble(16)).longValue());
                ckhIOFileStatsPreparedStatement.addBatch();                  
            }
            if(queryResult != null){
                queryResult.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error processing io file stats resultset from database!"
            );
            e.printStackTrace();
            return false;
        }
        try {
            ckhIOFileStatsPreparedStatement.executeBatch();
            ckhIOFileStatsPreparedStatement.clearBatch();
            ckhIOFileStatsPreparedStatement.clearWarnings();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error submitting io file stats data to ClickHouse!"
            );
            e.printStackTrace();
            return false;
        }        
        return true;
    }
    public boolean processIOFunctionStats (ResultSet queryResult, long currentDateTime) throws SQLException {
        try{
            while (queryResult != null && queryResult.next()){
                ckhIOFunctionStatsPreparedStatement.setString(1, dbUniqueName);
                ckhIOFunctionStatsPreparedStatement.setString(2, dbHostName);
                ckhIOFunctionStatsPreparedStatement.setLong(3, currentDateTime);
                ckhIOFunctionStatsPreparedStatement.setString(4,queryResult.getString(1) );
                ckhIOFunctionStatsPreparedStatement.setString(5, queryResult.getString(2) );
                ckhIOFunctionStatsPreparedStatement.setLong(6, new BigDecimal(queryResult.getDouble(3)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(7, new BigDecimal(queryResult.getDouble(4)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(8, new BigDecimal(queryResult.getDouble(5)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(9, new BigDecimal(queryResult.getDouble(6)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(10, new BigDecimal(queryResult.getDouble(7)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(11, new BigDecimal(queryResult.getDouble(8)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(12, new BigDecimal(queryResult.getDouble(9)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(13, new BigDecimal(queryResult.getDouble(10)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(14, new BigDecimal(queryResult.getDouble(11)).longValue());
                ckhIOFunctionStatsPreparedStatement.setLong(15, new BigDecimal(queryResult.getDouble(12)).longValue());
                ckhIOFunctionStatsPreparedStatement.addBatch();                
            }
            if(queryResult != null){
                queryResult.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error processing io function stats resultset from database!"
            );
            e.printStackTrace();
            return false;
        }
        try {
            ckhIOFunctionStatsPreparedStatement.executeBatch();
            ckhIOFunctionStatsPreparedStatement.clearBatch();
            ckhIOFunctionStatsPreparedStatement.clearWarnings();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error submitting io function stats data to ClickHouse!"
            );
            e.printStackTrace();
            return false;
        }           
        return true;        
    }    
    public boolean processSessions(ResultSet queryResult, long currentDateTime) throws SQLException {
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
                ckhSessionsPreparedStatement.setLong(23, 
                        (long) new BigDecimal(queryResult.getDouble(20)).setScale(0, RoundingMode.HALF_UP).doubleValue()
                );
                ckhSessionsPreparedStatement.setLong(24, queryResult.getLong(21));
                ckhSessionsPreparedStatement.addBatch();
            }
            if(queryResult != null){
                queryResult.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error processing resultset from Database!"
            );
            e.printStackTrace();
            return false;
        }
        try {
            ckhSessionsPreparedStatement.executeBatch();
            ckhSessionsPreparedStatement.clearBatch();
            ckhSessionsPreparedStatement.clearWarnings();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error submitting sessions data to ClickHouse!"
            );
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean processSysStats(ResultSet queryResult, long currentDateTime) {
        if(!handleSysStatsConnection()){
            return false;
        }
        try {
            while (queryResult != null && queryResult.next()) {
                ckhSysStatsPreparedStatement.setString(1, dbUniqueName);
                ckhSysStatsPreparedStatement.setLong(2, currentDateTime);
                ckhSysStatsPreparedStatement.setInt(3, queryResult.getInt(1));
                ckhSysStatsPreparedStatement.setLong(4,
                        (long) new BigDecimal(queryResult.getDouble(2)).setScale(0, RoundingMode.HALF_UP).doubleValue()
                );
                ckhSysStatsPreparedStatement.addBatch();
            }
            if(queryResult != null){
                queryResult.close();
            }
            ckhSysStatsPreparedStatement.executeBatch();
            ckhSysStatsPreparedStatement.clearBatch();
            ckhSysStatsPreparedStatement.clearWarnings();
            ckhSysStatsPreparedStatement.close();
            connClickHouse.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName +"\t"+
                    "Error processing system statistics!"
            );
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean processSesStats(ResultSet queryResult, long currentDateTime) {
        if (! handleSesStatsConnection()){
            return false;
        }
        try {

            while (queryResult != null && queryResult.next()) {
                ckhSesStatsPreparedStatement.setString(1, dbUniqueName);
                ckhSesStatsPreparedStatement.setLong(2, currentDateTime);
                ckhSesStatsPreparedStatement.setInt(3, queryResult.getInt(1));
                ckhSesStatsPreparedStatement.setInt(4, queryResult.getInt(2));
                ckhSesStatsPreparedStatement.setInt(5, queryResult.getInt(3));
                ckhSesStatsPreparedStatement.setLong(6,
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
            ckhSesStatsPreparedStatement.close();
            connClickHouse.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error processing session statistics!"
            );
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
    public boolean processSQLTexts (ResultSet queryResult) {
        if(! handleSQLTextsConnection()){
            return false;
        }
        try{
            while (queryResult != null && queryResult.next()) {
                ckhSQLTextsPreparedStatement.setString(1, queryResult.getString(1));
                ckhSQLTextsPreparedStatement.setString(2, queryResult.getString(2));
                ckhSQLTextsPreparedStatement.addBatch();
            }
           if(queryResult != null){
                queryResult.close();
            }            
            ckhSQLTextsPreparedStatement.executeBatch();
            ckhSQLTextsPreparedStatement.clearBatch();
            ckhSQLTextsPreparedStatement.clearWarnings();
            ckhSQLTextsPreparedStatement.close();
            connClickHouse.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error processing sql texts!"
            );
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
    public boolean processStatNames(ResultSet queryResult){
        try{
            while (queryResult != null && queryResult.next()) {
                ckhStatNamesPreparedStatement.setString(1, dbUniqueName);
                ckhStatNamesPreparedStatement.setString(2, queryResult.getString(1));
                ckhStatNamesPreparedStatement.setString(3, queryResult.getString(2));
                ckhStatNamesPreparedStatement.addBatch();
            }
           if(queryResult != null){
                queryResult.close();
            }            
            ckhStatNamesPreparedStatement.executeBatch();
            ckhStatNamesPreparedStatement.clearBatch();
            ckhStatNamesPreparedStatement.clearWarnings();  
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error processing statistics names!"
            );
            e.printStackTrace();
            return false;
        }
        try{
            if( ckhStatNamesPreparedStatement != null && ! ckhStatNamesPreparedStatement.isClosed()){
                ckhStatNamesPreparedStatement.close();
            }
        }catch(Exception e){
            
        }
        return true;
    }
    
    public boolean processSQLStats (ResultSet queryResult, long currentDateTime) {
        try{
            while (queryResult != null && queryResult.next()) {

                ckhSQLStatsPreparedStatement.addBatch();
                
            }
           if(queryResult != null){
                queryResult.close();
            }            
            ckhSQLStatsPreparedStatement.executeBatch();
            ckhSQLStatsPreparedStatement.clearBatch();
            ckhSQLStatsPreparedStatement.clearWarnings();           
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error processing sql stats!"
            );
            e.printStackTrace();
            return false;
        }
        return true;
    } 
    
    public boolean processSQLPlans (ResultSet queryResult) {
        if(! handleSQLPlansConnection()){
            return false;
        }        
        try{
            while (queryResult != null && queryResult.next()) {
                ckhSQLPlansPreparedStatement.setString(1, queryResult.getString(1));
                ckhSQLPlansPreparedStatement.setString(2, queryResult.getString(2));
                ckhSQLPlansPreparedStatement.addBatch();
            }
           if(queryResult != null){
                queryResult.close();
            }            
            ckhSQLPlansPreparedStatement.executeBatch();
            ckhSQLPlansPreparedStatement.clearBatch();
            ckhSQLPlansPreparedStatement.clearWarnings();
            ckhSQLPlansPreparedStatement.close();
            connClickHouse.close();
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error processing sql plans!"
            );
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
            if (ckhIOFileStatsPreparedStatement != null && !ckhIOFileStatsPreparedStatement.isClosed()) {
                ckhIOFileStatsPreparedStatement.close();
            }            
            if (ckhIOFunctionStatsPreparedStatement != null && !ckhIOFunctionStatsPreparedStatement.isClosed()) {
                ckhIOFunctionStatsPreparedStatement.close();
            }                        
            if (ckhSysStatsPreparedStatement != null && !ckhSysStatsPreparedStatement.isClosed()) {
                ckhSysStatsPreparedStatement.close();
            }
            if (ckhSesStatsPreparedStatement != null && !ckhSesStatsPreparedStatement.isClosed()) {
                ckhSesStatsPreparedStatement.close();
            }
            if (ckhSQLTextsPreparedStatement != null && !ckhSQLTextsPreparedStatement.isClosed()) {
                ckhSQLTextsPreparedStatement.close();
            }      
            if (ckhSQLPlansPreparedStatement != null && !ckhSQLPlansPreparedStatement.isClosed()) {
                ckhSQLPlansPreparedStatement.close();
            }                        
            if (connClickHouse != null && !connClickHouse.isClosed()) {
                connClickHouse.close();
            }
        } catch (SQLException e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + dbUniqueName + "\t" + dbHostName + "\t" + 
                    "Error durring Clickhouse resource cleanups!"
            );
            //e.printStackTrace();
        }
    }
}
