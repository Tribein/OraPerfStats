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
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.ClickHouseConnection;                                                                                                                                                                                                                         
import ru.yandex.clickhouse.ClickHousePreparedStatement;                                                                                                                                                                                                                  
import ru.yandex.clickhouse.settings.ClickHouseProperties; 


public class StatCollectorCKH extends Thread {
    private int secondsBetweenSnaps = 10;                                                                                                                                                                                                                                 
    private String dbUserName = "dbsnmp";                                                                                                                                                                                                                                 
    private String dbPassword = "dbsnmp";                                                                                                                                                                                                                                 
    private String connString;                                                                                                                                                                                                                                            
    private String dbUniqueName;                                                                                                                                                                                                                                          
    private String dbHostName;                                                                                                                                                                                                                                            
    private Connection con;                                                                                                                                                                                                                                               
    private PreparedStatement waitsPreparedStatement;                                                                                                                                                                                                                     
    private DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");                                                                                                                                                                        
    private long  currentDateTime;                                                                                                                                                                                                                                   
    private LocalDate currentDate;
    private ClickHousePreparedStatement sessionsPreparedStatement;                                                                                                                                                                                                                     
    private ClickHouseConnection connClickHouse;                                                                                                                                                                                                                          
    private ClickHouseProperties connClickHouseProperties = new ClickHouseProperties().withCredentials("default", "secret");                                                                                                                                              
    private String connClickHouseString = "jdbc:clickhouse://10.64.130.69:8123/testdb";                                                                                                                                                                          
    private String insertSessions = "insert into orasessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";                                                                                                                                                                                                                                                                                                                                                                                       
    String waitsQuery                                                                                                                                                                                                                                                     
            = "SELECT " +                                                                                                                                                                                                                                                 
"  sid," +                                                                                                                                                                                                                                                                
"  serial#," +                                                                                                                                                                                                                                                            
"  decode(taddr,NULL,'N','Y')," +
"  status," +
"  schemaname," +
"  nvl(osuser,'-')," +
"  nvl(machine,'-')," +
"  nvl(program,'-')," +
"  TYPE," +
"  nvl(MODULE,'-')," +
"  nvl(blocking_session,0)," +
"  Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event), " +
"  Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',wait_class)," +
"  round(wait_time_micro/1000000,3)," +
"  nvl(sql_id,'-')," +
"  nvl(sql_exec_start,sysdate - interval '360' month)," +
"  sql_exec_id," +
"  logon_time," +
"  seq#" +
" FROM gv$session" +
" WHERE wait_class#<>6 OR taddr IS NOT null";

    boolean shutdown = false;    

    public StatCollectorCKH(String inputString) {
        connString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
    }    
   public void run() {
        ResultSet queryResult;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (Exception e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load Oracle driver!");
            shutdown = true;
        }
        try{
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");            
        }catch (Exception e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load ClickHouse driver!");
            shutdown = true;
        }
        try {
            connClickHouse = new ClickHouseDriver().connect(connClickHouseString, connClickHouseProperties);
            sessionsPreparedStatement =(ClickHousePreparedStatement) connClickHouse.prepareStatement(insertSessions);
        } catch (Exception e){
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot connect to ClickHouse!");
            shutdown = true;
        }
        try{
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + connString, dbUserName, dbPassword);
            waitsPreparedStatement = con.prepareStatement(waitsQuery, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

            while(!shutdown) /*for (int i = 0; i < 1; i++)*/ {
                waitsPreparedStatement.execute();
                queryResult = waitsPreparedStatement.getResultSet();
                currentDateTime =   Instant.now().getEpochSecond();
                currentDate = LocalDate.now();
                while (queryResult.next() && !shutdown) {
                    try{
                        //--
                        sessionsPreparedStatement.setString(1, dbUniqueName);
                        sessionsPreparedStatement.setString(2, dbHostName);
                        sessionsPreparedStatement.setLong(3,  currentDateTime);
                        sessionsPreparedStatement.setInt(4,queryResult.getInt(1));
                        sessionsPreparedStatement.setInt(5,queryResult.getInt(2));
                        sessionsPreparedStatement.setString(6,queryResult.getString(3));
                        sessionsPreparedStatement.setString(7,queryResult.getString(4).substring(0,1));
                        sessionsPreparedStatement.setString(8,queryResult.getString(5));
                        sessionsPreparedStatement.setString(9,queryResult.getString(6));
                        sessionsPreparedStatement.setString(10,queryResult.getString(7));
                        sessionsPreparedStatement.setString(11,queryResult.getString(8));
                        sessionsPreparedStatement.setString(12,queryResult.getString(9).substring(0,1));
                        sessionsPreparedStatement.setString(13,queryResult.getString(10));
                        sessionsPreparedStatement.setInt(14,queryResult.getInt(11));
                        sessionsPreparedStatement.setString(15,queryResult.getString(12));
                        sessionsPreparedStatement.setString(16,queryResult.getString(13));
                        sessionsPreparedStatement.setFloat(17,queryResult.getFloat(14));
                        sessionsPreparedStatement.setString(18,queryResult.getString(15));
                        sessionsPreparedStatement.setLong(19, ((java.util.Date) queryResult.getTimestamp(16)).getTime() / 1000 );
                        sessionsPreparedStatement.setInt(20,queryResult.getInt(17));
                        sessionsPreparedStatement.setLong(21,((java.util.Date) queryResult.getTimestamp(18)).getTime() / 1000);
                        sessionsPreparedStatement.setInt(22,queryResult.getInt(19));
                        sessionsPreparedStatement.setDate(23,java.sql.Date.valueOf(currentDate));
                        sessionsPreparedStatement.addBatch();
                        //--
                    } catch (Exception e){
                        System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error submitting data to ClickHouse!");
                        shutdown = true;
                        e.printStackTrace();
                    }
                }
                queryResult.close();
                sessionsPreparedStatement.executeBatch();
                TimeUnit.SECONDS.sleep(secondsBetweenSnaps);
            }
            waitsPreparedStatement.close();
            sessionsPreparedStatement.close();
            con.close();
            connClickHouse.close();
        } catch (Exception e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error getting result from database " + connString);
            shutdown = true;
            e.printStackTrace();
        }
    }    
}