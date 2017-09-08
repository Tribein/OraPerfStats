/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

/**
 *
 * @author lesha
 */
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
    private long  currentDate;                                                                                                                                                                                                                                   
    private ClickHousePreparedStatement stmtSessions;                                                                                                                                                                                                                     
    private ClickHouseConnection connClickHouse;                                                                                                                                                                                                                          
    private ClickHouseProperties connClickHouseProperties = new ClickHouseProperties().withCredentials("default", "secret");                                                                                                                                              
    private String connClickHouseString = "jdbc:clickhouse://10.64.130.69:8123/testdb";                                                                                                                                                                          
    String insertSessions = "insert into orasessions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";                                                                                                                                                                                                                                                                                                                                                                                       
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
"  sql_id," +
"  nvl(sql_exec_start,sysdate)," +
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
            stmtSessions =(ClickHousePreparedStatement) connClickHouse.prepareStatement(insertSessions);
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
                currentDate =   Instant.now().getEpochSecond();
                while (queryResult.next() && !shutdown) {
                    try{
                        //--
                        stmtSessions.setString(1, dbUniqueName);
                        stmtSessions.setString(2, dbHostName);
                        stmtSessions.setLong(3,  currentDate);
                        stmtSessions.setInt(4,queryResult.getInt(1));
                        stmtSessions.setInt(5,queryResult.getInt(2));
                        stmtSessions.setString(6,queryResult.getString(3));
                        stmtSessions.setString(7,queryResult.getString(4));
                        stmtSessions.setString(8,queryResult.getString(5));
                        stmtSessions.setString(9,queryResult.getString(6));
                        stmtSessions.setString(10,queryResult.getString(7));
                        stmtSessions.setString(11,queryResult.getString(8));
                        stmtSessions.setString(12,queryResult.getString(9).substring(0,1));
                        stmtSessions.setString(13,queryResult.getString(10));
                        stmtSessions.setInt(14,queryResult.getInt(11));
                        stmtSessions.setString(15,queryResult.getString(12));
                        stmtSessions.setString(16,queryResult.getString(13));
                        stmtSessions.setFloat(17,queryResult.getFloat(14));
                        stmtSessions.setString(18,queryResult.getString(15));
                        stmtSessions.setLong(19, ((java.util.Date) queryResult.getTimestamp(16)).getTime()/1000 );
                        stmtSessions.setInt(20,queryResult.getInt(17));
                        stmtSessions.setLong(21,((java.util.Date) queryResult.getTimestamp(18)).getTime()/1000);
                        stmtSessions.setInt(22,queryResult.getInt(19));
                        //--
                        stmtSessions.execute();
                        connClickHouse.commit();
                    } catch (Exception e){
                        System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error submitting data to ClickHouse!");
                        shutdown = true;
                        e.printStackTrace();
                    }
                }
                queryResult.close();
                TimeUnit.SECONDS.sleep(secondsBetweenSnaps);
            }
            waitsPreparedStatement.close();
            stmtSessions.close();
            con.close();
            connClickHouse.close();
        } catch (Exception e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error getting result from database " + connString);
            shutdown = true;
            e.printStackTrace();
        }
    }    
}
