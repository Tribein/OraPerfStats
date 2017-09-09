/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oraperf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

/**
 *
 * @author lesha
 */
public class CKHOptimize extends Thread {
    private int secondsToSleep = 30;                                                                                                                                                                                                                                 
    private DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");                                                                                                                                                                        
    private ClickHousePreparedStatement stmtOptimize; 
    private ClickHouseConnection connClickHouse;                                                                                                                                                                                                                          
    private ClickHouseProperties connClickHouseProperties = new ClickHouseProperties().withCredentials("default", "secret");                                                                                                                                              
    private String connClickHouseString = "jdbc:clickhouse://10.64.130.69:8123/testdb";                                                                                                                                                                          
    private String optimizeTable;
    private String tableName;
    boolean shutdown = false;       
    public CKHOptimize(String inputString) {
        tableName = inputString;
        optimizeTable = "optimize table "+ tableName;
    }      
   public void run() {
        try{
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");            
        }catch (Exception e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load ClickHouse driver!");
            shutdown = true;
        }
        try {
            connClickHouse = new ClickHouseDriver().connect(connClickHouseString, connClickHouseProperties);
            stmtOptimize = (ClickHousePreparedStatement) connClickHouse.prepareStatement(optimizeTable);
        } catch (Exception e){
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot connect to ClickHouse!");
            shutdown = true;
        }
        try{
            while(!shutdown) {
                stmtOptimize.execute();
                TimeUnit.SECONDS.sleep(secondsToSleep);
            }
            stmtOptimize.close();
            connClickHouse.close();
        } catch (Exception e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error executin optimize for " + tableName);
            shutdown = true;
        }
    }        
}
