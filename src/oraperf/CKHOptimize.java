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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;


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
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error executing optimize for " + tableName);
            shutdown = true;
        }
    }        
}
