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
package oraperfelk;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 *
 * @author Tribein
 */
public class StatCollector extends Thread {

    private int secondsBetweenSnaps = 10;
    private String dbUserName = "dbsnmp";
    private String dbPassword = "dbsnmp";
    private String elasticUrl = "http://ogw.moscow.sportmaster.ru:9200/";
    private String indexPrefix = "grid";
    private String connString;
    private String dbUniqueName;
    private String dbHostName;
    private String jsonString;
    private Connection con;
    private PreparedStatement waitsPreparedStatement;
    private DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private DateTimeFormatter dateFormatIndex = DateTimeFormatter.ofPattern("ddMMYYYY");
    private ZonedDateTime  currentDate;
    private InputStream responseInputStream;
    private BufferedReader responseContent;
    private String responseLine;
    private ArrayList<String> jsonWaitsArray;
    private ArrayList<String> jsonEventsArray;
    private HashMap <String, HttpURLConnection> elkConnectionMap;
    private HashMap <String, String> elkIndexMap;
    private HttpURLConnection currentConnection;    
    String waitsQuery
            = "select 'W',Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',wait_class),count(1) \n"
            + "from v$session where wait_class#<>6 \n"
            + "group by Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',wait_class)\n"
            + "UNION ALL\n"
            + "SELECT 'E',Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event),Count(1) \n"
            + "from v$session where wait_class#<>6 \n"
            + "group BY Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event) ";

    boolean shutdown = false;
    public StatCollector(String inputString) {
        connString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
        
        elkConnectionMap = new HashMap <String, HttpURLConnection>(); 
        elkIndexMap = new HashMap<String,String>();
        
        elkConnectionMap.put("waits", null );
        elkIndexMap.put("waits","/waits");
        elkConnectionMap.put("events", null );
        elkIndexMap.put("events","/events");
        
    }

    public void SendToELK(String dataType, String jsonContent) {
        int dummy = 0;
        
        if (jsonContent == null || dataType == null || ! elkConnectionMap.containsKey(dataType)) {
            return;
        }
        
        currentConnection = elkConnectionMap.get(dataType);
        if (currentConnection == null) {
            try {
                currentConnection = (HttpURLConnection) new URL(
                        elasticUrl 
                        + indexPrefix 
                        + "_"
                        + dateFormatIndex.format(ZonedDateTime.now(ZoneOffset.UTC))
                        + elkIndexMap.get(dataType) 
                ).openConnection();
                currentConnection.setConnectTimeout(5000);
                currentConnection.setRequestMethod("POST");
                currentConnection.setDoOutput(true);
                currentConnection.addRequestProperty("Content-Type", "application/json");
            } catch (IOException e) {
                System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error opening connection to ELK: " + elasticUrl);
                shutdown = true;
            }
        }
        try {
            currentConnection.getOutputStream().write(jsonContent.getBytes("UTF8"));
            currentConnection.getOutputStream().close();
            responseInputStream = (InputStream) currentConnection.getInputStream();
            responseContent = new BufferedReader(new InputStreamReader(responseInputStream));
            while ((responseLine = responseContent.readLine()) != null) {
                //System.out.println(responseLine);
                dummy++;
            }
            responseContent.close();
            responseInputStream.close(); 
        } catch (IOException e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error sending "+dataType+" data to ELK: " + elasticUrl);
            System.out.println(                        elasticUrl 
                        + indexPrefix 
                        + "_"
                        + dateFormatIndex.format(ZonedDateTime.now(ZoneOffset.UTC))
                        + elkIndexMap.get(dataType) 
             );
            shutdown = true;
        }
    }

    public void run() {
        ResultSet queryResult;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Cannot load Oracle driver!");
            shutdown = true;
        }
        try {
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + connString, dbUserName, dbPassword);
            waitsPreparedStatement = con.prepareStatement(waitsQuery, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            jsonWaitsArray = new ArrayList();
            jsonEventsArray = new ArrayList();
            while(!shutdown) /*for (int i = 0; i < 1; i++)*/ {
                currentDate = ZonedDateTime.now(ZoneOffset.UTC);
                waitsPreparedStatement.execute();
                queryResult = waitsPreparedStatement.getResultSet();
                while (queryResult.next()) {

                    switch (queryResult.getString(1)) {
                        case "W":
                            jsonWaitsArray.add(
                                    "{ \"WaitClass\" : \"" + queryResult.getString(2) + "\" , \"SessionsWaiting\" : " + queryResult.getInt(3) + " }"
                                    //"{ \"" + queryResult.getString(2) + "\" : " + queryResult.getInt(3) + " }"
                            );
                            break;
                        case "E":
                            jsonEventsArray.add(
                                    //"{ \"EventName\" : \"" + queryResult.getString(2) + "\" , \"SessionsWaiting\" : " + queryResult.getInt(3) + " }"
                                    "{ \"" + queryResult.getString(2) + "\" : " + queryResult.getInt(3) + " }"
                            );
                            break;
                        default:
                            break;
                    }
                }
                queryResult.close();
                if (jsonWaitsArray.size() > 0) {
                    jsonString = "{ " +
                            " \"Database\" : \"" + dbUniqueName + "\", " +
                            " \"Hostname\" : \"" + dbHostName + "\", " + 
                            " \"SnapTime\" : \"" + dateFormatData.format(currentDate) + "\", " +
                            "\"Waits\" : [ " + String.join(",", jsonWaitsArray) + " ]"
                            + " }";
                    SendToELK("waits",jsonString);
                    //System.out.println(jsonString);
                }
                
                if (jsonEventsArray.size() > 0) {
                    jsonString = "{ " +
                            " \"Database\" : \"" + dbUniqueName + "\", " +
                            " \"Hostname\" : \"" + dbHostName + "\", " + 
                            " \"SnapTime\" : \"" + dateFormatData.format(currentDate) + "\", " +
                            "\"Events\" : [ " + String.join(",", jsonEventsArray) + " ]"
                            + " }";
                    //SendToELK("events",jsonString);
                    //System.out.println(jsonString );
                }
                jsonWaitsArray.clear();
                jsonEventsArray.clear();                
                TimeUnit.SECONDS.sleep(secondsBetweenSnaps);
            }
            
            waitsPreparedStatement.close();
            con.close();
        } catch (Exception e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error getting result from database " + connString);
            shutdown = true;
        }
    }
}
