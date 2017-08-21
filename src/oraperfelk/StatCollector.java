/* 
 * Copyright (C) 2017 lesha
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

/**
 *
 * @author Tribein
 */
public class StatCollector extends Thread {

    private String dbUserName = "dbsnmp";
    private String dbPassword = "dbsnmp";
    private String connString;
    private String dbUniqueName;
    private Connection con;
    private PreparedStatement waitsPreparedStatement;
    DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    LocalDateTime currentDate;
    String elasticUrl = "http://elasticsearch.example.net:9200/";
    String waitsQuery
            = "select 'W',Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',wait_class),count(1) \n"
            + "from v$session where wait_class#<>6 \n"
            + "group by Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',wait_class)\n"
            + "UNION ALL\n"
            + "SELECT 'E',Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event),Count(1) \n"
            + "from v$session where wait_class#<>6 \n"
            + "group BY Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event) ";
    InputStream responseInputStream;
    BufferedReader responseContent;
    String responseLine;
    ArrayList<String> jsonWaitsArray;
    ArrayList<String> jsonEventsArray;
    HashMap <String, HttpURLConnection> elkConnectionMap;
    HashMap <String, String> elkIndexMap;
    HttpURLConnection currentConnection;
    boolean shutdown = false;
    public StatCollector(String inputString) {
        connString = inputString;
        dbUniqueName = inputString.split("/")[1];
        
        elkConnectionMap = new HashMap <String, HttpURLConnection>(); 
        elkIndexMap = new HashMap<String,String>();
        
        elkConnectionMap.put("waits", null );
        elkIndexMap.put("waits","grid/waits");
        elkConnectionMap.put("events", null );
        elkIndexMap.put("events","grid/events");
        
    }

    public void SendToELK(String dataType, String jsonContent) {
        int dummy = 0;
        HttpURLConnection currentConnection;
        if (jsonContent == null || dataType == null || ! elkConnectionMap.containsKey(dataType)) {
            return;
        }
        
        currentConnection = elkConnectionMap.get(dataType);
        if (currentConnection == null) {
            try {
                currentConnection = (HttpURLConnection) new URL(elasticUrl + elkIndexMap.get(dataType)).openConnection();
                currentConnection.setConnectTimeout(5000);
                currentConnection.setRequestMethod("POST");
                currentConnection.setDoOutput(true);
                currentConnection.addRequestProperty("Content-Type", "application/json");
            } catch (IOException e) {
                System.out.println(dateFormat.format(LocalDateTime.now()) + "\t" + "Error opening connection to ELK: " + elasticUrl);
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
            System.out.println(dateFormat.format(LocalDateTime.now()) + "\t" + "Error sending "+dataType+" data to ELK: " + elasticUrl);
            shutdown = true;
        }
    }

    public void run() {
        ResultSet queryResult;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.out.println(dateFormat.format(LocalDateTime.now()) + "\t" + "Cannot load Oracle driver!");
            shutdown = true;
        }
        try {
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + connString, dbUserName, dbPassword);
            waitsPreparedStatement = con.prepareStatement(waitsQuery, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            jsonWaitsArray = new ArrayList();
            jsonEventsArray = new ArrayList();
            while(!shutdown)/*for (int i = 0; i < 1; i++)*/ {
                currentDate = LocalDateTime.now();
                waitsPreparedStatement.execute();
                queryResult = waitsPreparedStatement.getResultSet();
                while (queryResult.next()) {

                    switch (queryResult.getString(1)) {
                        case "W":
                            jsonWaitsArray.add("\"" + queryResult.getString(2) + "\" : " + queryResult.getString(3));
                            break;
                        case "E":
                            jsonEventsArray.add("\"" + queryResult.getString(2) + "\" : " + queryResult.getString(3));
                            break;
                        default:
                            break;
                    }
                }
                queryResult.close();
                if (jsonWaitsArray.size() > 0) {
                    jsonWaitsArray.add(" \"Database\" : \"" + dbUniqueName + "\" ");
                    jsonWaitsArray.add(" \"SnapTime\" : \"" + dateFormat.format(currentDate) + "\" ");
                    SendToELK("waits","{ " + String.join(",", jsonWaitsArray) + " }");
                    /*
                    System.out.println(
                            "{ " + String.join(",", jsonWaitsArray) + " }"
                    );
                    */
                }
                if (jsonEventsArray.size() > 0) {
                    jsonEventsArray.add(" \"Database\" : \"" + dbUniqueName + "\" ");
                    jsonEventsArray.add(" \"SnapTime\" : \"" + dateFormat.format(currentDate) + "\" ");
                    SendToELK("events","{ " + String.join(",", jsonEventsArray) + " }");
                    /*
                    System.out.println(
                            "{ " + String.join(",", jsonEventsArray) + " }"
                    );
                    */
                }
                TimeUnit.SECONDS.sleep(5);
            }
            
            waitsPreparedStatement.close();
            con.close();
        } catch (Exception e) {
            System.out.println(dateFormat.format(LocalDateTime.now()) + "\t" + "Error getting result from database " + connString);
            shutdown = true;
        }
    }
}
