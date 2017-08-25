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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.TimeZone;

/**
 *
 * @author Tribein
 */
public class StatCollector extends Thread {

    private int secondsBetweenSnaps = 10;
    private String dbUserName = "dbsnmp";
    private String dbPassword = "dbsnmp";
    private String elasticUrl = "http://elasticsearch.example.net:9200/";
    private String indexPrefix = "grid";
    private String connString;
    private String dbUniqueName;
    private String dbHostName;
    private String jsonString;
    private String formatedDate;
    private Connection con;
    private int dummy;
    private PreparedStatement sessionsPreparedStatement;
    private DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private DateTimeFormatter dateFormatIndex = DateTimeFormatter.ofPattern("ddMMYYYY");
    private ZonedDateTime  currentDate;
    private InputStream responseInputStream;
    private BufferedReader responseContent;
    private String responseLine;
    private ArrayList<String> jsonSessionArray;
    private HashMap <String, HttpURLConnection> elkConnectionMap;
    private HashMap <String, String> elkIndexMap;
    private HttpURLConnection currentConnection; 
    private Calendar cal = Calendar.getInstance();
    String waitsQuery
            = "SELECT " +
"  sid," +
"  serial#," +
"  decode(taddr,NULL,'N','Y')," +
"  status," +
"  schemaname," +
"  osuser," +
"  machine," +
"  program," +
"  TYPE," +
"  MODULE," +
"  blocking_session," +
"  Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',event), " +
"  Decode(state,'WAITED KNOWN TIME','CPU','WAITED SHORT TIME','CPU',wait_class)," +
"  wait_time_micro," +
"  sql_id," +
"  sql_exec_start," +
"  sql_exec_id," +
"  logon_time," +
"  seq#" +
" FROM gv$session" +
" WHERE wait_class#<>6 OR taddr IS NOT null";

    boolean shutdown = false;
    public StatCollector(String inputString) {
        connString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
        
        elkConnectionMap = new HashMap <String, HttpURLConnection>(); 
        elkIndexMap = new HashMap<String,String>();
        
        elkConnectionMap.put("sessions", null );
        elkIndexMap.put("sessions","/sessions");
    }

    public void SendToELK(String dataType, String jsonContent) {
        dummy = 0;
        
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
            System.out.println(jsonContent);
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
            cal.setTimeZone(TimeZone.getTimeZone("GMT"));
            con = DriverManager.getConnection("jdbc:oracle:thin:@" + connString, dbUserName, dbPassword);
            waitsPreparedStatement = con.prepareStatement(waitsQuery, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            jsonSessionArray = new ArrayList();
            while(!shutdown) /*for (int i = 0; i < 1; i++)*/ {
                currentDate = ZonedDateTime.now(ZoneOffset.UTC);
                waitsPreparedStatement.execute();
                queryResult = waitsPreparedStatement.getResultSet();
                cal.setTimeZone(TimeZone.getTimeZone("GMT"));
                while (queryResult.next()) {
                    jsonSessionArray.add(
                        "{ "
                                +"\"SID\" : "+queryResult.getInt(1) + ", "
                                +"\"Serial\" : "+queryResult.getInt(2) + ", "
                                +"\"Schema\" : \""+queryResult.getString(5) + "\", "
                                +"\"Status\" : \""+queryResult.getString(4).substring(0,1)+ "\", "
                                +((queryResult.getString(3) == null ) ? "" : "\"Transaction\" : true, " )  
                                +((queryResult.getString(6) == null) ?  "" : "\"OSUsername\" : \""+queryResult.getString(6).replace("\\", "/") + "\"," )
                                +((queryResult.getString(7) == null) ? "" : "\"Machine\" : \""+queryResult.getString(7).replace("\\", "/") + "\", " )
                                +((queryResult.getString(8) == null) ? "" : "\"Program\" : \""+queryResult.getString(8).replace("\\", "/") + "\", " )
                                +((queryResult.getString(10) == null) ? "" : "\"Module\" : \""+queryResult.getString(10).replace("\\", "/") + "\", " )
                                +"\"Type\" : \""+queryResult.getString(9).substring(0,1) + "\", "
                                +"\"WaitSequence\" : "+queryResult.getInt(19) + ", "
                                +"\"WaitEvent\" : \""+queryResult.getString(12) + "\", "
                                +"\"WaitClass\" : \""+queryResult.getString(13) + "\","
                                +((queryResult.getString(15) == null) ? "" : "\"SQLID\" : \""+queryResult.getString(15) + "\", " )
                                +((queryResult.getDate(16) == null ) ? "" : "\"SQLExecStart\" : "+queryResult.getDate(16, cal).getTime()+ ", " )
                                +((queryResult.getString(17) == null) ? "" : "\"SQLExecID\" : \""+queryResult.getString(17) + "\", " )
                                +((queryResult.getString(18) == null)? "" : "\"LogonTime\" : "+queryResult.getDate(18,cal).getTime()+", " )
                                +((queryResult.getString(11) == null) ? "" : "\"BlockingSID\" : \""+queryResult.getInt(11) +"\", " )
                                +"\"us\" : "+queryResult.getLong(14)
                        +" }"
                    ); 
                }
                queryResult.close();
                if (jsonSessionArray.size() > 0) {
                    jsonString = "{ " +
                            " \"Database\" : \"" + dbUniqueName + "\", " +
                            " \"Hostname\" : \"" + dbHostName + "\", " + 
                            " \"SnapTime\" : \"" + dateFormatData.format(currentDate) + "\", " +
                            "\"SessionsData\" : [ " + String.join(",", jsonSessionArray) + " ]"
                            + " }";
                   SendToELK("sessions",jsonString);
                   //System.out.println(jsonString);
                }
                
                jsonSessionArray.clear();              
                TimeUnit.SECONDS.sleep(secondsBetweenSnaps);
            }
            
            sessionsPreparedStatement.close();
            con.close();
        } catch (Exception e) {
            System.out.println(dateFormatData.format(LocalDateTime.now()) + "\t" + "Error getting result from database " + connString);
            shutdown = true;
            e.printStackTrace();
        }
    }
}
