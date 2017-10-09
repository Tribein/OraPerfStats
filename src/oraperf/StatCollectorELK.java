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
import java.util.Calendar;
import java.util.TimeZone;


public class StatCollectorELK extends Thread {

    private int secondsBetweenSnaps = 10;
    private String dbUserName = "dbsnmp";
    private String dbPassword = "dbsnmp";
    private String elasticUrl;
    private String indexPrefix = "grid";
    private String connString;
    private String dbUniqueName;
    private String dbHostName;
    private String jsonString;
    private String formatedDate;
    private Connection con;
    private int dummyInt;
    private Long dummyLong;
    private java.sql.Date dummyDate;
    private String dummyString;
    private PreparedStatement sessionsPreparedStatement;
    private DateTimeFormatter dateFormatData = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private DateTimeFormatter dateFormatIndex = DateTimeFormatter.ofPattern("ddMMYYYY");
    private ZonedDateTime  currentDate;
    private InputStream responseInputStream;
    private BufferedReader responseContent;
    private String responseLine;
    private ArrayList<String> jsonSessionArray;
    private ArrayList<String> jsonSessionRowArray;
    private HashMap <String, HttpURLConnection> elkConnectionMap;
    private HashMap <String, String> elkIndexMap;
    private HttpURLConnection currentConnection; 
    private Calendar cal = Calendar.getInstance();
    String sessionsQuery
            = "SELECT " +
"  sid," +
"  serial#," +
"  decode(taddr,NULL,'false','true')," +
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
"  round(wait_time_micro/1000000,3)," +
"  sql_id," +
"  sql_exec_start," +
"  sql_exec_id," +
"  logon_time," +
"  seq#," +
"  p1," +
"  p2" +         
"  FROM gv$session" +
"  WHERE (sid<>sys_context('USERENV','SID')) and  (wait_class#<>6 OR taddr IS NOT null)";

    boolean shutdown = false;
    public StatCollectorELK(String inputString, String elkURL) {
        connString = inputString;
        dbUniqueName = inputString.split("/")[1];
        dbHostName = inputString.split(":")[0];
        elasticUrl = elkURL;
        elkConnectionMap = new HashMap <String, HttpURLConnection>(); 
        elkIndexMap = new HashMap<String,String>();
        
        elkConnectionMap.put("sessions", null );
        elkIndexMap.put("sessions","/sessions/_bulk");
    }

    public void SendToELK(String dataType, String jsonContent) {
        dummyInt = 0;
        
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
                dummyInt++;
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
            sessionsPreparedStatement = con.prepareStatement(sessionsQuery, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            jsonSessionArray = new ArrayList();
            jsonSessionRowArray = new ArrayList();
            while(!shutdown) /*for (int i = 0; i < 1; i++)*/ {
                currentDate = ZonedDateTime.now(ZoneOffset.UTC);
                formatedDate = dateFormatData.format(currentDate);
                sessionsPreparedStatement.execute();
                queryResult = sessionsPreparedStatement.getResultSet();
                while (queryResult.next()) {
                                //-
                                jsonSessionRowArray.add("\"SID\" : "+queryResult.getInt(1) );
                                jsonSessionRowArray.add("\"Serial\" : "+queryResult.getInt(2) );
                                jsonSessionRowArray.add("\"Schema\" : \""+queryResult.getString(5)+"\"" );
                                jsonSessionRowArray.add("\"Status\" : \""+queryResult.getString(4).substring(0,1)+"\"");
                                jsonSessionRowArray.add("\"Transaction\" : " + queryResult.getString(3));
                                dummyString = queryResult.getString(6);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"OSUsername\" : \""+queryResult.getString(6).replace("\\", "/")+"\"");
                                }
                                dummyString = queryResult.getString(7);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"Machine\" : \""+queryResult.getString(7).replace("\\", "/")+"\"");
                                }                                
                                dummyString = queryResult.getString(8);
                                
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"Program\" : \""+queryResult.getString(8).replace("\\", "/")+"\"");
                                }                                                                
                                dummyString = queryResult.getString(10);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"Module\" : \""+queryResult.getString(10).replace("\\", "/")+"\"");
                                } 
                                
                                jsonSessionRowArray.add("\"Type\" : \""+queryResult.getString(9).substring(0,1)+"\"" );
                                jsonSessionRowArray.add("\"WaitSequence\" : "+queryResult.getInt(19));
                                //jsonSessionRowArray.add("\"WaitEvent\" : \""+queryResult.getString(12)+"\"");
                                jsonSessionRowArray.add("\"WaitClass\" : \""+queryResult.getString(13)+"\"");
                                dummyString = queryResult.getString(15);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"SQLID\" : \""+queryResult.getString(15)+"\"");
                                } 
                                dummyString = "\"SQLID\" : \""+queryResult.getString(15)+"\"";
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add(dummyString);
                                }
                                dummyDate = queryResult.getDate(16);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"SQLExecStart\" : "+queryResult.getDate(16, cal).getTime());
                                }        
                                dummyDate = queryResult.getDate(18);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"LogonTime\" : "+queryResult.getDate(18,cal).getTime());
                                }                                    
                                dummyInt = queryResult.getInt(11);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"BlockingSID\" : "+queryResult.getInt(11));
                                }                                     
                                dummyLong = queryResult.getLong(17);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"SQLExecID\" : "+queryResult.getLong(17));
                                }                                                                   
                                jsonSessionRowArray.add("\"WaitTime\" : "+queryResult.getFloat(14));
                                dummyLong = queryResult.getLong(20);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"P1\" : "+queryResult.getLong(20));
                                }                                   
                                dummyLong = queryResult.getLong(21);
                                if(!queryResult.wasNull()){
                                    jsonSessionRowArray.add("\"P2\" : "+queryResult.getLong(21));
                                } 
                                //--
                    if (!jsonSessionRowArray.isEmpty()) {
                        jsonSessionArray.add( "{ \"index\": {} }");
                        jsonSessionArray.add("{ " +
                            " \"Database\" : \"" + dbUniqueName + "\", " +
                            " \"Hostname\" : \"" + dbHostName + "\", " + 
                            " \"SnapTime\" : \"" + formatedDate + "\", " +
                            String.join(",",jsonSessionRowArray)
                        + " }");
                        jsonSessionRowArray.clear();
                    }              
                }
                queryResult.close();                
                if(jsonSessionArray.size()>1){
                    //System.out.println(String.join("\n", jsonSessionArray));
                    SendToELK("sessions",String.join("\n", jsonSessionArray));
                    jsonSessionArray.clear();
                }
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
