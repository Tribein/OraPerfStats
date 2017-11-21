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

import java.io.File;
import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;


public class OraPerf {

    private static final int SECONDSTOSLEEP = 60;
    private static final String DBLISTFILENAME = "db.lst";
    private static final String CKHUSERNAME = "oracle";
    private static final String CKHPASSWORD = "elcaro";
    private static final String CKHCONNECTIONSTRING = "jdbc:clickhouse://10.64.139.57:8123/oradb";
    private static final String ELASTICURL = "";
    private static final String CKHOPTIMIZETABLE = "sessions";
    private static Scanner fileScanner;
    public static void main(String[] args) throws InterruptedException {
        /*
        Logger oraperfLog = LogManager.getLogManager().getLogger("");
        oraperfLog.setLevel(Level.SEVERE);
        for (Handler h : oraperfLog.getHandlers()) {
            h.setLevel(Level.SEVERE);
        } 
        
        System.setProperty("org.apache.commons.logging.Log","org.apache.commons.logging.impl.NoOpLog");
        
        Logger oraperfLog = Logger.getGlobal();
        oraperfLog.setUseParentHandlers( false ); 
        Logger.getLogger("").setLevel( Level.OFF );
        
        Logger globalLogger = Logger.getLogger("global");
        Handler[] handlers = globalLogger.getHandlers();
        for(Handler handler : handlers) {
            globalLogger.removeHandler(handler);
        }
        
        LogManager.getLogManager().reset();
        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.OFF);        
        */
        
        SL4JLogger lg = new SL4JLogger();
        Map <String, Thread> dbList = new HashMap();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
        String dbLine; 
        Thread optimizeThreadSessions = null;
        File dbListFile = new File(DBLISTFILENAME);
        lg.LogTrace(dateFormat.format(LocalDateTime.now()) + "\t" + "Starting");
        while(true) /*for(int i=0; i<1; i++)*/{
            try{
                fileScanner = new Scanner(dbListFile);
                while ( fileScanner.hasNext()){
                    dbLine = fileScanner.nextLine();
                    if ( !dbList.containsKey(dbLine) || dbList.get(dbLine) == null || !dbList.get(dbLine).isAlive() ){
                        try{
                            dbList.put(dbLine, new StatCollectorCKH(dbLine, CKHCONNECTIONSTRING,CKHUSERNAME, CKHPASSWORD ));
                            //dbList.put(dbLine, new StatCollectorELK(dbLine,ELASTICURL));
                            lg.LogTrace(dateFormat.format(LocalDateTime.now()) + "\t" + "Adding new database for monitoring: "+dbLine);
                            dbList.get(dbLine).start();
                        }catch(Exception e){
                            lg.LogError(dateFormat.format(LocalDateTime.now()) + "\t" + "Error running thread for "+dbLine);
                            e.printStackTrace();
                        }
                    }
                }
                //for ClickHouse Only
                if(optimizeThreadSessions==null || !optimizeThreadSessions.isAlive()){
                    lg.LogTrace(dateFormat.format(LocalDateTime.now()) + "\t" + "Runnign ClickHouse thread for optimize sessions table!");
                    optimizeThreadSessions = new OptimizeCKH(CKHOPTIMIZETABLE, CKHCONNECTIONSTRING,CKHUSERNAME, CKHPASSWORD);
                    optimizeThreadSessions.start();
                }
                fileScanner.close();               
            } catch (FileNotFoundException e){
                lg.LogError(dateFormat.format(LocalDateTime.now()) + "\t" +"Error reading database list!");
                e.printStackTrace();
                System.exit(2);
            }
                TimeUnit.SECONDS.sleep(SECONDSTOSLEEP);
        }
        //Done
    }
    
}
