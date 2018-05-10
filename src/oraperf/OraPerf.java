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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;

import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class OraPerf {

    private static final String PROPERTIESFILENAME = "oraperf.properties";
    private static final int SECONDSTOSLEEP = 60;
    private static Scanner fileScanner;
    private static ArrayList<String> oraDBList;
    private static String DBLISTFILENAME = "";
    private static String DBUSERNAME = "";
    private static String DBPASSWORD = "";
    private static String CKHUSERNAME = "";
    private static String CKHPASSWORD = "";
    private static String CKHCONNECTIONSTRING = "";

    private static ArrayList<String> getListFromFile(File dbListFile) {
        ArrayList<String> retList = new ArrayList<String>();
        try {
            fileScanner = new Scanner(dbListFile);
            while (fileScanner.hasNext()) {
                retList.add(fileScanner.nextLine());
            }
            fileScanner.close();
        } catch (FileNotFoundException e) {
            System.out.println("Error reading database list!");
            e.printStackTrace();

        } finally {
            return retList;
        }
    }

    private ArrayList<String> getListFromOraDB() {
        ArrayList<String> retList = new ArrayList<>();
        return retList;
    }

    private ArrayList<String> getListFromHTTP() {
        ArrayList<String> retList = new ArrayList<>();
        return retList;
    }

    private static boolean processProperties(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
            DBUSERNAME = properties.getProperty("DBUSERNAME");
            DBPASSWORD = properties.getProperty("DBPASSWORD");
            DBLISTFILENAME = properties.getProperty("DBLISTFILENAME");
            CKHUSERNAME = properties.getProperty("CKHUSERNAME");
            CKHPASSWORD = properties.getProperty("CKHPASSWORD");
            CKHCONNECTIONSTRING = properties.getProperty("CKHCONNECTIONSTRING");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (!processProperties(PROPERTIESFILENAME)) {
            System.exit(1);
        }
        Logger oraperfLog = LogManager.getLogManager().getLogger("");
        oraperfLog.setLevel(Level.WARNING);
        Handler[] handlers = oraperfLog.getHandlers();
        for (Handler handler : handlers) {
            oraperfLog.removeHandler(handler);
        }
        Handler conHdlr = new ConsoleHandler();
        conHdlr.setFormatter(
                new java.util.logging.Formatter() {
            @Override
            public String format(LogRecord record) {
                return record.getMessage() + "\n";
            }
        }
        );
        oraperfLog.addHandler(conHdlr);

        SL4JLogger lg = new SL4JLogger();

        Map<String, Thread> dbList = new HashMap();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
        String dbLine;
        File dbListFile = new File(DBLISTFILENAME);
        lg.LogWarn(dateFormat.format(LocalDateTime.now()) + "\t" + "Starting");
        while (true) /*for(int i=0; i<1; i++)*/ {
            oraDBList = getListFromFile(dbListFile);
            for (int i = 0; i < oraDBList.size(); i++) {
                dbLine = oraDBList.get(i);
                if (!dbList.containsKey(dbLine) || dbList.get(dbLine) == null || !dbList.get(dbLine).isAlive()) {
                    try {
                        dbList.put(dbLine, new StatCollector(dbLine, DBUSERNAME, DBPASSWORD, CKHUSERNAME, CKHPASSWORD, CKHCONNECTIONSTRING));
                        lg.LogWarn(dateFormat.format(LocalDateTime.now()) + "\t" + "Adding new database for monitoring: " + dbLine);
                        dbList.get(dbLine).start();
                    } catch (Exception e) {
                        lg.LogError(dateFormat.format(LocalDateTime.now()) + "\t" + "Error running thread for " + dbLine);
                        e.printStackTrace();
                    }
                }
            }
            TimeUnit.SECONDS.sleep(SECONDSTOSLEEP);
        }
        //Done
    }

}
