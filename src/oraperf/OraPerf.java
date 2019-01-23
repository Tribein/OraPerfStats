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

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;

import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class OraPerf {

    private static SL4JLogger lg;

    private static final String PROPERTIESFILENAME = "oraperf.properties";
    private static final int SECONDSTOSLEEP = 60;
    private static final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
    private static final int CKHQUEUECONSUMERSLEEP = 5;

    private static Scanner fileScanner;
    private static ArrayList<String> oraDBList;
    private static String DBLISTFILENAME = "";
    private static String DBUSERNAME = "";
    private static String DBPASSWORD = "";
    private static String CKHUSERNAME = "";
    private static String CKHPASSWORD = "";
    private static String DBLISTSOURCE = "";
    private static String ORADBLISTCSTR = "";
    private static String ORADBLISTUSERNAME = "";
    private static String ORADBLISTPASSWORD = "";
    private static String ORADBLISTQUERY = "";
    private static String CKHCONNECTIONSTRING = "";
    private static int CKHQUEUECONSUMERS  = 1;
    private static boolean GATHERSESSIONS = false;
    private static boolean GATHERSESSTATS = false;
    private static boolean GATHERSYSSTATS = false;
    private static ComboPooledDataSource CKHDataSource;
    private static BlockingQueue<OraCkhMsg> CKHQueue = new LinkedBlockingQueue<>();

    static Map<String, Thread> dbSessionsList = new HashMap();
    static Map<String, Thread> dbSessStatsList = new HashMap();
    static Map<String, Thread> dbSyssStatsList = new HashMap();

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

    private static ArrayList<String> getListFromOraDB(String cstr, String usn, String pwd, String query)
            throws ClassNotFoundException, SQLException {
        ArrayList<String> retList = new ArrayList<>();
        Connection dbListcon;
        Statement dbListstmt;
        ResultSet dbListrs;

        dbListcon = DriverManager.getConnection(cstr, usn, pwd);
        dbListcon.setAutoCommit(false);
        dbListstmt = dbListcon.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        dbListrs = dbListstmt.executeQuery(query);
        while (dbListrs.next()) {
            retList.add(dbListrs.getString(1));
        }
        dbListrs.close();
        dbListstmt.close();
        dbListcon.close();
        return retList;
    }

    private static ArrayList<String> getListFromHTTP() {
        ArrayList<String> retList = new ArrayList<>();
        return retList;
    }

    private static ArrayList<String> getOraDBList() {
        try {
            switch (DBLISTSOURCE.toUpperCase()) {
                case "FILE":
                    return getListFromFile(new File(DBLISTFILENAME));
                case "ORADB":
                    return getListFromOraDB(ORADBLISTCSTR, ORADBLISTUSERNAME, ORADBLISTPASSWORD, ORADBLISTQUERY);
                default:
                    return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static boolean processProperties(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
            DBUSERNAME = properties.getProperty("DBUSERNAME");
            DBPASSWORD = properties.getProperty("DBPASSWORD");
            CKHUSERNAME = properties.getProperty("CKHUSERNAME");
            CKHPASSWORD = properties.getProperty("CKHPASSWORD");
            CKHCONNECTIONSTRING = properties.getProperty("CKHCONNECTIONSTRING");
            DBLISTSOURCE = properties.getProperty("DBLISTSOURCE");
            CKHQUEUECONSUMERS = Integer.parseInt(properties.getProperty("QUEUECONSUMERS"));
            switch (DBLISTSOURCE.toUpperCase()) {
                case "FILE":
                    DBLISTFILENAME = properties.getProperty("DBLISTFILENAME");
                    break;
                case "ORADB":
                    ORADBLISTCSTR = properties.getProperty("ORADBLISTCONNECTIONSTRING");
                    ORADBLISTUSERNAME = properties.getProperty("ORADBLISTUSERNAME");
                    ORADBLISTPASSWORD = properties.getProperty("ORADBLISTPASSWORD");
                    ORADBLISTQUERY = properties.getProperty("ORADBLISTQUERY");
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    break;
                default:
                    lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                            "No proper database list source was provided!"
                    );
            }
            if (properties.getProperty("SESSIONS").compareToIgnoreCase("TRUE") == 0) {
                GATHERSESSIONS = true;
            }
            if (properties.getProperty("SESSTATS").compareToIgnoreCase("TRUE") == 0) {
                GATHERSESSTATS = true;
            }
            if (properties.getProperty("SYSSTATS").compareToIgnoreCase("TRUE") == 0) {
                GATHERSYSSTATS = true;
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private static void configureLogger() {

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
    }

    private static ComboPooledDataSource initDataSource() {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("ru.yandex.clickhouse.ClickHouseDriver");
            cpds.setJdbcUrl(CKHCONNECTIONSTRING);
            cpds.setUser(CKHUSERNAME);
            cpds.setPassword(CKHPASSWORD);
            cpds.setMinPoolSize(100);
            cpds.setAcquireIncrement(10);
            cpds.setMaxPoolSize(5000);
            cpds.setMaxIdleTime(180);
            return cpds;
        } catch (Exception e) {
            lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                    "Cannot connect to ClickHouse server!"
            );
            return null;
        }
    }

    private static void processSessions(String dbLine) {
        if (!dbSessionsList.containsKey(dbLine) || dbSessionsList.get(dbLine) == null || !dbSessionsList.get(dbLine).isAlive()) {
            try {
                dbSessionsList.put(dbLine, new StatCollector(dbLine, DBUSERNAME, DBPASSWORD, CKHDataSource, DATEFORMAT, 0));
                lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        "Starting sessions thread for " + dbLine
                );
                dbSessionsList.get(dbLine).start();
            } catch (Exception e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        "Error running sessions thread for " + dbLine
                );
                e.printStackTrace();
            }
        }
    }

    private static void processSessionStats(String dbLine) {
        if (!dbSessStatsList.containsKey(dbLine) || dbSessStatsList.get(dbLine) == null || !dbSessStatsList.get(dbLine).isAlive()) {
            try {
                dbSessStatsList.put(dbLine, new StatCollector(dbLine, DBUSERNAME, DBPASSWORD, CKHDataSource, DATEFORMAT, 1));
                lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        "Starting sessions stats thread for " + dbLine
                );
                dbSessStatsList.get(dbLine).start();
            } catch (Exception e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        "Error running sessions stats thread for " + dbLine
                );
                e.printStackTrace();
            }
        }
    }

    private static void processSystemRoutines(String dbLine) {
        if (!dbSyssStatsList.containsKey(dbLine) || dbSyssStatsList.get(dbLine) == null || !dbSyssStatsList.get(dbLine).isAlive()) {
            try {
                dbSyssStatsList.put(dbLine, new StatCollector(dbLine, DBUSERNAME, DBPASSWORD, CKHDataSource, DATEFORMAT, 2));
                lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        "Starting system stats thread for " + dbLine
                );
                dbSyssStatsList.get(dbLine).start();
            } catch (Exception e) {
                lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
                        "Error running system stats thread for " + dbLine
                );
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (!processProperties(PROPERTIESFILENAME)) {
            System.exit(1);
        }

        configureLogger();

        lg = new SL4JLogger();

        CKHDataSource = initDataSource();
        if (CKHDataSource == null) {
            System.exit(2);
        }
        String dbLine;

        lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t" + "Starting");

        while (true) /*for(int i=0; i<1; i++)*/ {
            oraDBList = getOraDBList();
            for (int i = 0; i < oraDBList.size(); i++) {
                dbLine = oraDBList.get(i);
                //lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t" + "Adding new database for monitoring: " + dbLine);
                if (GATHERSESSIONS) {
                    //session waits
                    processSessions(dbLine);
                }
                if (GATHERSESSTATS) {
                    //session stats
                    processSessionStats(dbLine);
                }
                if (GATHERSYSSTATS) {
                    //system stats & sql texts/plan hash values
                    processSystemRoutines(dbLine);
                }
            }
            TimeUnit.SECONDS.sleep(SECONDSTOSLEEP);
        }
        //Done
    }

}
