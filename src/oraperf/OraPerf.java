package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
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
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class OraPerf
{
  private static SL4JLogger lg;
  private static final String PROPERTIESFILENAME = "oraperf.properties";
  private static final int SECONDSTOSLEEP = 60;
  private static final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
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
  private static int CKHQUEUECONSUMERS = 1;
  private static boolean GATHERSESSIONS = false;
  private static boolean GATHERSESSTATS = false;
  private static boolean GATHERSYSSTATS = false;
  private static ComboPooledDataSource CKHDataSource;
  private static BlockingQueue<OraCkhMsg> ckhQueue = new LinkedBlockingQueue();
  static Map<String, Thread> dbSessionsList = new HashMap();
  static Map<String, Thread> dbSessStatsList = new HashMap();
  static Map<String, Thread> dbSyssStatsList = new HashMap();
  static Thread[] ckhQueueThreads;
  
  private static ArrayList<String> getListFromFile(File dbListFile)
  {
    ArrayList<String> retList = new ArrayList();
    try
    {
      fileScanner = new Scanner(dbListFile);
      while (fileScanner.hasNext()) {
        retList.add(fileScanner.nextLine());
      }
      fileScanner.close();
      
      return retList;
    }
    catch (FileNotFoundException e)
    {
        System.out.println("Error reading database list!");e.printStackTrace();return retList;
    }
  }
  
  private static ArrayList<String> getListFromOraDB(String cstr, String usn, String pwd, String query)
    throws ClassNotFoundException, SQLException
  {
    ArrayList<String> retList = new ArrayList();
    
    Connection dbListcon = DriverManager.getConnection(cstr, usn, pwd);
    dbListcon.setAutoCommit(false);
    Statement dbListstmt = dbListcon.createStatement(1005, 1007);
    ResultSet dbListrs = dbListstmt.executeQuery(query);
    while (dbListrs.next()) {
      retList.add(dbListrs.getString(1));
    }
    dbListrs.close();
    dbListstmt.close();
    dbListcon.close();
    return retList;
  }
  
  private static ArrayList<String> getListFromHTTP()
  {
    ArrayList<String> retList = new ArrayList();
    return retList;
  }
  
  private static ArrayList<String> getOraDBList()
  {
    try
    {
      switch (DBLISTSOURCE.toUpperCase())
      {
      case "FILE": 
        return getListFromFile(new File(DBLISTFILENAME));
      case "ORADB": 
        return getListFromOraDB(ORADBLISTCSTR, ORADBLISTUSERNAME, ORADBLISTPASSWORD, ORADBLISTQUERY);
      }
      return null;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  private static boolean processProperties(String fileName)
  {
    Properties properties = new Properties();
    try
    {
      properties.load(new FileInputStream(fileName));
      DBUSERNAME = properties.getProperty("DBUSERNAME");
      DBPASSWORD = properties.getProperty("DBPASSWORD");
      CKHUSERNAME = properties.getProperty("CKHUSERNAME");
      CKHPASSWORD = properties.getProperty("CKHPASSWORD");
      CKHCONNECTIONSTRING = properties.getProperty("CKHCONNECTIONSTRING");
      DBLISTSOURCE = properties.getProperty("DBLISTSOURCE");
      CKHQUEUECONSUMERS = Integer.parseInt(properties.getProperty("QUEUECONSUMERS"));
      switch (DBLISTSOURCE.toUpperCase())
      {
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
        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
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
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return false;
  }
  
  private static void configureLogger()
  {
    Logger oraperfLog = LogManager.getLogManager().getLogger("");
    oraperfLog.setLevel(Level.WARNING);
    
    Handler[] handlers = oraperfLog.getHandlers();
    for (Handler handler : handlers) {
      oraperfLog.removeHandler(handler);
    }
    Object conHdlr = new ConsoleHandler();
    ((Handler)conHdlr).setFormatter(new Formatter()
    {
      public String format(LogRecord record)
      {
        return record.getMessage() + "\n";
      }
    });
    oraperfLog.addHandler((Handler)conHdlr);
  }
  
  private static ComboPooledDataSource initDataSource()
  {
    ComboPooledDataSource cpds = new ComboPooledDataSource();
    try
    {
      cpds.setDriverClass("ru.yandex.clickhouse.ClickHouseDriver");
      cpds.setJdbcUrl(CKHCONNECTIONSTRING);
      cpds.setUser(CKHUSERNAME);
      cpds.setPassword(CKHPASSWORD);
      cpds.setMinPoolSize(100);
      cpds.setAcquireIncrement(100);
      cpds.setMaxPoolSize(4096);
      cpds.setMaxIdleTime(180);
      
      cpds.setForceSynchronousCheckins(true);
      return cpds;
    }
    catch (Exception e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
              "Cannot connect to ClickHouse server!"
      );
    }
    return null;
  }
  
  private static void processSessions(String dbLine)
  {
    if ((!dbSessionsList.containsKey(dbLine)) || (dbSessionsList.get(dbLine) == null) || (!((Thread)dbSessionsList.get(dbLine)).isAlive())) {
      try
      {
        dbSessionsList.put(dbLine, new StatCollector(dbLine, DBUSERNAME, DBPASSWORD, CKHDataSource, 0, ckhQueue));
        lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Starting sessions waits thread for " + dbLine
        );
        
        ((Thread)dbSessionsList.get(dbLine)).start();
      }
      catch (Exception e)
      {
        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Error running sessions thread for " + dbLine
        );
        
        e.printStackTrace();
      }
    }
  }
  
  private static void processSessionStats(String dbLine)
  {
    if ((!dbSessStatsList.containsKey(dbLine)) || (dbSessStatsList.get(dbLine) == null) || (!((Thread)dbSessStatsList.get(dbLine)).isAlive())) {
      try
      {
        dbSessStatsList.put(dbLine, new StatCollector(dbLine, DBUSERNAME, DBPASSWORD, CKHDataSource, 1, ckhQueue));
        lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Starting sessions stats thread for " + dbLine
        );
        
        ((Thread)dbSessStatsList.get(dbLine)).start();
      }
      catch (Exception e)
      {
        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Error running sessions stats thread for " + dbLine
        );
        
        e.printStackTrace();
      }
    }
  }
  
  private static void processSystemRoutines(String dbLine)
  {
    if (
            dbSyssStatsList.get(dbLine) == null || 
            !dbSyssStatsList.containsKey(dbLine) ||  
            !((Thread)dbSyssStatsList.get(dbLine)).isAlive()
    ) {
      try
      {
        dbSyssStatsList.put(dbLine, new StatCollector(dbLine, DBUSERNAME, DBPASSWORD, CKHDataSource, 2, ckhQueue));
        lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Starting system stats thread for " + dbLine
        );
        
        ((Thread)dbSyssStatsList.get(dbLine)).start();
      }
      catch (Exception e)
      {
        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Error running system stats thread for " + dbLine
        );
        
        e.printStackTrace();
      }
    }
  }
  
  private static void processCKHQueueConsumers()
  {
    for (int i = 0; i < CKHQUEUECONSUMERS; i++) {
      if ((ckhQueueThreads[i] == null) || (!ckhQueueThreads[i].isAlive()))
      {
        lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Starting clickhouse queue consumer #" + i
        );
        
        ckhQueueThreads[i] = new CkhQueueConsumer(ckhQueue, CKHDataSource);
        ckhQueueThreads[i].start();
      }
    }
  }
  
  public static void main(String[] args)
    throws InterruptedException
  {
    if (!processProperties(PROPERTIESFILENAME)) {
      System.exit(1);
    }
    configureLogger();
    
    lg = new SL4JLogger();
    
    ckhQueueThreads = new Thread[CKHQUEUECONSUMERS];
    
    CKHDataSource = initDataSource();
    if (CKHDataSource == null) {
      System.exit(2);
    }
    lg.LogWarn(DATEFORMAT.format(LocalDateTime.now()) + "\t"+"Starting");
    while(true)
    {
      processCKHQueueConsumers();
      
      oraDBList = getOraDBList();
      for (int i = 0; i < oraDBList.size(); i++)
      {
        String dbLine = (String)oraDBList.get(i);
        if (GATHERSESSIONS) {
          processSessions(dbLine);
        }
        if (GATHERSESSTATS) {
          processSessionStats(dbLine);
        }
        if (GATHERSYSSTATS) {
          processSystemRoutines(dbLine);
        }
      }
      TimeUnit.SECONDS.sleep(SECONDSTOSLEEP);
    }
  }
}
