package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class StatProcessorCKH
  extends Thread
{
  SL4JLogger lg;
  private final int RSSESSIONWAIT = 0;
  private final int RSSESSIONSTAT = 1;
  private final int RSSYSTEMSTAT = 2;
  private final int RSSQLSTAT = 3;
  private final int RSSEGMENTSTAT = 4;
  private final int RSSQLPHV = 5;
  private final int RSSQLTEXT = 6;
  private final int RSSTATNAME = 7;
  private final int RSIOFILESTAT = 8;
  private final int RSIOFUNCTIONSTAT = 9;
  private final DateTimeFormatter DATEFORMAT = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss");
  private ComboPooledDataSource ckhDataSource;
  private final int dataType;
  private final long dataTS;
  private final List dataList;
  private final String dbUniqueName;
  private final String dbHostName;
  private final String CKHINSERTSESSIONSQUERY = "insert into sessions_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  private final String CKHINSERTSYSSTATSQUERY = "insert into sysstats_buffer values (?,?,?,?)";
  private final String CKHINSERTSESSTATSQUERY = "insert into sesstats_buffer values (?,?,?,?,?,?)";
  private final String CKHINSERTSQLTEXTSQUERY = "insert into sqltexts_buffer values (?,?)";
  private final String CKHINSERTSQLPLANSQUERY = "insert into sqlplans_buffer values (?,?)";
  private final String CKHINSERTSQLSTATSQUERY = "insert into sqlstats_buffer values ()";
  private final String CKHINSERTSTATNAMESQUERY = "insert into statnames_buffer values (?,?,?)";
  private final String CKHINSERTIOFILESTATSQUERY = "insert into iofilestats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  private final String CKHINSERTIOFUNCTIONSTATSQUERY = "insert into iofunctionstats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  
  public StatProcessorCKH(int inpType, long inpTS, String inpDBName, String inpDBHost, ComboPooledDataSource ckhDS, List inpList)
  {
    this.dbUniqueName = inpDBName;
    this.dbHostName = inpDBHost;
    this.dataList = inpList;
    
    this.dataType = inpType;
    this.ckhDataSource = ckhDS;
    this.dataTS = inpTS;
  }
  
  public void processIOFileStats(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, this.dbUniqueName);
        prep.setString(2, this.dbHostName);
        prep.setLong(3, currentDateTime);
        prep.setString(4, (String)row.get(0));
        prep.setString(5, (String)row.get(1));
        prep.setLong(6, ((Long)row.get(2)).longValue());
        prep.setLong(7, ((Long)row.get(3)).longValue());
        prep.setLong(8, ((Long)row.get(4)).longValue());
        prep.setLong(9, ((Long)row.get(5)).longValue());
        prep.setLong(10, ((Long)row.get(6)).longValue());
        prep.setLong(11, ((Long)row.get(7)).longValue());
        prep.setLong(12, ((Long)row.get(8)).longValue());
        prep.setLong(13, ((Long)row.get(9)).longValue());
        prep.setLong(14, ((Long)row.get(10)).longValue());
        prep.setLong(15, ((Long)row.get(11)).longValue());
        prep.setLong(16, ((Long)row.get(12)).longValue());
        prep.setLong(17, ((Long)row.get(13)).longValue());
        prep.setLong(18, ((Long)row.get(14)).longValue());
        prep.setLong(19, ((Long)row.get(15)).longValue());
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError submitting io file stats data to ClickHouse!");
      
      e.printStackTrace();
    }
  }
  
  public void processIOFunctionStats(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, this.dbUniqueName);
        prep.setString(2, this.dbHostName);
        prep.setLong(3, currentDateTime);
        prep.setString(4, (String)row.get(0));
        prep.setString(5, (String)row.get(1));
        prep.setLong(6, ((Long)row.get(2)).longValue());
        prep.setLong(7, ((Long)row.get(3)).longValue());
        prep.setLong(8, ((Long)row.get(4)).longValue());
        prep.setLong(9, ((Long)row.get(5)).longValue());
        prep.setLong(10, ((Long)row.get(6)).longValue());
        prep.setLong(11, ((Long)row.get(7)).longValue());
        prep.setLong(12, ((Long)row.get(8)).longValue());
        prep.setLong(13, ((Long)row.get(9)).longValue());
        prep.setLong(14, ((Long)row.get(10)).longValue());
        prep.setLong(15, ((Long)row.get(11)).longValue());
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError submitting io function stats data to ClickHouse!");
      
      e.printStackTrace();
    }
  }
  
  public void processSessions(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, this.dbUniqueName);
        prep.setString(2, this.dbHostName);
        prep.setLong(3, currentDateTime);
        prep.setInt(4, ((Integer)row.get(0)).intValue());
        prep.setInt(5, ((Integer)row.get(1)).intValue());
        prep.setString(6, (String)row.get(2));
        prep.setString(7, (String)row.get(3));
        prep.setString(8, (String)row.get(4));
        prep.setString(9, (String)row.get(5));
        prep.setString(10, (String)row.get(6));
        prep.setString(11, (String)row.get(7));
        prep.setString(12, (String)row.get(8));
        prep.setString(13, (String)row.get(9));
        prep.setInt(14, ((Integer)row.get(10)).intValue());
        prep.setString(15, (String)row.get(11));
        prep.setLong(16, ((Long)row.get(12)).longValue());
        prep.setFloat(17, ((Float)row.get(13)).floatValue());
        prep.setString(18, (String)row.get(14));
        prep.setLong(19, ((Long)row.get(15)).longValue());
        prep.setInt(20, ((Integer)row.get(16)).intValue());
        prep.setLong(21, ((Long)row.get(17)).longValue());
        prep.setInt(22, ((Integer)row.get(18)).intValue());
        prep.setLong(23, ((Long)row.get(19)).longValue());
        prep.setLong(24, ((Long)row.get(20)).longValue());
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError submitting sessions data to ClickHouse!");
      
      e.printStackTrace();
    }
  }
  
  public void processSysStats(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, this.dbUniqueName);
        prep.setLong(2, currentDateTime);
        prep.setInt(3, ((Integer)row.get(0)).intValue());
        prep.setLong(4, ((Long)row.get(1)).longValue());
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError processing system statistics!");
      
      e.printStackTrace();
    }
  }
  
  public void processSesStats(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, this.dbUniqueName);
        prep.setLong(2, currentDateTime);
        prep.setInt(3, ((Integer)row.get(0)).intValue());
        prep.setInt(4, ((Integer)row.get(1)).intValue());
        prep.setInt(5, ((Integer)row.get(2)).intValue());
        prep.setLong(6, ((Long)row.get(3)).longValue());
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError processing session statistics!");
      
      e.printStackTrace();
    }
  }
  
  public void processSQLTexts(PreparedStatement prep, List lst)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, (String)row.get(0));
        prep.setString(2, (String)row.get(1));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError processing sql texts!");
      
      e.printStackTrace();
    }
  }
  
  public void processStatNames(PreparedStatement prep, List lst)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, this.dbUniqueName);
        prep.setInt(2, ((Integer)row.get(0)).intValue());
        prep.setString(3, (String)row.get(1));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError processing statistics names!");
      
      e.printStackTrace();
    }
  }
  
  public void processSQLStats(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError processing sql stats!");
      
      e.printStackTrace();
    }
  }
  
  public void processSQLPlans(PreparedStatement prep, List lst)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, (String)row.get(0));
        prep.setLong(2, ((Long)row.get(1)).longValue());
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\t" + this.dbUniqueName + "\t" + this.dbHostName + "\tError processing sql plans!");
      
      e.printStackTrace();
    }
  }
  
  public void run()
  {
    this.lg = new SL4JLogger();
    try
    {
      Connection ckhConnection = this.ckhDataSource.getConnection();
      PreparedStatement ckhPreparedStatement = null;
      switch (this.dataType)
      {
      case 0: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into sessions_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        processSessions(ckhPreparedStatement, this.dataList, this.dataTS);
        break;
      case 8: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into iofilestats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        processIOFileStats(ckhPreparedStatement, this.dataList, this.dataTS);
        break;
      case 9: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into iofunctionstats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        processIOFunctionStats(ckhPreparedStatement, this.dataList, this.dataTS);
        break;
      case 1: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into sesstats_buffer values (?,?,?,?,?,?)");
        processSesStats(ckhPreparedStatement, this.dataList, this.dataTS);
        break;
      case 2: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into sysstats_buffer values (?,?,?,?)");
        processSysStats(ckhPreparedStatement, this.dataList, this.dataTS);
        break;
      case 6: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into sqltexts_buffer values (?,?)");
        processSQLTexts(ckhPreparedStatement, this.dataList);
        break;
      case 7: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into statnames_buffer values (?,?,?)");
        processStatNames(ckhPreparedStatement, this.dataList);
        break;
      case 5: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into sqlplans_buffer values (?,?)");
        processSQLPlans(ckhPreparedStatement, this.dataList);
        break;
      case 3: 
        ckhPreparedStatement = ckhConnection.prepareStatement("insert into sqlstats_buffer values ()");
        
        break;
      case 4: 
      default: 
        this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tUnsupported run type: " + this.dataType);
      }
      if ((ckhPreparedStatement != null) && (!ckhPreparedStatement.isClosed())) {
        ckhPreparedStatement.close();
      }
      if ((ckhConnection != null) && (!ckhConnection.isClosed())) {
        ckhConnection.close();
      }
      this.ckhDataSource = null;
    }
    catch (SQLException e)
    {
      this.lg.LogError(this.DATEFORMAT.format(LocalDateTime.now()) + "\tCannot connect to ClickHouse server!");
      
      e.printStackTrace();
    }
  }
}
