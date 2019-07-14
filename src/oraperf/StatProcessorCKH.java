package oraperf;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class StatProcessorCKH
  extends Thread implements Configurable
{
  SL4JLogger lg;
  private ComboPooledDataSource ckhDataSource;
  private final int dataType;
  private final long dataTS;
  private final List dataList;
  private final String dbUniqueName;
  private final String dbHostName;
  private final String CKHINSERTSESSIONSQUERY = "insert into sessions_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  private final String CKHINSERTSYSSTATSQUERY = "insert into sysstats_buffer values (?,?,?,?)";
  private final String CKHINSERTSESSTATSQUERY = "insert into sesstats_buffer values (?,?,?,?,?,?)";
  private final String CKHINSERTSQLTEXTSQUERY = "insert into sqltexts_buffer values (?,?)";
  private final String CKHINSERTSQLPLANSQUERY = "insert into sqlplans_buffer values (?,?)";
  private final String CKHINSERTSQLSTATSQUERY = "insert into sqlstats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  private final String CKHINSERTSTATNAMESQUERY = "insert into statnames_buffer values (?,?,?)";
  private final String CKHINSERTIOFILESTATSQUERY = "insert into iofilestats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  private final String CKHINSERTIOFUNCTIONSTATSQUERY = "insert into iofunctionstats_buffer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  private final String CKHINSERTFILESSIZEQUERY = "insert into dbfiles_buffer values(?,?,?,?,?,?,?,?,?,?)";
  private final String CKHINSERTSEGMENTSSIZEQUERY = "insert into segments_buffer values(?,?,?,?,?,?,?,?,?)";
  
  public StatProcessorCKH(int inpType, long inpTS, String inpDBName, String inpDBHost, ComboPooledDataSource ckhDS, List inpList)
  {
    dbUniqueName        = inpDBName;
    dbHostName          = inpDBHost;
    dataList            = inpList;
    dataType            = inpType;
    ckhDataSource       = ckhDS;
    dataTS              = inpTS;
  }
  
  public void processIOFileStats(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1, dbUniqueName);
        prep.setString(2, dbHostName);
        prep.setLong(3, currentDateTime);
        prep.setString(4, (String)row.get(0));
        prep.setString(5, (String)row.get(1));
        prep.setLong(6, ((long)row.get(2)));
        prep.setLong(7, ((long)row.get(3)));
        prep.setLong(8, ((long)row.get(4)));
        prep.setLong(9, ((long)row.get(5)));
        prep.setLong(10, ((long)row.get(6)));
        prep.setLong(11, ((long)row.get(7)));
        prep.setLong(12, ((long)row.get(8)));
        prep.setLong(13, ((long)row.get(9)));
        prep.setLong(14, ((long)row.get(10)));
        prep.setLong(15, ((long)row.get(11)));
        prep.setLong(16, ((long)row.get(12)));
        prep.setLong(17, ((long)row.get(13)));
        prep.setLong(18, ((long)row.get(14)));
        prep.setLong(19, ((long)row.get(15)));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error submitting io file stats data to ClickHouse!"
      );
      
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
        prep.setString(1, dbUniqueName);
        prep.setString(2, dbHostName);
        prep.setLong(3, currentDateTime);
        prep.setString(4, (String)row.get(0));
        prep.setString(5, (String)row.get(1));
        prep.setLong(6, ((long)row.get(2)));
        prep.setLong(7, ((long)row.get(3)));
        prep.setLong(8, ((long)row.get(4)));
        prep.setLong(9, ((long)row.get(5)));
        prep.setLong(10, ((long)row.get(6)));
        prep.setLong(11, ((long)row.get(7)));
        prep.setLong(12, ((long)row.get(8)));
        prep.setLong(13, ((long)row.get(9)));
        prep.setLong(14, ((long)row.get(10)));
        prep.setLong(15, ((long)row.get(11)));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error submitting io function stats data to ClickHouse!"
      );
      
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
        prep.setString(1, dbUniqueName);
        prep.setString(2, dbHostName);
        prep.setLong(3, currentDateTime);
        prep.setInt(4, ((int)row.get(0)));
        prep.setInt(5, ((int)row.get(1)));
        prep.setString(6, (String)row.get(2));
        prep.setString(7, (String)row.get(3));
        prep.setString(8, (String)row.get(4));
        prep.setString(9, (String)row.get(5));
        prep.setString(10, (String)row.get(6));
        prep.setString(11, (String)row.get(7));
        prep.setString(12, (String)row.get(8));
        prep.setString(13, (String)row.get(9));
        prep.setInt(14, ((int)row.get(10)));
        prep.setString(15, (String)row.get(11));
        prep.setLong(16, ((long)row.get(12)));
        prep.setFloat(17, ((Float)row.get(13)));
        prep.setString(18, (String)row.get(14));
        prep.setLong(19, ((long)row.get(15)));
        prep.setInt(20, ((int)row.get(16)));
        prep.setLong(21, ((long)row.get(17)));
        prep.setInt(22, ((int)row.get(18)));
        prep.setBigDecimal(23, ((BigDecimal)row.get(19)));
        prep.setLong(24, ((long)row.get(20)));
        prep.setLong(25, ((long)row.get(21)));
        prep.setLong(26, ((long)row.get(22)));
        prep.setLong(27, ((long)row.get(23)));
        prep.setLong(28, ((long)row.get(24)));
        prep.setLong(29, ((long)row.get(25)));
        prep.setLong(30, ((long)row.get(26)));
        prep.setString(31, (String)row.get(27));
        prep.setString(32, (String)row.get(28));
        prep.setString(33, (String)row.get(29));
        prep.setString(34, (String)row.get(30));
        //prep.setInt(35, (int) row.get(31)); //SQL_CHILD_NUMBER
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + 
              "\t" + dbUniqueName + "\t" + dbHostName + "\tError submitting sessions data to ClickHouse!"
      );
      
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
        prep.setString(1, dbUniqueName);
        prep.setLong(2, currentDateTime);
        prep.setInt(3, ((int)row.get(0)));
        prep.setLong(4, ((long)row.get(1)));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error processing system statistics!"
      );
      
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
        prep.setString(1, dbUniqueName);
        prep.setLong(2, currentDateTime);
        prep.setInt(3, ((int)row.get(0)));
        prep.setInt(4, ((int)row.get(1)));
        prep.setInt(5, ((int)row.get(2)));
        prep.setBigDecimal(6, ((BigDecimal)row.get(3)));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error processing session statistics!"
      );
      
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
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error processing sql texts!"
      );
      
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
        prep.setString(1, dbUniqueName);
        prep.setInt(2, ((int)row.get(0)));
        prep.setString(3, (String)row.get(1));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error processing statistics names!"
      );
      
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
        prep.setString(1, dbUniqueName);
        prep.setString(2, dbHostName);
        prep.setLong(3, currentDateTime);
        prep.setString(4, (String) row.get(0) );
        prep.setLong(5, (Long) row.get(1) );
        prep.setInt(6, (int) row.get(2) );
        prep.setLong(7, (Long) row.get(3) );
        prep.setLong(8, (Long) row.get(4) );
        prep.setLong(9, (Long) row.get(5) );
        prep.setLong(10, (Long) row.get(6) );
        prep.setInt(11, (int) row.get(7) );
        prep.setInt(12, (int) row.get(8) );
        prep.setInt(13, (int) row.get(9) );
        prep.setInt(14, (int) row.get(10) );
        prep.setLong(15, (Long) row.get(11) );
        prep.setLong(16, (Long) row.get(12) );
        prep.setLong(17, (Long) row.get(13) );
        prep.setLong(18, (Long) row.get(14) );
        prep.setInt(19, (int) row.get(15) );
        prep.setLong(20, (Long) row.get(16) );
        prep.setLong(21, (Long) row.get(17) );
        prep.setLong(22, (Long) row.get(18) );
        prep.setLong(23, (Long) row.get(19) );
        prep.setLong(24, (Long) row.get(20) );
        prep.setBigDecimal(25, (BigDecimal) row.get(21) );
        prep.setBigDecimal(26, (BigDecimal) row.get(22) );
        prep.setBigDecimal(27, (BigDecimal) row.get(23) );
        prep.setLong(28, (Long) row.get(24) );
        prep.setLong(29, (Long) row.get(25) );
        prep.setLong(30, (Long) row.get(26) );
        prep.setLong(31, (Long) row.get(27) );
        prep.setLong(32, (Long) row.get(28) );
        prep.setLong(33, (Long) row.get(29) );
        prep.setLong(34, (Long) row.get(30) );
        prep.setLong(35, (Long) row.get(31) );
        prep.setBigDecimal(36, (BigDecimal) row.get(32) ); 
        prep.setInt(37, (int) row.get(33) );
        prep.setString(38, (String) row.get(34) );
        prep.setString(39, (String) row.get(35) );
        prep.setInt(40, (int) row.get(36) );
        prep.setString(41, (String) row.get(37) );
        prep.setString(42, (String) row.get(38) );
        prep.setInt(43, (int) row.get(39) );
        prep.setInt(44, (int) row.get(40) );
        prep.setBigDecimal(45, (BigDecimal) row.get(41) );
        prep.setBigDecimal(46, (BigDecimal) row.get(42) );
        prep.setBigDecimal(47, (BigDecimal) row.get(43) );
        prep.setBigDecimal(48, (BigDecimal) row.get(44) );
        prep.setBigDecimal(49, (BigDecimal) row.get(45) );
        prep.setBigDecimal(50, (BigDecimal) row.get(46) );
        prep.setBigDecimal(51, (BigDecimal) row.get(47) );
        prep.setBigDecimal(52, (BigDecimal) row.get(48) );
        prep.setBigDecimal(53, (BigDecimal) row.get(49) );
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error processing sql stats!"
      );
      
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
        prep.setLong(2, ((long)row.get(1)));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error processing sql plans!"
      );
      
      e.printStackTrace();
    }
  }
  
  public void processFilesSize(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1,   dbUniqueName);
        prep.setString(2,   dbHostName);
        prep.setLong(3,     currentDateTime);
        prep.setInt(4,      ((int)row.get(0)));
        prep.setInt(5,      ((int)row.get(1)));
        prep.setString(6,   ((String)row.get(2)));
        prep.setLong(7,     ((long)row.get(3)));        
        prep.setString(8,   ((String)row.get(4)));
        prep.setLong(9,     ((long)row.get(5)));        
        prep.setString(10,  ((String)row.get(6)));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error processing file size!"
      );
      
      e.printStackTrace();
    }
  }  

  public void processSegmentsSize(PreparedStatement prep, List lst, long currentDateTime)
  {
    List row = new ArrayList();
    try
    {
      for (int i = 0; i < lst.size(); i++)
      {
        row = (List)lst.get(i);
        prep.setString(1,   dbUniqueName);
        prep.setString(2,   dbHostName);
        prep.setLong(3,     currentDateTime);
        prep.setString(4,   ((String)row.get(0)));
        prep.setString(5,   ((String)row.get(1)));
        prep.setString(6,   ((String)row.get(2)));
        prep.setString(7,   ((String)row.get(3)));    
        prep.setString(8,   ((String)row.get(4)));
        prep.setLong(9,     ((long)row.get(5)));        ;        
        prep.addBatch();
      }
      prep.executeBatch();
    }
    catch (SQLException e)
    {
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t" + 
              dbUniqueName + "\t" + dbHostName + "\t"+"Error processing segment size!"
      );
      
      e.printStackTrace();
    }
  }    
  @Override
  public void run()
  {
    lg = new SL4JLogger();
    try{
      Connection ckhConnection = ckhDataSource.getConnection();
      PreparedStatement ckhPreparedStatement = null;
      switch (dataType)
      {
      case RSSESSIONWAIT: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSESSIONSQUERY);
        processSessions(ckhPreparedStatement, dataList, dataTS);
        break;
      case RSIOFILESTAT: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTIOFILESTATSQUERY);
        processIOFileStats(ckhPreparedStatement, dataList, dataTS);
        break;
      case RSIOFUNCTIONSTAT: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTIOFUNCTIONSTATSQUERY);
        processIOFunctionStats(ckhPreparedStatement, dataList, dataTS);
        break;
      case RSSESSIONSTAT: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSESSTATSQUERY);
        processSesStats(ckhPreparedStatement, dataList, dataTS);
        break;
      case RSSYSTEMSTAT: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSYSSTATSQUERY);
        processSysStats(ckhPreparedStatement, dataList, dataTS);
        break;
      case RSSQLTEXT: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSQLTEXTSQUERY);
        processSQLTexts(ckhPreparedStatement, dataList);
        break;
      case RSSTATNAME: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSTATNAMESQUERY);
        processStatNames(ckhPreparedStatement, dataList);
        break;
      case RSSQLPHV: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSQLPLANSQUERY);
        processSQLPlans(ckhPreparedStatement, dataList);
        break;
      case RSSQLSTAT: 
        ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSQLSTATSQUERY);
        processSQLStats(ckhPreparedStatement, dataList,dataTS);
        break;
      case RSSEGMENTSTAT: 
          break;
      case RSFILESSIZE:
          ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTFILESSIZEQUERY);
          processFilesSize(ckhPreparedStatement,dataList,dataTS);
          break;
      case RSSEGMENTSSIZE:
          ckhPreparedStatement = ckhConnection.prepareStatement(CKHINSERTSEGMENTSSIZEQUERY);
          processSegmentsSize(ckhPreparedStatement,dataList, dataTS);
          break;
      default: 
        lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
                "Unsupported run type: " + dataType
        );
      }
      if ((ckhPreparedStatement != null) && (!ckhPreparedStatement.isClosed())) {
        ckhPreparedStatement.close();
      }
      if ((ckhConnection != null) && (!ckhConnection.isClosed())) {
        ckhConnection.close();
      }
      ckhDataSource = null;
    } catch (SQLException e){
      lg.LogError(DATEFORMAT.format(LocalDateTime.now()) + "\t"+
            "Cannot connect to ClickHouse server!"
      );
      
      e.printStackTrace();
    }
  }
}
