package oraperf;

import java.util.List;

public class OraCkhMsg {
  
    public int dataType; 
    public long currentDateTime;
    public String dbUniqueName;
    public String dbHostaName;
    public List dataList;
    
   
    public OraCkhMsg( int type, long dt, String db, String host, List lst ){
        currentDateTime         = dt;
        dataList                = lst;
        dbUniqueName            = db;
        dbHostaName             = host;
        dataType                = type;
    }
}
