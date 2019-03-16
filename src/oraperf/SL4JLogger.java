package oraperf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SL4JLogger {
    
    
    private final Logger slf4jLogger = LoggerFactory.getLogger(SL4JLogger.class);
    
    public void LogWarn(String message) {

        slf4jLogger.warn(message);
    }
    
    public void LogInfo(String message) {

        slf4jLogger.info(message);
    }
    
    public void LogTrace(String message){
        
        slf4jLogger.trace(message);
        
    }
    public void LogError(String message) {

        slf4jLogger.error(message);
    }    
    
}
