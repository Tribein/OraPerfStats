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
