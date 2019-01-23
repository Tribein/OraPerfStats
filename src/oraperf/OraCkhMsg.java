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

import java.sql.ResultSet;

public class OraCkhMsg {
    public long currentDateTime;
    public String currentDate;
    public ResultSet oracleResultSet;
    public int dataType;
    
    public OraCkhMsg( int data, long curDT, String curD, ResultSet oraRS){
        currentDateTime = curDT;
        currentDate = curD;
        oracleResultSet = oraRS;
        dataType = data;
    }
}
