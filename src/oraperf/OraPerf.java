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

/**
 *
 * @author Tribein
 */
public class OraPerf {

    /**
     * @param args the command line arguments
     */
    private static final String dbListFileName = "db.lst";
    private static final int secondsToSleep = 60;
    public static void main(String[] args) throws InterruptedException {
        Map <String, Thread> dbList = new HashMap();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("dd.MM.YYYY HH:mm:ss");
        String dbLine; 
        System.out.println(dateFormat.format(LocalDateTime.now()) + "\t" + "Starting");
        while(true) /*for(int i=0; i<1; i++)*/{
            try{
                File dbListFile = new File(dbListFileName);
                Scanner fileScanner = new Scanner(dbListFile);
                while ( fileScanner.hasNext()){
                    dbLine = fileScanner.nextLine();
                    if ( !dbList.containsKey(dbLine) || !dbList.get(dbLine).isAlive() ){
                        try{
                            dbList.put(dbLine, new StatCollectorCKH(dbLine));
                            System.out.println(dateFormat.format(LocalDateTime.now()) + "\t" + "Adding new database for monitoring: "+dbLine);
                            dbList.get(dbLine).start();
                        }catch(Exception e){
                            System.out.println(dateFormat.format(LocalDateTime.now()) + "\t" + "Error running thread for "+dbLine);
                            e.printStackTrace();
                        }
                    }
                }            
            } catch (FileNotFoundException e){
                System.out.println(dateFormat.format(LocalDateTime.now()) + "\t" +"Error reading database list!");
                e.printStackTrace();
                System.exit(2);
            }
                TimeUnit.SECONDS.sleep(secondsToSleep);
        }
        //Done
    }
    
}
