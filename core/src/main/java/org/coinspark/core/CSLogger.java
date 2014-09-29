/* 
 * SparkBit's Bitcoinj
 *
 * Copyright 2014 Coin Sciences Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.coinspark.core;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSLogger {
    
    public enum CSLevel {
        
        ERROR("ERR!"),
        WARNING("WRN!"),
        INFO("I   "),
        DEBUG("DBG "),
        TRACE("T   ");
        
        private String abbr;
        
        CSLevel(String abbr) {
            this.abbr = abbr;
        }
        
        public String getAbbr() {
            return abbr;
        }       
    }
    
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String fileName;
    private static final Logger log = LoggerFactory.getLogger(CSLogger.class);
    
    public CSLogger(String FileName)
    {
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));        
        fileName=FileName;
    }
    
    public CSLogger(String FileName,boolean ShowDate)
    {
        this(FileName);
        if(!ShowDate)
        {
            sdf=null;
        }
    }
    
    public String line(String Message, CSLevel Level, int depth)
    {
        String result="";

        if(sdf != null)
        {
            result += sdf.format(new Date()) + "\t";
        }
        result += Level.getAbbr() + "\t";
        result += Message;
        
        if(depth>0)
        {
            result += "\t";
            StackTraceElement ste=Thread.currentThread().getStackTrace()[depth];
            result += "[" + ste.getClassName() + "." + ste.getMethodName() + ", line " + ste.getLineNumber() +"]";
        }
        
        return result;
    }

    public void debug(String Message)
    {
        String output=line(Message, CSLevel.DEBUG,3);
        if(fileName != null)
        {
            try {
                FileUtils.writeStringToFile(new File(fileName), output + "\n",true);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(CSLogger.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else
        {
            log.info(output);
        }
    }
    
    public void trace(String Message)
    {
        String output=line(Message, CSLevel.TRACE,3);
        if(fileName != null)
        {
            try {
                FileUtils.writeStringToFile(new File(fileName), output + "\n",true);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(CSLogger.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else
        {
            log.info(output);
        }
    }
    
    public void error(String Message)
    {
        String output=line(Message, CSLevel.ERROR,3);
        if(fileName != null)
        {
            try {
                FileUtils.writeStringToFile(new File(fileName), output + "\n",true);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(CSLogger.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else
        {
            log.error(output);
        }
    }

    public void warning(String Message,boolean trace)
    {
        int depth=0;
        if(trace){
            depth=3;
        }
        String output=line(Message, CSLevel.WARNING,depth);
        if(fileName != null)
        {
            try {
                FileUtils.writeStringToFile(new File(fileName), output + "\n",true);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(CSLogger.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else
        {
            log.error(output);
        }
    }
    
    public void warning(String Message)
    {
        warning(Message,false);
    }
    
    public void info(String Message,boolean trace)
    {
        int depth=0;
        if(trace){
            depth=3;
        }
        
        String output=line(Message, CSLevel.INFO,depth);
        if(fileName != null)
        {
            try {
                FileUtils.writeStringToFile(new File(fileName), output + "\n",true);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(CSLogger.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else
        {
            log.error(output);
        }
    }
    
    public void info(String Message)
    {
        info(Message,false);
    }
    
}
