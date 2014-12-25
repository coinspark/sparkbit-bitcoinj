/*
 * Copyright 2014 mike.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.coinspark.wallet;

import com.google.bitcoin.core.Wallet;
import com.google.bitcoin.utils.Threading;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.coinspark.core.CSLogger;
import org.coinspark.core.CSUtils;
import org.coinspark.protocol.CoinSparkMessage;
import org.coinspark.protocol.CoinSparkMessagePart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mike
 */
public class CSMessageDatabase {
 
    private static final Logger log = LoggerFactory.getLogger(CSAssetDatabase.class);
    private CSLogger csLog;
    
    protected final ReentrantLock lock = Threading.lock("assetdb");
    
    private static final String MESSAGE_DB_SUFFIX = "messages.csdb";
    private static final String MESSAGE_DIR_SUFFIX = ".csmessages";
        
    private HashMap<String, CSMessage> mapByTxID=new HashMap<String, CSMessage>(16, 0.9f);
    
    private String fileName;
    private String dirName;
    private int fileSize;
    private Wallet wallet;

    public CSMessageDatabase(String FilePrefix,CSLogger CSLog,Wallet ParentWallet)
    {
        dirName = FilePrefix + MESSAGE_DIR_SUFFIX+File.separator;
        fileName = dirName + MESSAGE_DB_SUFFIX;
        csLog=CSLog;
        wallet=ParentWallet;
        
        File theDir = new File(dirName);

        if (!theDir.exists()) 
        {
            try{
                theDir.mkdir();
             } catch(SecurityException ex){
                log.error("Message DB: Cannot create files directory" + ex.getClass().getName() + " " + ex.getMessage());                
                return;
             }        
        }        
        
        load();
    }

/**
 * Returns directory name where files should be stored 
 * @return 
 */    
    
    public String getDirName()
    {
        return dirName;
    }
    
/**
 * Returns number of messages in the database
 * @return 
 */    
    
    public int getSize()
    {
        return mapByTxID.size();
    }

    
    private void load()
    {        
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(fileName, "r");
        } catch (FileNotFoundException ex) {
            log.info("Message DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            return;
        }
                
        fileSize=0;
        try {  
            fileSize = (int)aFile.length();                    
        } catch (IOException ex) {
            log.error("Message DB: Cannot get file size " + ex.getClass().getName() + " " + ex.getMessage());                
            return;
        }
            
        mapByTxID.clear();
                
        int off=0;
        int size,code;
        
        byte[] Serialized=new byte[CSMessage.serializedSize];
        while(off+CSMessage.serializedSize<=fileSize)
        {
            if(!CSUtils.readFromFileToBytes(aFile, Serialized))
            {
                log.error("Message DB: Cannot read file");                
                return;
            }            

            CSMessage message=new CSMessage();
            message.set(Serialized, 0, off, dirName);
            mapByTxID.put(CSUtils.byte2Hex(Arrays.copyOf(Serialized, 32)), message);
            
            off+=CSMessage.serializedSize;            
        }        
        
        if(off<fileSize)
        {
            log.error("Message DB: Corrupted file, on " + off);                
            fileSize=off;
            return;
        }        
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Message DB: Cannot close file " + ex.getClass().getName() + " " + ex.getMessage());                
        }        
        
        log.info("Message DB: File opened: " + fileName);                
    }
    
    private boolean saveMessage(String TxID,CSMessage Message,int Offset)
    {
        RandomAccessFile aFile;
        
        try {
            aFile = new RandomAccessFile(fileName, "rw");
        } catch (FileNotFoundException ex) {
            log.info("Message DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        try {
            aFile.seek(Offset);
        } catch (IOException ex) {
            log.error("Message DB: Cannot set file position " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        try {
            aFile.write(Message.serialize());
        } catch (IOException ex) {
            log.error("Message DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }

        if(Offset>=fileSize)
        {
            fileSize=Offset+CSMessage.serializedSize;
        }
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Message DB: Cannot close file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }        
        
        return true;
    }
    
    public boolean insertReceivedMessage(String TxID,int countOutputs,CoinSparkMessage Message,String [] Addresses)
    {
        if(Message == null)
        {
            return false;
        }
        
        if(mapByTxID.get(TxID) != null)
        {
            log.info("Message DB: TxID " + TxID + " already in the database");                            
            return true;
        }
        
        boolean result=true;
        CSMessage message=new CSMessage();
        
        lock.lock();
        try {                 
            
            result=message.set(TxID, countOutputs, Message,  Addresses, fileSize, dirName);
            
            if(result)
            {
                mapByTxID.put(TxID,message);

                if(!saveMessage(TxID, message, fileSize))
                {
                    mapByTxID.remove(TxID);
                    result=false;
                }
                else
                {
                    log.info("Message DB: Tx " + TxID + " inserted");                
                }
            }
            else
            {
                log.info("Message DB: Cannot insert message for Tx " + TxID + " inserted");                                
            }
        } finally {
            lock.unlock();
        }

        if(result)
        {
            csLog.info("Message DB: Inserted new Tx: " + TxID + ", State: " + message.messageState);
        }
        
        return result;
    }

    public boolean insertSentMessage(String TxID,int countOutputs,CoinSparkMessage Message,CoinSparkMessagePart [] MessageParts,CSMessage.CSMessageParams MessageParams)
    {
        if(Message == null)
        {
            return false;
        }
        
        if(mapByTxID.get(TxID) != null)
        {
            log.info("Message DB: TxID " + TxID + " already in the database");                            
            return true;
        }
        
        boolean result=true;
        CSMessage message=new CSMessage();
        
        lock.lock();
        try {                 
            
            result=message.set(TxID, countOutputs, Message, MessageParts,MessageParams, fileSize, dirName);
            
            if(result)
            {
                mapByTxID.put(TxID,message);

                if(!saveMessage(TxID, message, fileSize))
                {
                    mapByTxID.remove(TxID);
                    result=false;
                }
                else
                {
                    log.info("Message DB: Tx " + TxID + " inserted");                
                }
            }
            else
            {
                log.info("Message DB: Cannot insert message for Tx " + TxID + " inserted");                                
            }
        } finally {
            lock.unlock();
        }

        if(result)
        {
            csLog.info("Message DB: Inserted new Tx: " + TxID + ", State: " + message.messageState);
        }
        
        return result;
    }

    public boolean updateMessage(String TxID,CSMessage Message)
    {
        if(Message == null)
        {
            return false;
        }
        
        if(mapByTxID.get(TxID) == null)
        {
            log.info("Message DB: TxID " + TxID + " not in the database");                            
            return false;
        }
        
        boolean result=true;
        
        lock.lock();
        try {                        
            mapByTxID.put(TxID,Message);
            
            if(!saveMessage(TxID, Message, Message.getOffsetInDB()))
            {
                mapByTxID.remove(TxID);
                result=false;
            }
            else
            {
                log.info("Message DB: Tx " + TxID + " inserted");                
            }
        } finally {
            lock.unlock();
        }
        
        csLog.info("Message DB: Updated Tx: " + TxID + ", State: " + Message.messageState);
        return result;
    }
    
    
    CSMessage getMessage(String TxID)
    {
        if(TxID == null)
        {
            return null;
        }
        
        CSMessage message=mapByTxID.get(TxID);                
                
        if(message == null)
        {
            return null;
        }
        
        return message.load();
    }

    private boolean retrievalInProgress=false;
    
    public void retrieveMessages()
    {
        if(retrievalInProgress){
            return;            
        }
        
        retrievalInProgress=true;

                                
        for (Map.Entry<String, CSMessage> entryMessage : mapByTxID.entrySet()) 
        {
            CSMessage message=entryMessage.getValue();
                        
            if(message.mayBeRetrieve(wallet))
            {
                updateMessage(entryMessage.getKey(), message);
                CSEventBus.INSTANCE.postAsyncEvent(CSEventType.MESSAGE_RETRIEVAL_COMPLETED, message.getTxID());
            }
            
        }        

        retrievalInProgress=false;
    }
    
}
