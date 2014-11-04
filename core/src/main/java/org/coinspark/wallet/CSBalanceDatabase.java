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
package org.coinspark.wallet;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.TransactionInput;
import com.google.bitcoin.core.TransactionOutPoint;
import com.google.bitcoin.core.TransactionOutput;
import com.google.bitcoin.utils.Threading;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import org.coinspark.core.CSLogger;
import org.coinspark.core.CSUtils;
import org.coinspark.protocol.CoinSparkBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.Arrays;

public class CSBalanceDatabase {

    protected final ReentrantLock lock = Threading.lock("balancedb");
    
    private static final Logger log = LoggerFactory.getLogger(CSBalanceDatabase.class);
    private CSLogger csLog;
    
    private class CSBalanceEntry
    {
        public int offsetInDB;
        public BigInteger qty;
        public Date qtyChecked;
        public int qtyFailures;
        public CSBalance.CSBalanceState balanceState;
        public static final int serializedSize=16;
        
        public CSBalanceEntry()
        {
            qty=null;
            qtyChecked=null;
            qtyFailures=0;
            balanceState= CSBalance.CSBalanceState.NEVERCHECKED;
        }
        
        public CSBalanceEntry(CSBalanceEntry Model)
        {
            qty=Model.qty;
            qtyChecked=Model.qtyChecked;
            qtyFailures=Model.qtyFailures;
            balanceState= Model.balanceState;
            offsetInDB=Model.offsetInDB;            
        }
        
        public CSBalanceEntry(byte[] Serialized,int off,int OffsetInDB)
        {
            offsetInDB=OffsetInDB;            
            qty=null;
            qtyChecked=null;
            qtyChecked=new Date((long)CSUtils.littleEndianToInt(Serialized, off+8)*1000);
            qtyFailures=CSUtils.littleEndianToInt(Serialized, off+12) & 0xFFFFFF;
            switch(Serialized[off+15])
            {
                case 0: balanceState= CSBalance.CSBalanceState.ZERO;qty=new BigInteger("0");break;
                case 1: balanceState= CSBalance.CSBalanceState.VALID;qty=CSUtils.littleEndianToBigInteger(Serialized, off);break;
                case 2: balanceState= CSBalance.CSBalanceState.UNKNOWN;qty=CSUtils.littleEndianToBigInteger(Serialized, off);break;
                case 3: balanceState= CSBalance.CSBalanceState.SPENT;qty=CSUtils.littleEndianToBigInteger(Serialized, off);break;
                case 4: balanceState= CSBalance.CSBalanceState.NEVERCHECKED;qty=CSUtils.littleEndianToBigInteger(Serialized, off);qtyChecked=null;break;
                case 5: balanceState= CSBalance.CSBalanceState.REFRESH;qtyChecked=null;break;
                case 6: balanceState= CSBalance.CSBalanceState.REFRESH;qty=CSUtils.littleEndianToBigInteger(Serialized, off);qtyChecked=null;break;
                case 7: balanceState= CSBalance.CSBalanceState.DELETED;qtyChecked=null;break;
                case 8: balanceState= CSBalance.CSBalanceState.SELF;qty=CSUtils.littleEndianToBigInteger(Serialized, off);break;
                case 9: balanceState= CSBalance.CSBalanceState.CALCULATED;qty=CSUtils.littleEndianToBigInteger(Serialized, off);break;
            }
        }
        
        public CSBalanceDatabase.CSBalanceEntry setQty(BigInteger Qty,CSBalance.CSBalanceState State)
        {
            balanceState=State;
            switch(State)
            {
                case UNKNOWN:
                case SELF:
                case CALCULATED:
                case SPENT:
                    qtyChecked=new Date();
                    qtyFailures++;
                    qty=Qty;
                    break;
                case ZERO:
                case VALID:
                    qtyChecked=new Date();
                    qtyFailures=0;
                    if(Qty.longValue() == 0)
                    {
                        balanceState=CSBalance.CSBalanceState.ZERO;
                    }
                    qty=Qty;
                    break;
                case REFRESH:
                case DELETED:
                    qtyChecked=null;
                    qtyFailures=0;
                    break;
                case NEVERCHECKED:
                    qtyChecked=null;
                    qtyFailures=0;
                    qty=Qty;
                    break;
            }
            return this;
        }
        
        public byte[] serialize()
        {
            int balanceCode=0; 
            BigInteger storedQty=new BigInteger("0");
            int ts=0;
            if(qtyChecked != null)
            {
                ts=(int)(qtyChecked.getTime()/1000);
            }
            switch(balanceState){
                case NEVERCHECKED: balanceCode=4;if(qty != null){storedQty=qty;};break;
                case REFRESH:      balanceCode=5;if(qty != null){balanceCode=6;storedQty=qty;} break;
                case SPENT:        balanceCode=3;storedQty=qty;break;
                case UNKNOWN:      balanceCode=2;if(qty != null){storedQty=qty;};break;
                case SELF:         balanceCode=8;if(qty != null){storedQty=qty;};break;
                case CALCULATED:   balanceCode=9;if(qty != null){storedQty=qty;};break;
                case VALID:        balanceCode=1;storedQty=qty; break;
                case ZERO:         balanceCode=0;break;
                case DELETED:      balanceCode=7;break;
            }            
            
            byte[] s=new byte[16];
            CSUtils.littleEndianByteArray(storedQty, s, 0);
            CSUtils.littleEndianByteArray(ts, s, 8);
            CSUtils.littleEndianByteArray(CSUtils.codedSize(qtyFailures,balanceCode), s, 12);
            return s;
        }
    }
    
    private class CSTxOutEntry
    {
        public ConcurrentHashMap<Integer, CSBalanceDatabase.CSBalanceEntry> map;
        public int offsetInDB;
        public int mapSize;
        public int serializedSize=0;
        public static final int serializedRowSize=20;
        public static final int serializedHeaderSize=40;
        
        CSTxOutEntry(){}
        
        CSTxOutEntry(byte[] Serialized,int OffsetInDB)
        {            
            offsetInDB=OffsetInDB;            
            mapSize=Serialized.length/serializedRowSize;
            map=new ConcurrentHashMap<Integer, CSBalanceDatabase.CSBalanceEntry>(mapSize, 0.9f, 1);
            int off=0;
            int AssetID;
            for(int i=0;i<mapSize;i++)
            {
                AssetID=CSUtils.littleEndianToInt(Serialized, off);
                off+=4;
                map.put(AssetID,new CSBalanceDatabase.CSBalanceEntry(Serialized, off,OffsetInDB+CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize+off-4));
                off+=CSBalanceDatabase.CSBalanceEntry.serializedSize;                
            }            
        }
        
        CSTxOutEntry(int EstimatedSize,int OffsetInDB)
        {            
            offsetInDB=OffsetInDB;          
            mapSize=0;
            map=new ConcurrentHashMap<Integer, CSBalanceDatabase.CSBalanceEntry>(EstimatedSize, 0.9f, 1);
        }
        
        CSTxOutEntry(int[] AssetIDs,Map <Integer,BigInteger> balances,int OffsetInDB)
        {
            offsetInDB=OffsetInDB;            
            mapSize=AssetIDs.length+1;
            
            map=new ConcurrentHashMap<Integer, CSBalanceDatabase.CSBalanceEntry>(mapSize, 0.9f, 1);
            if(balances.containsKey(0))
            {
                map.put(0, new CSBalanceDatabase.CSBalanceEntry().setQty(balances.get(0), CSBalance.CSBalanceState.VALID));
            }
            else
            {
                map.put(0, new CSBalanceDatabase.CSBalanceEntry());
            }
            for(int i=0;i<AssetIDs.length;i++)
            {
                if(AssetIDs[i]>0)
                {
                    if(map.get(AssetIDs[i]) == null)
                    {
                        if(balances.containsKey(AssetIDs[i]))
                        {
                            long value=balances.get(AssetIDs[i]).longValue();
                            CSBalance.CSBalanceState type=CSBalance.CSBalanceState.SELF;
                            if(value<0)
                            {
                                value=-value;
                                type=CSBalance.CSBalanceState.NEVERCHECKED;
                            }
                            csLog.info("Balance DB: AssetID: " + AssetIDs[i] + ", Quantity: " + value + " (" + type + ")");
                            map.put(AssetIDs[i], new CSBalanceDatabase.CSBalanceEntry().setQty(BigInteger.valueOf(value), type));                                                            
                        }
                        else
                        {
                            map.put(AssetIDs[i], new CSBalanceDatabase.CSBalanceEntry());
                        }
                    }
                }
            }
            mapSize=map.size();
            serializedSize=mapSize*16;                    
        }

        public void add(int AssetID, CSBalanceDatabase.CSBalanceEntry Entry)
        {
            map.put(AssetID,Entry);
            mapSize++;
        }
        
        public byte[] serialize()
        {
            byte[] s=new byte[CSBalanceDatabase.CSTxOutEntry.serializedRowSize*mapSize]; 
            
            int off=0;
            for (Map.Entry<Integer, CSBalanceDatabase.CSBalanceEntry> entry : map.entrySet()) 
            {
                CSUtils.littleEndianByteArray(entry.getKey(), s, off);
                off+=4;
                CSUtils.copyArray(s, off,entry.getValue().serialize());
                off+=CSBalanceDatabase.CSBalanceEntry.serializedSize;                
            }            
            return s;
        }
        
    }    
    
    

    private CSAssetDatabase assetDB; 
    public static final String BALANCE_DB_SUFFIX = ".csbalances";
    private static final int BALANCE_DB_CODE_TXOUT        = 0;
    private static final int BALANCE_DB_CODE_DELETED      = 1;
    private static final int BALANCE_DB_CODE_NEWASSET     = 2;
    private static final int BALANCE_DB_CODE_DELETEDASSET = 3;
    private static final int BALANCE_DB_MAXIMAL_UNKNOWN_DEPTH = 5000;
    private String fileName;
    private int fileSize=0;
    private int deadSize=0;
    private HashMap<String, CSBalanceDatabase.CSTxOutEntry> map=new HashMap<String, CSBalanceDatabase.CSTxOutEntry>(16, 0.9f);
    private List<Integer> newAssets= new ArrayList<Integer>();        
    private List<Integer> deletedAssets= new ArrayList<Integer>();        
    private List<CSBalanceTransaction> trackedTransactions= new ArrayList<CSBalanceTransaction>();        
//    private List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates= new ArrayList<CSBalanceDatabase.CSBalanceUpdate>();        
    
    public CSBalanceDatabase() {}
    
    public CSBalanceDatabase(String FilePrefix,CSAssetDatabase AssetDB,CSLogger CSLog)
    {
        fileName = FilePrefix + BALANCE_DB_SUFFIX;
        assetDB=AssetDB;
        csLog=CSLog;
        load();        
    }

    
    private void load()
    {
        
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(fileName, "r");
        } catch (FileNotFoundException ex) {
            log.info("Balance DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            return;
        }
                
        fileSize=0;
        try {  
            fileSize = (int)aFile.length();                    
        } catch (IOException ex) {
            log.error("Balance DB: Cannot get file size " + ex.getClass().getName() + " " + ex.getMessage());                
            return;
        }
            
        map.clear();
        newAssets.clear();
        deletedAssets.clear();
        
        int off=0;
        deadSize=0;
        int size,code;
        
        byte[] SerializedTxOut=new byte[CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize];
        while(off+CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize<=fileSize)
        {
            if(!CSUtils.readFromFileToBytes(aFile, SerializedTxOut))
            {
                log.error("Balance DB: Cannot read file");                
                return;
            }            
            
            size=(CSUtils.littleEndianToInt(SerializedTxOut, 36)&0xFFFFFF)*CSBalanceDatabase.CSTxOutEntry.serializedRowSize;
            code=SerializedTxOut[39];
            if(off+size>fileSize)
            {
                log.error("Balance DB: Corrupted file, on " + off);                
                fileSize=off;
                return;                
            }
            
            Sha256Hash hash=new Sha256Hash(Arrays.copyOf(SerializedTxOut, 32));
            CSTransactionOutput txOut=new CSTransactionOutput(hash, CSUtils.littleEndianToInt(SerializedTxOut, 32));

            byte[] Serialized=null;
            if(size>0)
            {
                Serialized=new byte[size];
                if(!CSUtils.readFromFileToBytes(aFile, Serialized))
                {
                    log.error("Balance DB: Cannot read file");                
                    return;
                }            
            }
            
            if(!hash.equals(Sha256Hash.ZERO_HASH))
            {
                if(code == BALANCE_DB_CODE_TXOUT)
                {
                    map.put(txOut.getStrValue(),new CSBalanceDatabase.CSTxOutEntry(Serialized, off));
                }
                else
                {
                    deadSize+=CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize+size;
                }
            }
            else
            {
                int AssetID=CSUtils.littleEndianToInt(SerializedTxOut, 32);
                if(code == BALANCE_DB_CODE_NEWASSET)
                {
                    int pos=deletedAssets.indexOf(AssetID);
                    if(pos>=0)
                    {
                        deletedAssets.remove(pos);
                    }
                    newAssets.add(AssetID);
                }                
                if(code == BALANCE_DB_CODE_DELETEDASSET)
                {
                    int pos=newAssets.indexOf(AssetID);
                    if(pos>=0)
                    {
                        newAssets.remove(pos);
                    }
                    deletedAssets.add(AssetID);
                }                
            }
            
            off+=CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize+size;            
        }        
        
        if(off<fileSize)
        {
            log.error("Balance DB: Corrupted file, on " + off);                
            fileSize=off;
            return;
        }        
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Balance DB: Cannot close file " + ex.getClass().getName() + " " + ex.getMessage());                
        }        
        
        log.info("Balance DB: File opened: " + fileName);                
    }
    
    private boolean saveTxOut(CSTransactionOutput TxOut,CSBalanceDatabase.CSTxOutEntry TxOutEntry,int Offset,int CodedSize)
    {
        RandomAccessFile aFile;
        
        try {
            aFile = new RandomAccessFile(fileName, "rw");
        } catch (FileNotFoundException ex) {
            log.info("Balance DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        try {
            aFile.seek(Offset);
        } catch (IOException ex) {
            log.error("Balance DB: Cannot set file position " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }

        byte[] s;
        if(TxOutEntry != null)
        {
            s=new byte [CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize+TxOutEntry.mapSize*CSBalanceDatabase.CSTxOutEntry.serializedRowSize];
            CSUtils.copyArray(s, CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize,TxOutEntry.serialize());
        }
        else
        {
            s=new byte [CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize];
        }
        
        CSUtils.copyArray(s, 0, TxOut.getTxID().getBytes());
        CSUtils.littleEndianByteArray(TxOut.getIndex(), s, 32);            
        CSUtils.littleEndianByteArray(CodedSize, s, 36);
        
        try {
            aFile.write(s);
        } catch (IOException ex) {
            log.error("Balance DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }

        if(Offset>=fileSize)
        {
            fileSize=Offset+s.length;
        }
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Balance DB: Cannot close file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }        
        
        return true;
    }
    
    public boolean insertTxOut(CSTransactionOutput TxOut, int[] AssetIDs,Map <Integer,BigInteger> outputBalances)
    {
        if(TxOut == null)
        {
            return false;
        }
        
        if(map.get(TxOut.getStrValue()) != null)
        {
            log.info("Balance DB: TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex() + " already in the database");                            
            return true;
        }

        int [] ids;
        ids=AssetIDs;
        if(ids == null)
        {
            csLog.info("Balance DB: Inserting new all-assets TxOut: " + TxOut.toString());
            if(assetDB != null)
            {
                ids=assetDB.getAssetIDs();
            }
            else
            {
                return false;
            }
        }
        else
        {
            csLog.info("Balance DB: Inserting new TxOut: " + TxOut.toString());
        }

        if(ids == null)
        {
            log.info("Balance DB: TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex() + " has no relevant assets");                            
            return true;                        
        }
        
        boolean result=true;
        
        lock.lock();
        try {            
            CSBalanceDatabase.CSTxOutEntry TxOutEntry=new CSBalanceDatabase.CSTxOutEntry(ids,outputBalances,fileSize);
            
            map.put(TxOut.getStrValue(), TxOutEntry);
            
            if(!saveTxOut(TxOut, TxOutEntry, fileSize,CSUtils.codedSize(TxOutEntry.mapSize, BALANCE_DB_CODE_TXOUT)))
            {
                map.remove(TxOut.getStrValue());
                result=false;
            }
            else
            {
                log.info("Balance DB: TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex() + " inserted");                
            }
        } finally {
            lock.unlock();
        }
        
        return result;
    }
    
    public boolean deleteTxOut(CSTransactionOutput TxOut)
    {
        if(TxOut == null)
        {
            return false;
        }
        
        CSBalanceDatabase.CSTxOutEntry TxOutEntry=map.get(TxOut.getStrValue());                
                
        if(TxOutEntry == null)
        {
            log.info("Balance DB: TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex() + " not found in the database");                            
            return true;                        
        }
        
        boolean result=true;
        
        lock.lock();
        try {            
            if(!saveTxOut(TxOut, null, TxOutEntry.offsetInDB,CSUtils.codedSize(TxOutEntry.mapSize, BALANCE_DB_CODE_DELETED)))
            {
                result=false;
            }
            else
            {
                deadSize+=CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize+TxOutEntry.mapSize*CSBalanceDatabase.CSTxOutEntry.serializedRowSize;
                map.remove(TxOut.getStrValue());

                log.info("Balance DB: TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex() + " deleted");                            
            }
        } finally {
            lock.unlock();
        }
        return result;
    }
    
    
    
    
    public boolean insertAsset(int AssetID)
    {
        if(newAssets.indexOf(AssetID)>=0)
        {
            log.info("Balance DB: Asset " + AssetID + " already in the database");                            
            return true;            
        }

        csLog.info("Balance DB: Inserting new asset " + AssetID);
        
        boolean result=true;
        
        lock.lock();
        try {            
            int pos=deletedAssets.indexOf(AssetID);
            if(pos>=0)
            {
                deletedAssets.remove(pos);
            }

            newAssets.add(AssetID);

            CSTransactionOutput TxOut=new CSTransactionOutput(Sha256Hash.ZERO_HASH, AssetID);
            if(!saveTxOut(TxOut, null, fileSize,CSUtils.codedSize(0, BALANCE_DB_CODE_NEWASSET)))
            {
                pos=newAssets.indexOf(AssetID);
                if(pos>=0)
                {
                    newAssets.remove(pos);
                }
                result=false;
            }
            else
            {
                csLog.info("Balance DB: Asset " + AssetID + " inserted");                            
            }
        } finally {
            lock.unlock();
        }
        return result;
    }

    public boolean deleteAsset(int AssetID)
    {              
        if(deletedAssets.indexOf(AssetID)>=0)
        {
            log.info("Balance DB: Asset " + AssetID + " already deleted from  the database");                            
            return true;            
        }

        boolean result=true;
        
        lock.lock();
        try {            
            int pos=newAssets.indexOf(AssetID);
            if(pos>=0)
            {
                newAssets.remove(pos);
            }

            deletedAssets.add(AssetID);

            CSTransactionOutput TxOut=new CSTransactionOutput(Sha256Hash.ZERO_HASH, AssetID);
            if(!saveTxOut(TxOut, null, fileSize,CSUtils.codedSize(0, BALANCE_DB_CODE_DELETEDASSET)))
            {
                pos=deletedAssets.indexOf(AssetID);
                if(pos>=0)
                {
                    deletedAssets.remove(pos);
                }
                result=false;
            }
            else
            {
                csLog.info("Balance DB: Asset " + AssetID + " deleted");                            
            }
        } finally {
            lock.unlock();
        }
        return result;
    }
    
    public class CSBalanceIterator
    {
        private List<CSBalance> list= new ArrayList<CSBalance>();        
        private int pointer;
        
        public CSBalanceIterator()
        {
            pointer=-1;
        }
        
        public boolean add(CSBalance Balance)
        {
            return list.add(Balance);
        }
        
        public CSBalance rewind()
        {            
            pointer=-1;
            return next();
        }
        
        public CSBalance next()
        {
            pointer++;
            if(pointer>=list.size())
            {
                return null;
            }
            return list.get(pointer);
        }
    }
    
    public CSBalanceDatabase.CSBalanceIterator getTxOutBalances(CSTransactionOutput TxOut)
    {
        CSBalanceDatabase.CSBalanceIterator iter=new CSBalanceDatabase.CSBalanceIterator();
        
        CSBalanceDatabase.CSTxOutEntry TxOutEntry=map.get(TxOut.getStrValue());                
                
        if(TxOutEntry == null)
        {
            log.info("Balance DB: TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex() + " not found in the database");                            
            return iter;
        }
        
        lock.lock();
        try {            
            for (Map.Entry<Integer, CSBalanceDatabase.CSBalanceEntry> entry : TxOutEntry.map.entrySet()) 
            {
                CSBalanceDatabase.CSBalanceEntry be=entry.getValue();
                int AssetID=entry.getKey();
                if(deletedAssets.indexOf(AssetID) < 0)
                {
                    CSBalance balance=new CSBalance(TxOut,AssetID,be.qty,be.qtyChecked,be.qtyFailures,be.balanceState);
                    iter.add(balance);
                }
            }            

            for (Integer newAsset : newAssets) {
                int asset = newAsset;
                if(!TxOutEntry.map.containsKey(asset))
                {
                    CSBalance balance=new CSBalance(TxOut, asset);
                    iter.add(balance);                
                }
            }
        
        } finally {
            lock.unlock();
        }
        return iter;
    }
    
    public CSBalance getBalance(CSTransactionOutput TxOut,int AssetID)
    {
        if(TxOut == null)
        {
            return null;
        }
        
        CSBalanceDatabase.CSTxOutEntry TxOutEntry=map.get(TxOut.getStrValue());                
                
        if(TxOutEntry == null)
        {
            log.info("Balance DB: TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex() + " not found in the database");                            
            return null;
        }
        
        CSBalanceDatabase.CSBalanceEntry be;
        
        lock.lock();
        try {            
            be=TxOutEntry.map.get(AssetID);
        } finally {
            lock.unlock();
        }

        if(be != null)
        {
            return new CSBalance(TxOut,AssetID,be.qty,be.qtyChecked,be.qtyFailures,be.balanceState);
        }
        
        return null;        
    }
    
    public CSBalanceDatabase.CSBalanceIterator getAssetBalances(int AssetID)
    {
        CSBalanceDatabase.CSBalanceIterator iter=new CSBalanceDatabase.CSBalanceIterator();
        
        if(deletedAssets.indexOf(AssetID) >= 0)
        {
            if(newAssets.indexOf(AssetID) < 0)
            {
                return iter;
            }            
        }
        
        lock.lock();
        try {            
            for (Map.Entry<String, CSBalanceDatabase.CSTxOutEntry> entryTxOut : map.entrySet()) 
            {
                CSTransactionOutput TxOut=new CSTransactionOutput(entryTxOut.getKey());
                CSBalanceDatabase.CSTxOutEntry TxOutEntry=entryTxOut.getValue();                

                for (Map.Entry<Integer, CSBalanceDatabase.CSBalanceEntry> entry : TxOutEntry.map.entrySet()) 
                {
                    if(entry.getKey() == AssetID)
                    {
                        CSBalanceDatabase.CSBalanceEntry be=entry.getValue();
                        CSBalance balance=new CSBalance(TxOut,AssetID,be.qty,be.qtyChecked,be.qtyFailures,be.balanceState);
                        iter.add(balance);
                    }                
                }
            }    

            if(newAssets.indexOf(AssetID) >= 0)
            {
                for (Map.Entry<String, CSBalanceDatabase.CSTxOutEntry> entryTxOut : map.entrySet()) 
                {
                    CSTransactionOutput TxOut=new CSTransactionOutput(entryTxOut.getKey());
                    CSBalanceDatabase.CSTxOutEntry TxOutEntry=entryTxOut.getValue();                

                    if(!TxOutEntry.map.containsKey(AssetID))
                    {
                        CSBalance balance=new CSBalance(TxOut, AssetID);
                        iter.add(balance);                                   
                    }
                }                
            }
        } finally {
            lock.unlock();
        }
        
        return iter;
    }

    private boolean defragment()
    {        
        File bdbFile = new File(fileName + ".new");
                        
        if(bdbFile.exists())
        {
            if(!bdbFile.delete())
            {
                log.error("Balance DB: Cannot deleted temporary file");                
                return false;
            }            
        }
     
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(fileName + ".new", "rw");
        } catch (FileNotFoundException ex) {
            log.info("Balance DB: Cannot open temporary file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        int off=0;
        
        for (Map.Entry<String, CSBalanceDatabase.CSTxOutEntry> entryTxOut : map.entrySet()) 
        {
            CSTransactionOutput TxOut=new CSTransactionOutput(entryTxOut.getKey());
            CSBalanceDatabase.CSTxOutEntry TxOutEntry=entryTxOut.getValue();                
            CSBalanceDatabase.CSTxOutEntry newTxOutEntry=new CSBalanceDatabase.CSTxOutEntry(TxOutEntry.mapSize+newAssets.size(), off);
            
            for (Map.Entry<Integer, CSBalanceDatabase.CSBalanceEntry> entry : TxOutEntry.map.entrySet()) 
            {
                int asset=entry.getKey();
                CSBalanceDatabase.CSBalanceEntry be=entry.getValue();
                
                switch(be.balanceState)
                {
                    case DELETED:
                    case ZERO:
                        break;
                    default:
                        if(deletedAssets.indexOf(asset) < 0)
                        {
                            newTxOutEntry.add(asset, be);
                        }
                        break;                    
                }                
            }
            for (Integer newAsset : newAssets) {
                int asset = newAsset;
                if(!TxOutEntry.map.containsKey(asset))
                {
                    newTxOutEntry.add(asset, new CSBalanceDatabase.CSBalanceEntry());
                }
            }
            
            if(newTxOutEntry.mapSize > 0)
            {
                byte[] s;
                s=new byte [CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize+newTxOutEntry.mapSize*CSBalanceDatabase.CSTxOutEntry.serializedRowSize];
                CSUtils.copyArray(s, CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize,newTxOutEntry.serialize());
                CSUtils.copyArray(s, 0, TxOut.getTxID().getBytes());
                CSUtils.littleEndianByteArray(TxOut.getIndex(), s, 32);            
                CSUtils.littleEndianByteArray(CSUtils.codedSize(newTxOutEntry.mapSize, BALANCE_DB_CODE_TXOUT), s, 36);                
                
                try {
                    aFile.write(s);
                } catch (IOException ex) {
                    log.error("Balance DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
                    return false;
                }
                off+=s.length;
            }
        }    

        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Balance DB: Cannot close db file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }        
        
        bdbFile = new File(fileName + ".new");
                        
        if(bdbFile.exists())
        {
            try {
                Files.move( bdbFile.toPath(), new File(fileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                log.error("Balance DB: Cannot rename temporary file: " + ex.getMessage());                
                return false;
            }
/*            
            if(!bdbFile.renameTo(new File(fileName)))
            {
                log.error("Balance DB: Cannot rename temporary file");                
                return false;
            }            
            */
        }
        
        load();
        
        log.info("Balance DB: Defragmentation completed");                            
        return true;
        
    }
    
    private class CSBalanceTxTransfers
    {
        public CSBalanceUpdate [] inputs;
        public CSBalanceUpdate [] outputs;
        public boolean valid;
        
        public CSBalanceTxTransfers(int AssetID,Transaction Tx)
        {
            valid=true;
            inputs=new CSBalanceUpdate[Tx.getInputs().size()];
            int count=0;
            for(TransactionInput input : Tx.getInputs())
            {
                TransactionOutPoint outPoint=input.getOutpoint();
                if(outPoint != null)
                {
                    CSBalanceEntry be=new CSBalanceEntry();
                    inputs[count]=new CSBalanceUpdate(new CSTransactionOutput(outPoint.getHash(), (int)outPoint.getIndex()), AssetID, new CSBalanceEntry(), -1, null);
                }
                else
                {
                    valid=false;
                }
                count++;
            }
                    
            outputs=new CSBalanceUpdate[Tx.getOutputs().size()];
            for(int i=0;i<Tx.getOutputs().size();i++)
            {
                outputs[i]=null;
            }
        }
                
    }            
    
    protected void addTrackedTransaction(Transaction Tx)
    {
        boolean found=false;
        for(CSBalanceTransaction balanceTransaction : trackedTransactions)
        {
            if(balanceTransaction.tx.getHash().equals(Tx.getHash()))
            {
                found=true;
            }
        }
        if(!found)
        {
            trackedTransactions.add(new CSBalanceTransaction(Tx));
        }
    }
    
    private class CSBalanceTransaction
    {
        public Transaction tx;
        public Date entryTime;
        public boolean deleteFlag;        
        Map<Integer,CSBalanceTxTransfers> assetsToTrack=null;
                
        public CSBalanceTransaction() {}
        
        public CSBalanceTransaction(Transaction Tx) 
        {
            tx=Tx;
            deleteFlag=false;
            entryTime=new Date();
        }
                
                
        public void addInputBalanceUpdates(List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates)
        {
            assetsToTrack= new HashMap<Integer,CSBalanceTxTransfers>();  
            
            for(int i=0;i<balanceUpdates.size();i++)
            {
                CSBalanceDatabase.CSBalanceUpdate bu=balanceUpdates.get(i);
                switch(bu.balance.balanceState)
                {
                    case NEVERCHECKED:
                    case UNKNOWN:
                        if(bu.txOut.getTxID().equals(tx.getHash()))
                        {
                            CSAsset asset=assetDB.getAsset(bu.assetID);
                            if((asset != null) && (asset.getAssetState() == CSAsset.CSAssetState.VALID) && (asset.getAssetReference() != null) && (asset.getAssetReference().isValid()))
                            {
                                if(!assetsToTrack.containsKey(bu.assetID))
                                {
                                    assetsToTrack.put(bu.assetID, new CSBalanceTxTransfers(bu.assetID,tx));
                                }
                                assetsToTrack.get(bu.assetID).outputs[bu.txOut.getIndex()]=bu;
                            }
                        }                
                        break;
                }
            }    
                        
            deleteFlag=true;
            
            for (Map.Entry<Integer, CSBalanceTxTransfers> entryTransfers : assetsToTrack.entrySet()) 
            {
                CSBalanceTxTransfers transfers=entryTransfers.getValue();
                if(transfers.valid)
                {
                    deleteFlag=false;
                    for(int i=0;i<transfers.inputs.length;i++)
                    {
                        balanceUpdates.add(transfers.inputs[i]);
                    }
                }
            }            
        }
        
        public void applyInputBalances(List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates)
        {
            for (Map.Entry<Integer, CSBalanceTxTransfers> entryTransfers : assetsToTrack.entrySet()) 
            {
                CSAsset asset=assetDB.getAsset(entryTransfers.getKey());
                CSBalanceTxTransfers balanceTransfers=entryTransfers.getValue();
                if((asset == null) || (asset.getAssetState() != CSAsset.CSAssetState.VALID) || (asset.getAssetReference() == null) || (!asset.getAssetReference().isValid()))
                {
                   balanceTransfers.valid=false; 
                }
                if(balanceTransfers.valid)
                {                    
                    int countInputs=balanceTransfers.inputs.length;
                    int countOutputs=balanceTransfers.outputs.length;
                    long [] inputBalances=new long[countInputs];
                    long [] outputBalances=new long[countOutputs];
                    boolean [] outputsRegular=new boolean[countOutputs];
                    
                    long totalInput=0;
                    long totalOutput=0;
                    long [] outputsSatoshis=new long [countOutputs];
                    
                    for(int input_id=0;input_id<countInputs;input_id++)
                    {
                        CSBalanceUpdate bu=balanceTransfers.inputs[input_id];
                        switch(bu.balance.balanceState)
                        {
                            case VALID:
                            case ZERO:
                            case SPENT:
                                inputBalances[input_id]=bu.balance.qty.longValue();
                                if(bu.qtyBTC != null)
                                {
                                    totalInput+=bu.qtyBTC.longValue();
                                }
                                else
                                {
                                    balanceTransfers.valid=false;
                                }
                                break;
                            default:
                                balanceTransfers.valid=false;
                        }
                    }

                    int output_id=0;
                    for (TransactionOutput output : tx.getOutputs())
                    {
                        outputBalances[output_id] = 0;
                        outputsRegular[output_id] = CoinSparkBase.scriptIsRegular(output.getScriptBytes());
                        long value=output.getValue().longValue();
                        outputsSatoshis[output_id]=value;
                        totalOutput+=value;
                        output_id++;
                    }
                    
                    long feeSatoshis=totalInput-totalOutput;
                    
                    CSTransactionAssets txAssets=new CSTransactionAssets(tx);
                    
                    if(balanceTransfers.valid)
                    {
                        if(txAssets.getGenesis() != null)
                        {
                            long validFeeSatoshis=txAssets.getGenesis().calcMinFee(outputsSatoshis, outputsRegular);
                            if(feeSatoshis >= validFeeSatoshis)
                            {
                                outputBalances=txAssets.getGenesis().apply(outputsRegular);
                            }
                        }
                        
                        if(txAssets.getTransfers() != null)
                        {
                            long validFeeSatoshis=txAssets.getTransfers().calcMinFee(countInputs, outputsSatoshis, outputsRegular);
                            if(feeSatoshis >= validFeeSatoshis)
                            {
                                outputBalances=txAssets.getTransfers().apply(asset.getAssetReference(), asset.getGenesis(), inputBalances,outputsRegular);
                            }
                            else
                            {
                                outputBalances=txAssets.getTransfers().applyNone(asset.getAssetReference(), asset.getGenesis(), inputBalances, outputsRegular);
                            }
                        }                        
                        
                        for(output_id=0;output_id<countOutputs;output_id++)
                        {
                            CSBalanceUpdate bu=balanceTransfers.outputs[output_id];
                            if(bu != null)
                            {
                                switch(bu.balance.balanceState)
                                {
                                    case UNKNOWN:
                                        if(outputBalances[output_id]>0)
                                        {
                                            bu.balance.setQty(BigInteger.valueOf(outputBalances[output_id]), CSBalance.CSBalanceState.CALCULATED);
                                            CSEventBus.INSTANCE.postAsyncEvent(CSEventType.BALANCE_VALID,     
                                                    new CSBalance(bu.txOut,
                                                    bu.assetID,
                                                    bu.balance.qty,
                                                    bu.balance.qtyChecked,
                                                    bu.balance.qtyFailures,
                                                    bu.balance.balanceState));
                                        }
                                        else
                                        {
                                            bu.balance.setQty(BigInteger.ZERO, CSBalance.CSBalanceState.CALCULATED);                                    
                                        }
                                        csLog.info("Balance DB: Update: " + bu.txOut.getTxID().toString() + "-"
                                                                        + bu.txOut.getIndex() + "-"
                                                                        + bu.assetID + "-"
                                                                        + bu.balance.qty + "-"
                                                                        + bu.balance.qtyChecked + "-"
                                                                        + bu.balance.qtyFailures + "-"
                                                                        + bu.balance.balanceState);                            
                                        break;
                                }
                            }
                        }
                        
                    }                    
                    
                }                
            }            

            deleteFlag=true;
            if(new Date().getTime() - entryTime.getTime() < 60000)
            {
                for(int i=0;i<balanceUpdates.size();i++)
                {
                    CSBalanceDatabase.CSBalanceUpdate bu=balanceUpdates.get(i);
                    switch(bu.balance.balanceState)
                    {
                        case NEVERCHECKED:
                        case UNKNOWN:
                            if(bu.txOut.getTxID().equals(tx.getHash()))
                            {
                                deleteFlag=false;
                            }                
                            break;
                    }
                }    
                
            }
        }
        
    }
    
    private class CSBalanceUpdate
    {
        public CSTransactionOutput txOut;
        public int assetID;
        public CSBalanceDatabase.CSBalanceEntry balance;
        public CSBalanceDatabase.CSBalanceEntry oldBalance=null;
        public BigInteger qtyBTC;
        public int offset;
        public int assetIDInRequest=-1;
        public int serverIDInRequest=-1;

        public CSBalanceUpdate() {}
        
        public CSBalanceUpdate(CSTransactionOutput TxOut,int AssetID,CSBalanceDatabase.CSBalanceEntry Balance,int Offset,BigInteger QtyBTC) 
        {
            txOut=TxOut;
            assetID=AssetID;
            balance=Balance;
            offset=Offset;
            oldBalance=null;    
            qtyBTC=QtyBTC;
        }
        
        public void setBalance(CSBalanceDatabase.CSBalanceEntry Balance)
        {
            oldBalance=balance;
            balance=Balance;
        }
    }
        
    private boolean applyBalanceUpdates(List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates)
    {
        if(balanceUpdates.isEmpty())
        {
            return true;
        }
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(fileName, "rw");
        } catch (FileNotFoundException ex) {
            log.info("Balance DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            balanceUpdates.clear();
            return false;
        }
        
        boolean result=true;
        int rollback=-1;
        for(int i=0;i<balanceUpdates.size();i++)
        {
            CSBalanceDatabase.CSBalanceUpdate bu=balanceUpdates.get(i);
            if(bu.offset>=0)
            {
                if((bu.oldBalance != null) || ((bu.balance != null) && (bu.balance.balanceState == CSBalance.CSBalanceState.REFRESH)))
                {
                    map.get(bu.txOut.getStrValue()).map.replace(bu.assetID,bu.balance);
                    try {
                        aFile.seek(bu.balance.offsetInDB+4);
                    } catch (IOException ex) {
                        log.error("Balance DB: Cannot set file position " + ex.getClass().getName() + " " + ex.getMessage());                
                        rollback=i;
                        break;
                    }
                    try {
                        aFile.write(bu.balance.serialize());
                    } catch (IOException ex) {
                        log.error("Balance DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
                        rollback=i;
                        break;
                    }                
                }
            }
        }

        if(rollback>=0)
        {
            result=false;
            for(int i=0;i<=rollback;i++)
            {
                CSBalanceDatabase.CSBalanceUpdate bu=balanceUpdates.get(i);
                if((bu.oldBalance != null) || ((bu.balance != null) && (bu.balance.balanceState == CSBalance.CSBalanceState.REFRESH)))
                {
                    if(bu.oldBalance != null)
                    {
                        map.get(bu.txOut.getStrValue()).map.replace(bu.assetID,bu.balance);
                        try {
                            aFile.seek(bu.offset);
                        } catch (IOException ex) {
                            log.error("Balance DB: Cannot set file position " + ex.getClass().getName() + " " + ex.getMessage());                
                            break;
                        }
                        try {
                            aFile.write(bu.oldBalance.serialize());
                        } catch (IOException ex) {
                            log.error("Balance DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
                            break;
                        }                
                    }
                }
            }            
        }

        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Balance DB: Cannot close file " + ex.getClass().getName() + " " + ex.getMessage());                
            result=false;
        }        
        
        log.info("Balance DB: Changes commited ");                            
        balanceUpdates.clear();
        return result;
    }
    
    public boolean refreshTxOut(CSTransactionOutput TxOut)
    {
        if(TxOut == null)
        {
            return false;            
        }
        boolean result=true;
        
        CSBalanceDatabase.CSTxOutEntry TxOutEntry=map.get(TxOut.getStrValue());                        
        
        if(TxOutEntry == null)
        {
            log.info("Balance DB: TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex() + " not found in the database");                            
            return true;
        }
        

        lock.lock();
        try {            
            List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates= new ArrayList<CSBalanceDatabase.CSBalanceUpdate>();
            BigInteger qtyBTC=null;
            if(TxOutEntry.map.containsKey(0))
            {
                if(TxOutEntry.map.get(0).balanceState == CSBalance.CSBalanceState.VALID)
                {
                    qtyBTC=TxOutEntry.map.get(0).qty;
                }
            }
            balanceUpdates.clear();
            int off=TxOutEntry.offsetInDB+CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize;
            for (Map.Entry<Integer, CSBalanceDatabase.CSBalanceEntry> entry : TxOutEntry.map.entrySet()) 
            {
                CSBalanceDatabase.CSBalanceEntry be=entry.getValue();
                int AssetID=entry.getKey();
                if(AssetID>0)
                {
                    if(deletedAssets.indexOf(AssetID) < 0)
                    {
                        be.setQty(be.qty, CSBalance.CSBalanceState.REFRESH);
                        balanceUpdates.add(new CSBalanceDatabase.CSBalanceUpdate(TxOut, AssetID, be, off,qtyBTC));
                    }
                }
                off+=CSBalanceDatabase.CSBalanceEntry.serializedSize;
            }            

            log.info("Balance DB: Refreshing TxOut " + TxOut.getTxID().toString() + "-" + TxOut.getIndex());                            
            result=applyBalanceUpdates(balanceUpdates);
        } finally {
            lock.unlock();
        }
        
        return result;
    }
    
    public boolean refreshAsset(int AssetID)
    {
        boolean result=true;
        
        if(AssetID<=0)
        {
            return false;
        }
        if(deletedAssets.indexOf(AssetID) >= 0)
        {
            if(newAssets.indexOf(AssetID) < 0)
            {
                return true;
            }            
        }
        
        lock.lock();
        try {            
            List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates= new ArrayList<CSBalanceDatabase.CSBalanceUpdate>();
            balanceUpdates.clear();
            for (Map.Entry<String, CSBalanceDatabase.CSTxOutEntry> entryTxOut : map.entrySet()) 
            {
                CSTransactionOutput TxOut=new CSTransactionOutput(entryTxOut.getKey());
                CSBalanceDatabase.CSTxOutEntry TxOutEntry=entryTxOut.getValue();                
                BigInteger qtyBTC=null;
                if(TxOutEntry.map.containsKey(0))
                {
                    if(TxOutEntry.map.get(0).balanceState == CSBalance.CSBalanceState.VALID)
                    {
                        qtyBTC=TxOutEntry.map.get(0).qty;
                    }
                }
                int off=TxOutEntry.offsetInDB+CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize;

                for (Map.Entry<Integer, CSBalanceDatabase.CSBalanceEntry> entry : TxOutEntry.map.entrySet()) 
                {
                    if(entry.getKey() == AssetID)
                    {
                        CSBalanceDatabase.CSBalanceEntry be=entry.getValue();
                        be.setQty(be.qty, CSBalance.CSBalanceState.REFRESH);
                        balanceUpdates.add(new CSBalanceDatabase.CSBalanceUpdate(TxOut, AssetID, be, off,qtyBTC));
                    }
                    off+=CSBalanceDatabase.CSBalanceEntry.serializedSize;
                }
            }    
        
            log.info("Balance DB: Refreshing Asset " + AssetID);                            
            result=applyBalanceUpdates(balanceUpdates);
            if(result)
            {
                insertAsset(AssetID);
                defragment();
            }
        } finally {
            lock.unlock();
        }
        
        return result;
    }
    
    public boolean refreshAll()
    {        
        
        boolean result=true;
        
        lock.lock();
        try {            
            List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates= new ArrayList<CSBalanceDatabase.CSBalanceUpdate>();
            balanceUpdates.clear();
            for (Map.Entry<String, CSBalanceDatabase.CSTxOutEntry> entryTxOut : map.entrySet()) 
            {
                CSTransactionOutput TxOut=new CSTransactionOutput(entryTxOut.getKey());
                CSBalanceDatabase.CSTxOutEntry TxOutEntry=entryTxOut.getValue();                
                BigInteger qtyBTC=null;
                if(TxOutEntry.map.containsKey(0))
                {
                    if(TxOutEntry.map.get(0).balanceState == CSBalance.CSBalanceState.VALID)
                    {
                        qtyBTC=TxOutEntry.map.get(0).qty;
                    }
                }
                int off=TxOutEntry.offsetInDB+CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize;

                for (Map.Entry<Integer, CSBalanceDatabase.CSBalanceEntry> entry : TxOutEntry.map.entrySet()) 
                {
                    int asset=entry.getKey();
                    if(asset>0)
                    {
                        if(deletedAssets.indexOf(asset) < 0)
                        {
                            CSBalanceDatabase.CSBalanceEntry be=entry.getValue();
                            be.setQty(be.qty, CSBalance.CSBalanceState.REFRESH);
                            balanceUpdates.add(new CSBalanceDatabase.CSBalanceUpdate(TxOut, asset, be, off,qtyBTC));
                        }
                    }
                    off+=CSBalanceDatabase.CSBalanceEntry.serializedSize;
                }
            }    
            
            log.info("Balance DB: Refreshing All");                            
            result=applyBalanceUpdates(balanceUpdates);
        } finally {
            lock.unlock();
        }
        
        return result;
    }
    
    private List<CSBalanceDatabase.CSBalanceUpdate> createBalanceUpdateList()
    {
        boolean takeIt;
        Date timeNow=new Date();
        long interval;
        
        List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates= new ArrayList<CSBalanceDatabase.CSBalanceUpdate>();
        balanceUpdates.clear();
        for (Map.Entry<String, CSBalanceDatabase.CSTxOutEntry> entryTxOut : map.entrySet()) 
        {
            CSTransactionOutput TxOut=new CSTransactionOutput(entryTxOut.getKey());
            CSBalanceDatabase.CSTxOutEntry TxOutEntry=entryTxOut.getValue();                
            BigInteger qtyBTC=null;
            if(TxOutEntry.map.containsKey(0))
            {
                if(TxOutEntry.map.get(0).balanceState == CSBalance.CSBalanceState.VALID)
                {
                    qtyBTC=TxOutEntry.map.get(0).qty;
                }
            }
            int off=TxOutEntry.offsetInDB+CSBalanceDatabase.CSTxOutEntry.serializedHeaderSize;
            
            
            for (Map.Entry<Integer, CSBalanceDatabase.CSBalanceEntry> entry : TxOutEntry.map.entrySet()) 
            {
                int asset=entry.getKey();
                CSBalanceDatabase.CSBalanceEntry be=entry.getValue();
                
//                if(qtyBTC != null)
                if(true)
                {
                    takeIt=false;
                    switch(be.balanceState)
                    {
                        case NEVERCHECKED:
                        case REFRESH:
                            takeIt=true;
                            break;
                        case UNKNOWN:
                        case SELF:
                        case CALCULATED:
                            interval=(timeNow.getTime()-be.qtyChecked.getTime())/1000;
                            if(be.qtyFailures<40)
                            {
                                takeIt=true;
                            }
                            else
                            {
                                if(be.qtyFailures<80)
                                {
                                    if(interval>15){
                                        takeIt=true;
                                    }
                                }                      
                                else
                                {
                                    if(be.qtyFailures<120)
                                    {
                                        if(interval>3600){
                                            takeIt=true;
                                        }
                                    }                      
                                    else
                                    {
                                        if(interval>86400){
                                            takeIt=true;
                                        }
                                    }                      
                                }                      

                            }
                            break;                        
                    }
                }
                else
                {
                    takeIt=true;
                }
                if(takeIt)
                {
                    balanceUpdates.add(new CSBalanceDatabase.CSBalanceUpdate(TxOut, asset, be, off,qtyBTC));
                }
                off+=CSBalanceDatabase.CSTxOutEntry.serializedRowSize;
            }
        }    
        
        return balanceUpdates;
    }
    
    private class JRequestTxOut
    {
        public String txid;
        public int vout;
        
        JRequestTxOut(String TxID,int Index)
        {
            txid=TxID;
            vout=Index;
        }
    }

    private class JRequestParams
    {
        public CSBalanceDatabase.JRequestTxOut[] txouts;
        public String[] assets;
    }

    private class JRequest
    {
        public int id;
        public String jsonrpc = "2.0";
        public String method;
        public CSBalanceDatabase.JRequestParams params;
    }
        
    private void processBalanceUpdateList(List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates,Map<String,Integer> TxDepthMap)
    {
        if(assetDB == null)
        {
            return; 
        }        
        if(balanceUpdates.isEmpty())
        {
            return; 
        }        
        
        List<Integer> assetIDs= new ArrayList<Integer>();        
        List<CSAsset> assets= new ArrayList<CSAsset>();        
        List<CSTransactionOutput> txOuts= new ArrayList<CSTransactionOutput>();        
        List<String> servers= new ArrayList<String>();        
        
        for (CSBalanceUpdate bu : balanceUpdates) 
        {
            if(assetIDs.indexOf(bu.assetID)<0)
            {
                assetIDs.add(bu.assetID);
            }
        }
        
        for (Integer assetID1 : assetIDs) 
        {
            int assetID = assetID1;
            CSAsset asset=assetDB.getAsset(assetID);
            boolean takeit=false;
            if(asset!=null)
            {
                asset.selectCoinsparkTrackerUrl();
                if(asset.getCoinsparkTrackerUrl() != null)
                {
                    if(servers.indexOf(asset.getCoinsparkTrackerUrl())<0)
                    {
                        servers.add(asset.getCoinsparkTrackerUrl());
                    }
                    takeit=true;
                }
            }
            if(takeit)
            {
                assets.add(asset);
            }            
        }

        if(assets.isEmpty())
        {
            return;
        }
        
        assetIDs.clear();
        for (CSAsset asset : assets) 
        {
            assetIDs.add(asset.getAssetID());
        }   

        int [] idMap=new int[assets.size()];
        
        for(int s=0;s<servers.size();s++)
        {
            CSBalanceDatabase.JRequest request=new CSBalanceDatabase.JRequest();

            request.id = (int)(new Date().getTime()/1000);
            request.jsonrpc = "2.0";
            request.method = "coinspark_assets_get_qty";
            request.params = new CSBalanceDatabase.JRequestParams();
            request.params.assets=new String[assets.size()];
        

            int c=0;
            for(int i=0;i<assets.size();i++)
            {
                idMap[i]=-1;
                if(servers.get(s).equals(assets.get(i).getCoinsparkTrackerUrl()))
                {
                    CSAsset asset=assets.get(i);            
                    request.params.assets[c]=asset.getGenTxID();
                    idMap[i]=c;    
                    c++;
                }
            }   
        
            CSTransactionOutput lastTxOut=null;        

            txOuts.clear();
            for(int i=0;i<balanceUpdates.size();i++)
            {
                CSBalanceDatabase.CSBalanceUpdate bu=balanceUpdates.get(i);
                bu.assetIDInRequest=assetIDs.indexOf(bu.assetID);
                if(bu.assetIDInRequest >= 0)
                {
                    if(idMap[bu.assetIDInRequest] >= 0)
                    {
                        bu.oldBalance=new CSBalanceEntry(bu.balance);
                        bu.serverIDInRequest=s;
                        balanceUpdates.set(i, bu);
                        if(!bu.txOut.equals(lastTxOut))
                        {
                            txOuts.add(bu.txOut);
                            lastTxOut=bu.txOut;
                        }
                    }
                }
            }
        
            if(!txOuts.isEmpty())
            {        
                request.params.txouts=new CSBalanceDatabase.JRequestTxOut[txOuts.size()];

                for(int i=0;i<txOuts.size();i++)
                {
                    CSTransactionOutput txout=txOuts.get(i);
                    request.params.txouts[i]=new CSBalanceDatabase.JRequestTxOut(txout.getTxID().toString(), txout.getIndex());
                }

                JsonObject jresult=null;

                try
                {
                    // Use Google GSON library for Java Object <--> JSON conversions
                    Gson gson = new Gson();

                    // convert java object to JSON format,
                    String json = gson.toJson(request);
                    try
                    {
                        JsonElement jelement;
                        JsonObject jobject = null;                        
                        
                        Map <String,String> headers=new HashMap<String, String>();
                        headers.put("Content-Type", "application/json");
                        
                        CSUtils.CSDownloadedURL downloaded=CSUtils.postURL(servers.get(s), 15, null, json, headers);
                        if(downloaded.error != null)
                        {
                            log.error(downloaded.error);
                        }
                        else
                        {
                            jelement = new JsonParser().parse(downloaded.contents);
                            jobject = jelement.getAsJsonObject();                                                    
                        }
                            
                        if(jobject != null)
                        {
                            if((jobject.get("id") == null) || jobject.get("id").getAsInt() != request.id)
                            {
                                log.error("Tracker: id doesn't match " + request.id);                
                            }
                            else
                            {
                                if((jobject.get("error") != null))
                                {
                                    if(jobject.get("error").isJsonObject())
                                    {
                                        JsonObject jerror=jobject.get("error").getAsJsonObject();
                                        String errorMessage="Query error: ";
                                        if(jerror.get("code") != null)
                                        {
                                            errorMessage+=jerror.get("code").getAsInt() + " - ";
                                        }
                                        if(jerror.get("message") != null)
                                        {
                                            errorMessage+=jerror.get("message").getAsString();
                                        }
                                        log.error("Tracker: " + errorMessage);                                    
                                    }
                                    else
                                    {
                                        log.error("Tracker: Query error");                                    
                                    }
                                }
                                else
                                {
                                    jresult = jobject.getAsJsonObject("result");                        
                                    if(jresult != null)
                                    {
                                        if(!jresult.isJsonObject())
                                        {
                                            log.error("Tracker: result object is not array");      
                                            jresult=null;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch(JsonSyntaxException ex)
                    {
                        log.error("Tracker, JSON syntax " + ex.getClass().getName() + " " + ex.getMessage());                                
                    }
                    catch(Exception ex)
                    {
                        log.error("Tracker, exception " + ex.getClass().getName() + " " + ex.getMessage());                                
                    }
                }

                catch (Exception ex)
                {
                    log.error("Tracker: " + ex.getClass().getName() + " " + ex.getMessage());                
                }

                if(jresult == null)
                {
                    for(int i=0;i<balanceUpdates.size();i++)
                    {
                        CSBalanceDatabase.CSBalanceUpdate bu=balanceUpdates.get(i);

                        if(bu.serverIDInRequest == s)
                        {
                            bu.oldBalance=null;                            
                            bu.serverIDInRequest=-1;
                            balanceUpdates.set(i, bu);
                        }
                    }            
                }
                else
                {
                    for(int i=0;i<balanceUpdates.size();i++)
                    {
                        CSBalanceDatabase.CSBalanceUpdate bu=balanceUpdates.get(i);

                        if(bu.serverIDInRequest == s)
                        {
                            try
                            {                                
                                if(bu.qtyBTC == null)
                                {
                                    if((jresult.get("BTC") != null) && (jresult.get("BTC").isJsonArray()))
                                    {
                                        JsonArray jarray = jresult.getAsJsonArray("BTC");                            
                                        for (int j = 0; j < jarray.size(); j++)
                                        {
                                            JsonObject jentry = jarray.get(j).getAsJsonObject();
                                            if(jentry.get("txid") != null)
                                            {
                                                String txID=jentry.get("txid").toString();
                                                txID=txID.substring(1,65);
                                                if((jentry.get("vout") != null) && bu.txOut.equals(txID,jentry.get("vout").getAsInt()))
                                                {
                                                    if((jentry.get("error") == null) && (jentry.get("qty") != null))
                                                    {
                                                        bu.qtyBTC=new BigInteger(jentry.get("qty").getAsString());
                                                    }
                                                }
                                            }
                                        }
                                    }                                    
                                }
                                
                                String genTxID=assets.get(bu.assetIDInRequest).getGenTxID();

                                if((jresult.get(genTxID) != null) && (jresult.get(genTxID).isJsonArray()))
                                {
                                    JsonArray jarray = jresult.getAsJsonArray(genTxID);                            
                                    for (int j = 0; j < jarray.size(); j++)
                                    {
                                        JsonObject jentry = jarray.get(j).getAsJsonObject();
                                        if(jentry.get("txid") != null)
                                        {
                                            String txID=jentry.get("txid").toString();
                                            txID=txID.substring(1,65);
                                            if((jentry.get("vout") != null) && bu.txOut.equals(txID,jentry.get("vout").getAsInt()))
                                            {
                                                if((jentry.get("error") != null) || (jentry.get("qty") == null))
                                                {
                                                    if((bu.balance.balanceState != CSBalance.CSBalanceState.SELF) && (bu.balance.balanceState != CSBalance.CSBalanceState.CALCULATED))
                                                    {
                                                        bu.balance.setQty(bu.balance.qty, CSBalance.CSBalanceState.UNKNOWN);
                                                    }
                                                    else
                                                    {
                                                        bu.balance.setQty(bu.balance.qty, bu.balance.balanceState);                                                        
                                                    }
                                                    if(TxDepthMap != null)
                                                    {
                                                        if(TxDepthMap.containsKey(txID))
                                                        {
                                                            if(TxDepthMap.get(txID) > BALANCE_DB_MAXIMAL_UNKNOWN_DEPTH)
                                                            {
                                                                bu.balance.setQty(BigInteger.ZERO, CSBalance.CSBalanceState.ZERO);                                    
                                                                csLog.info("Balance DB: Tx is too deep in block chain (depth " + TxDepthMap.get(txID) + ": " 
                                                                                                + bu.txOut.getTxID().toString() + "-"
                                                                                                + bu.txOut.getIndex() + "-"
                                                                                                + bu.assetID + " - setting UNKNOWN state to ZERO");                                                                                            
                                                            }
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    if((jentry.get("spent") != null) && (jentry.get("spent").getAsInt()>0))
                                                    {
                                                        bu.balance.setQty(new BigInteger(jentry.get("qty").getAsString()), CSBalance.CSBalanceState.SPENT);
                                                    }
                                                    else
                                                    {
                                                        if(jentry.get("qty").getAsLong()>0)
                                                        {
                                                            bu.balance.setQty(new BigInteger(jentry.get("qty").getAsString()), CSBalance.CSBalanceState.VALID);
                                                            CSEventBus.INSTANCE.postAsyncEvent(CSEventType.BALANCE_VALID,     
                                                                    new CSBalance(bu.txOut,
                                                                    bu.assetID,
                                                                    bu.balance.qty,
                                                                    bu.balance.qtyChecked,
                                                                    bu.balance.qtyFailures,
                                                                    bu.balance.balanceState));
                                                        }
                                                        else
                                                        {
                                                            bu.balance.setQty(new BigInteger(jentry.get("qty").getAsString()), CSBalance.CSBalanceState.ZERO);                                    
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                csLog.info("Balance DB: Update: " + bu.txOut.getTxID().toString() + "-"
                                                                + bu.txOut.getIndex() + "-"
                                                                + bu.assetID + "-"
                                                                + bu.balance.qty + "-"
                                                                + bu.balance.qtyChecked + "-"
                                                                + bu.balance.qtyFailures + "-"
                                                                + bu.balance.balanceState);                            

                            }
                            catch (Exception ex){
                                log.error("Tracker: " + ex.getClass().getName() + " " + ex.getMessage());                
                                bu.oldBalance = null;
                            }
                            balanceUpdates.set(i, bu);
                        }
                    }
                }
            }
        }                    
    }

    
    private void checkDuplicates()
    {
        if(assetDB == null)
        {
            return;
        }
        
        int [] ids=assetDB.getAssetIDs();
        
        if(ids != null)
        {
            for(int assetID : ids)
            {
                CSAsset asset=assetDB.getAsset(assetID);
                if(asset.getAssetState() == CSAsset.CSAssetState.DUPLICATE)
                {
                    csLog.info("Balance DB: Duplicate asset, ID: " + assetID + ": "+ asset.status());
                    CSAsset found=assetDB.findAsset(asset, true);
                    if(found != null)
                    {
                        if(found.getAssetID() == asset.getAssetID())
                        {
                            csLog.error("Balance DB: Asset is mapped: " + asset.status());
                        }
                        else
                        {
                            csLog.info("Balance DB: Mapped to ID: " + found.getAssetID() + ": " + found.status());
                            insertAsset(found.getAssetID());
                            deleteAsset(assetID);
                            assetDB.deleteAsset(asset);
                        }
                    }
                    else
                    {
                        csLog.error("Cannot find copy of asset " + asset.status());
                    }
                }
            }
        }
    }
    
    
    private boolean calculationInProgress=false;
    
    private boolean calculateBalances(boolean Defragment,Map<String,Integer> TxDepthMap)
    {
        if(calculationInProgress){
            return false;
        }
        
        calculationInProgress=true;
        
        List<CSBalanceDatabase.CSBalanceUpdate> balanceUpdates;//= new ArrayList<CSBalanceDatabase.CSBalanceUpdate>();        

        checkDuplicates();
        
        lock.lock();
        try {            
            if(!newAssets.isEmpty() || !deletedAssets.isEmpty() || (deadSize>0.5*fileSize) || Defragment)
            {
                defragment();
            }

            balanceUpdates=createBalanceUpdateList();
            for(CSBalanceTransaction trackedTx : trackedTransactions)
            {
                trackedTx.addInputBalanceUpdates(balanceUpdates);
            }
        } finally {
            lock.unlock();
        }
        
        processBalanceUpdateList(balanceUpdates,TxDepthMap);
        
        CSBalanceTransaction [] trackedTxToRemove=new CSBalanceTransaction[trackedTransactions.size()];
        int count=0;
        for(CSBalanceTransaction trackedTx : trackedTransactions)
        {            
            trackedTx.applyInputBalances(balanceUpdates);
            if(trackedTx.deleteFlag)
            {
                trackedTxToRemove[count]=trackedTx;
                count++;
            }
        }
        
        for(int i=0;i<count;i++)
        {
            trackedTransactions.remove(trackedTxToRemove[i]);            
        }
        
        lock.lock();
        try {            
            applyBalanceUpdates(balanceUpdates);
        } finally {
            lock.unlock();
        }

        log.info("Balance DB: Synchronization completed");                            
        calculationInProgress=false;
        return false;
    }
    
    public boolean calculateBalances()
    {
        return calculateBalances(true,null);
    }    
    
    public boolean calculateBalances(Map<String,Integer> TxDepthMap)
    {
        return calculateBalances(true,TxDepthMap);
    }    
}
