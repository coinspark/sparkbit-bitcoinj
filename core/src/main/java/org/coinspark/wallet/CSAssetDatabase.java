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

import com.google.bitcoin.core.PeerGroup;
import com.google.bitcoin.utils.Threading;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.coinspark.core.CSLogger;
import org.coinspark.core.CSUtils;
import org.coinspark.protocol.CoinSparkAssetRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.Arrays;

public class CSAssetDatabase 
{
    private static final Logger log = LoggerFactory.getLogger(CSAssetDatabase.class);
    private CSLogger csLog;
    
    protected final ReentrantLock lock = Threading.lock("assetdb");
    
    private static final String ASSET_DB_SUFFIX = ".csassets";
    private static final String ASSET_DIR_SUFFIX = ".csfiles";
    
    private int maxAssetID;
    
    private HashMap<Integer, CSAsset> map=new HashMap<Integer, CSAsset>(16, 0.9f);
    private HashMap<String, Integer> mapByTxID=new HashMap<String, Integer>(16, 0.9f);
    private HashMap<String, Integer> mapByAssetRef=new HashMap<String, Integer>(16, 0.9f);
    
    private String fileName;
    private String dirName;
    private int fileSize;

    public CSAssetDatabase(String FilePrefix,CSLogger CSLog)
    {
        fileName = FilePrefix + ASSET_DB_SUFFIX;
        dirName = FilePrefix + ASSET_DIR_SUFFIX+File.separator;
        csLog=CSLog;
        
        maxAssetID=0;
        
        File theDir = new File(dirName);

        if (!theDir.exists()) 
        {
            try{
                theDir.mkdir();
             } catch(SecurityException ex){
                log.error("Asset DB: Cannot create files directory" + ex.getClass().getName() + " " + ex.getMessage());                
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
 * Returns number of assets in the database
 * @return 
 */    
    
    public int getSize()
    {
        return map.size();
    }

    private void load()
    {
        
        RandomAccessFile aFile;
        
        map.clear();
        mapByTxID.clear();
        mapByAssetRef.clear();
        maxAssetID=0;
        
        try {
            aFile = new RandomAccessFile(fileName, "r");
        } catch (FileNotFoundException ex) {
            log.info("Asset DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            return;
        }
                
        fileSize=0;
        try {  
            fileSize = (int)aFile.length();                    
        } catch (IOException ex) {
            log.error("Asset DB: Cannot get file size " + ex.getClass().getName() + " " + ex.getMessage());                
            return;
        }
        if(fileSize<8)
        {
            log.error("Asset DB: Empty database ");                            
            return;
        }
        
        byte[] raw=new byte[8];
        
        if(!CSUtils.readFromFileToBytes(aFile, raw))
        {
            log.error("Asset DB: Cannot read file");                
            return;
        }            
        
        int off=0;
        off+=4;                                                                 // Version
        maxAssetID=CSUtils.littleEndianToInt(raw, off);
        off+=4;
        
        while(off+8<=fileSize)
        {
            if(!CSUtils.readFromFileToBytes(aFile, raw))
            {
                log.error("Asset DB: Cannot read file");                
                return;
            }            
            
            int AssetID=CSUtils.littleEndianToInt(raw, 0);
            off+=4;
            int AssetLen=CSUtils.littleEndianToInt(raw, 4);
            off+=4;
        
            if(AssetLen>0)
            {
                if(off+AssetLen<=fileSize)
                {
                    byte[] SerializedAsset=new byte[AssetLen];
                    if(!CSUtils.readFromFileToBytes(aFile, SerializedAsset))
                    {
                        log.error("Asset DB: Cannot read file");                
                        return;
                    }            
                    String JSONString;
                    try {
                        JSONString = new String(SerializedAsset, "UTF-8");
                    } catch (UnsupportedEncodingException ex) {
                        log.error("Asset DB: Cannot read asset");                
                        return;
                    }
                    CSAsset asset=new CSAsset(dirName, AssetID, JSONString);
                    map.put(AssetID, asset);
                    if(asset.getGenTxID() != null)
                    {
                        mapByTxID.put(asset.getGenTxID(), AssetID);
                    }
                    if(asset.getAssetReference() != null)
                    {
                        if(asset.getAssetReference().getTxOffset() > 0)
                        {
                            mapByAssetRef.put(asset.getAssetReference().encode(), AssetID);
                        }
                    }
                }
            }
            
            off+=AssetLen;
        }
        
        if(off!=fileSize)
        {
            log.error("Asset DB: Corrupted file, on " + off);                
            if(off<fileSize)
            {
                fileSize=off;
            }
            return;
        }        
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Asset DB: Cannot close file " + ex.getClass().getName() + " " + ex.getMessage());                
        }    
        
        log.info("Asset DB: File opened: " + fileName);                        
    }
    
    private boolean save()
    {
        File bdbFile = new File(fileName + ".new");
                        
        if(bdbFile.exists())
        {
            if(!bdbFile.delete())
            {
                log.error("Asset DB: Cannot deleted temporary file");                
                return false;
            }            
        }
     
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(fileName + ".new", "rw");
        } catch (FileNotFoundException ex) {
            log.info("Asset DB: Cannot open temporary file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        byte [] raw=new byte[8];
        CSUtils.littleEndianByteArray(0, raw, 0);                             // Version
        CSUtils.littleEndianByteArray(maxAssetID, raw, 4);                    
        try {
            aFile.write(raw);
        } catch (IOException ex) {
            log.error("Asset DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        for (Map.Entry<Integer, CSAsset> entryAsset : map.entrySet()) 
        {
            String JSONString=entryAsset.getValue().serialize();
            CSUtils.littleEndianByteArray(entryAsset.getKey(), raw, 0);                             
            try {                    
                CSUtils.littleEndianByteArray(JSONString.getBytes("UTF-8").length, raw, 4);
            } catch (UnsupportedEncodingException ex) {
                log.error("Asset DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
            }
            try {
                aFile.write(raw);
            } catch (IOException ex) {
                log.error("Asset DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
                return false;
            }
            try {
                aFile.write(JSONString.getBytes("UTF-8"));
            } catch (IOException ex) {
                log.error("Asset DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
                return false;
            }            
        }        
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Asset DB: Cannot close db file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }        
        
        bdbFile = new File(fileName + ".new");
                        
        if(bdbFile.exists())
        {
            try {
                Files.move( bdbFile.toPath(), new File(fileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                log.error("Asset DB: Cannot rename temporary file: " + ex.getMessage());                
                return false;
            }
        }
        
        log.info("Asset DB: Database saved");                            
        return true;
    }   
    

/**
 * Returns Asset from database found by one of the following (in this order):
 * Asset ID,
 * Genesis TxID,
 * Asset Reference
 * @param Asset asset object containing one of the key fields
 * @return matching asset object stored in the database
 */
    public CSAsset findAsset(CSAsset Asset)
    {
        return findAsset(Asset,false);
    }
    
    protected CSAsset findAsset(CSAsset Asset,boolean IgnoreID)
    {
        if(Asset == null)
        {
            return null;
        }
        
        CSAsset asset=null;
        
        lock.lock();
        
        try {
            
            if(!IgnoreID)
            {
                if(asset == null)
                {
                    if(Asset.getAssetID() > 0)
                    {
                        asset = map.get(Asset.getAssetID());
                    }
                }
            }
            
            if(asset == null)
            {
                if(Asset.getGenTxID() != null)
                {
                    Integer AssetID=mapByTxID.get(Asset.getGenTxID());
                    if(AssetID != null)
                    {
                        if(AssetID > 0)                
                        {
                            asset = map.get(AssetID);                
                        }
                    }
                }
            }
            
            if(asset == null)
            {
                if(Asset.getAssetReference()!= null)
                {
                    Integer AssetID=mapByAssetRef.get(Asset.getAssetReference().encode());
                    if(AssetID != null)
                    {
                        if(AssetID > 0)                
                        {
                            asset = map.get(AssetID);                
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        
        return asset;
    }

/**
 * Returns asset with specified ID.
 * @param AssetID
 * @return  matching asset object stored in the database
 */
    
    public CSAsset getAsset(int AssetID)
    {
        if(AssetID > 0)
        {
            return map.get(AssetID);
        }
        return null;
    }

/**
 * Inserts new asset into database.
 * @param Asset
 * @return stored Asset on success, null on failure
 */
    
    public CSAsset insertAsset(CSAsset Asset)
    {
        CSAsset asset=Asset;
        
        lock.lock();
                
        try {
            maxAssetID++;
            
            csLog.info("Asset DB: Inserting new asset: " + Asset.status());
            
            if(asset != null)
            {
                if(Asset.getGenTxID() != null)
                {
                    if(mapByTxID.containsKey(Asset.getGenTxID()))
                    {
                        log.error("Asset DB: Asset with TxID " + Asset.getGenTxID() + " already exists, Asset ID: " + mapByTxID.get(Asset.getGenTxID()));                                    
                        csLog.info("Asset DB: Asset with TxID " + Asset.getGenTxID() + " already exists, Asset ID: " + mapByTxID.get(Asset.getGenTxID()));                                    
                        asset = null;
                    }
                    else
                    {
                        mapByTxID.put(Asset.getGenTxID(), maxAssetID);
                    }
                }
            }
            
            if(asset != null)
            {
                if(Asset.getAssetReference() != null)
                {
                    if(Asset.getAssetReference().getTxOffset() > 0)
                    {
                        if(mapByAssetRef.containsKey(Asset.getAssetReference().encode()))
                        {
                            log.error("Asset DB: Asset with reference " + Asset.getAssetReference().encode() + " already exists, Asset ID: " + mapByAssetRef.get(Asset.getAssetReference().encode()));                                    
                            csLog.info("Asset DB: Asset with reference " + Asset.getAssetReference().encode() + " already exists, Asset ID: " + mapByAssetRef.get(Asset.getAssetReference().encode()));                                    
                            asset = null;
                        }
                        else
                        {
                            mapByAssetRef.put(Asset.getAssetReference().encode(), maxAssetID);
                        }
                    }
                }
            }
            
            if(asset != null)
            {
                asset.setAssetID(maxAssetID);

                map.put(maxAssetID, asset);
                if(!save())
                {
                    csLog.warning("Cannot insert asset.");
                    asset=null;
                    load();
                }
                else
                {
                    CSEventBus.INSTANCE.postAsyncEvent(CSEventType.ASSET_INSERTED, maxAssetID);
                    csLog.info("Asset DB: Asset inserted, ID: " + maxAssetID + ": " + asset.status());                    
                }
            }
        } finally {
            lock.unlock();
        }
        
        return asset;
    }

/**
 * Sets Asset referenece to specified asset.
 * @param Asset asset stored in the database
 * @param AssetRef new asset reference
 * @return true on success, false on failure.
 */    

    public boolean setAssetReference(CSAsset Asset, CoinSparkAssetRef AssetRef)
    {        
        if(Asset == null)
        {
            return false;
        }
        
        boolean result=true;
        
        if (Asset.getAssetID() <= 0)
        {
            return false;
        }
                
        lock.lock();
        
        try {
            
            csLog.info("Asset DB: Setting asset reference " + AssetRef.encode() + " to asset " + Asset.getAssetID());
            
            Asset.setAssetRef(AssetRef);
            
            if(AssetRef.getTxOffset() > 0)
            {
                if(!mapByAssetRef.containsKey(AssetRef.encode()))
                {
                    mapByAssetRef.put(AssetRef.encode(), Asset.getAssetID());
                }
                else
                {
                    Integer AssetID=mapByAssetRef.get(AssetRef.encode());
                    if(AssetID != null)
                    {
                        if(AssetID != Asset.getAssetID())
                        {
                            log.error("Asset DB: Asset with Reference " + AssetRef.encode() + " has different Asset ID, " + Asset.getAssetID() + " given "+ AssetID + " found");                                    
                            csLog.info("Asset DB: Asset with Reference " + AssetRef.encode() + " has different Asset ID, " + Asset.getAssetID() + " given "+ AssetID + " found");                                    
                            result=false;
                        }                
                    }
                }
            }
            
            if(result)
            {
                map.put(Asset.getAssetID(), Asset);
                if(!save())
                {
                    csLog.warning("Cannot set asset reference.");
                    result=false;
                    load();
                }
                else
                {
                    csLog.info("Asset DB: Asset reference " + AssetRef.encode() + " set to asset " + Asset.getAssetID());                    
                }
            }
            
        } finally {
            lock.unlock();
        }
        
        
        return result;        
    }

/**
 * Clears asset reference from specified asset.
 * @param Asset stored in the database
 * @return true on success, false on failure.
 */
    
    public boolean clearAssetReference(CSAsset Asset)
    {        
        if(Asset == null)
        {
            return false;
        }
        
        boolean result=true;
        
        if (Asset.getAssetID() <= 0)
        {
            return false;
        }
        
        if(Asset.getAssetReference() == null)
        {
            return true;
        }
        
        lock.lock();
        
        try {
            
            csLog.info("Asset DB: Clearing asset reference from asset " + Asset.getAssetID());
            
            if(mapByAssetRef.containsKey(Asset.getAssetReference().encode()))
            {
                mapByAssetRef.remove(Asset.getAssetReference().encode());
            }
            
            Asset.setAssetRef(null);           
            
            if(result)
            {
                map.put(Asset.getAssetID(), Asset);
                if(!save())
                {
                    csLog.warning("Cannot clear asset reference.");
                    result=false;
                    load();
                }
                else
                {
                    csLog.info("Asset DB: Asset reference cleared from asset " + Asset.getAssetID());                    
                }
            }
            
        } finally {
            lock.unlock();
        }
        
        return result;        
    }
    
/**
 * Sets asset visibility
 * @param Asset stored in database
 * @param Visibility to set
 * @return true on success, false on failure.
 */    
    
    public boolean setAssetVisibility(CSAsset Asset,boolean Visibility)
    {
        if(Asset == null)
        {
            return false;
        }
        
        if (Asset.getAssetID() <= 0)
        {
            return false;
        }
        
        if(Visibility != Asset.isVisible())
        {
            csLog.info("Asset DB: Setting asset " + Asset.getAssetID() + " visibility to " + Visibility);
            Asset.setVisibility(Visibility);
            return updateAsset(Asset);            
        }
        
        return true;
    }
    
/**
 * Updates Asset in database
 * @param Asset
 * @return true on success, false on failure.
 */
    
    public boolean updateAsset(CSAsset Asset)
    {
        if(Asset == null)
        {
            return false;
        }
        
        boolean result=true;
        boolean isDuplicate=false;
        boolean checkDuplicate=true;
                
        if (Asset.getAssetID() <= 0)
        {
            return false;
        }
        
        lock.lock();
        
        try {
            csLog.info("Asset DB: Updating asset " + Asset.getAssetID());
            
            while(checkDuplicate)
            {
                checkDuplicate=false;
                if(result)
                {
                    if(Asset.getGenTxID() != null)
                    {
                        if(!mapByTxID.containsKey(Asset.getGenTxID()))
                        {
                            mapByTxID.put(Asset.getGenTxID(), Asset.getAssetID());
                        }
                        else
                        {
                            Integer AssetID=mapByTxID.get(Asset.getGenTxID());
                            if(AssetID != null)
                            {
                                if(AssetID != Asset.getAssetID())
                                {
                                    if(!isDuplicate)
                                    {
                                        csLog.warning("Asset DB: Asset with TxID " + Asset.getGenTxID() + " has different Asset ID, " + Asset.getAssetID() + " given, "+ AssetID + " found");                                    
                                        checkDuplicate=true;
                                        isDuplicate=true;
                                    }
                                }                
                                else
                                {
                                    if(isDuplicate)
                                    {
                                        mapByTxID.remove(Asset.getGenTxID());
                                    }
                                }
                            }
                        }
                    }
                }

                if(result)
                {
                    if(Asset.getAssetReference() != null)
                    {
                        if(Asset.getAssetReference().getTxOffset() > 0)
                        {
                            if(!mapByAssetRef.containsKey(Asset.getAssetReference().encode()))
                            {
                                mapByAssetRef.put(Asset.getAssetReference().encode(), Asset.getAssetID());
                            }
                            else
                            {
                                Integer AssetID=mapByAssetRef.get(Asset.getAssetReference().encode());
                                if(AssetID != null)
                                {
                                    if(AssetID != Asset.getAssetID())
                                    {
                                        if(!isDuplicate)
                                        {
                                            csLog.warning("Asset DB: Asset with Reference " + Asset.getAssetReference().encode() + " has different Asset ID, " + Asset.getAssetID() + " given, "+ AssetID + " found");                                    
                                            checkDuplicate=true;
                                            isDuplicate=true;
                                        }
                                    }                
                                }
                                else
                                {
                                    if(isDuplicate)
                                    {
                                        mapByTxID.remove(Asset.getGenTxID());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if(isDuplicate)
            {
                Asset.setAssetState(CSAsset.CSAssetState.DUPLICATE);
            }
            
            if(result)
            {
                map.put(Asset.getAssetID(), Asset);
                if(!save())
                {
                    csLog.warning("Cannot update asset.");
                    result=false;
                    load();
                }
                else
                {
                    CSEventBus.INSTANCE.postAsyncEvent(CSEventType.ASSET_UPDATED, Asset.getAssetID());
                    csLog.info("Asset DB: Asset updated, ID: " + Asset.getAssetID() + ": " + Asset.status() + " (" + Asset.getValidFailures() + ")");                    
                }
            }
        } finally {
            lock.unlock();
        }
        return result;
    }

/**
 * Deletes Asset from database;
 * @param Asset aseet stored in the databse
 * @return true on success, false on failure.
 */
    public boolean deleteAsset(CSAsset Asset)
    {
        return deleteAsset(Asset,true);
    }
    
    protected boolean deleteAsset(CSAsset Asset,boolean RemoveMaps)
    {
        if(Asset == null)
        {
            return false;
        }
        
        boolean result=true;
        
        if (Asset.getAssetID() <= 0)
        {
            return false;
        }
        
        lock.lock();
        
        try {
            csLog.info("Asset DB: Deleting asset " + Asset.getAssetID());
            
            if(RemoveMaps)
            {
                if(Asset.getGenTxID() != null)
                {
                    if(mapByTxID.containsKey(Asset.getGenTxID()))
                    {
                        mapByTxID.remove(Asset.getGenTxID());
                    }
                }
                if(Asset.getAssetReference() != null)
                {
                    if(Asset.getAssetReference().getTxOffset() > 0)
                    {
                        if(mapByAssetRef.containsKey(Asset.getAssetReference().encode()))
                        {
                            mapByAssetRef.remove(Asset.getAssetReference().encode());
                        }
                    }
                }
            }
            
            if(map.containsKey(Asset.getAssetID()))
            {
                map.remove(Asset.getAssetID());
            }
            
            if(!save())
            {
                csLog.warning("Cannot delete asset.");
                result=false;
                load();
            }
            else
            {
                if(Asset.getContractPath() != null && !Asset.getContractPath().isEmpty())
                {
                    File aFile = new File(Asset.getContractPath());
                    if(aFile.exists())
                    {
                        aFile.delete();
                    }                    
                }
                
                if(Asset.getIconPath() != null && !Asset.getIconPath().isEmpty())
                {
                    File aFile = new File(Asset.getContractPath());
                    if(aFile.exists())
                    {
                        aFile.delete();
                    }                    
                }
                
                if(Asset.getImagePath()!= null && !Asset.getImagePath().isEmpty())
                {
                    File aFile = new File(Asset.getContractPath());
                    if(aFile.exists())
                    {
                        aFile.delete();
                    }                    
                }
                
                CSEventBus.INSTANCE.postAsyncEvent(CSEventType.ASSET_DELETED, Asset.getAssetID());
                csLog.info("Asset DB: Asset deleted, ID: " + Asset.getAssetID() + ": " + Asset.status() + " (" + Asset.getValidFailures() + ")");                    
            }
        } finally {
        
            lock.unlock();
        }
            
        return result;
    }

        
/**
 * Returns array of all Asset IDs in database
 * @return array of Asset IDs
 */
    
    public int[] getAssetIDs() 
    {
        int size=getSize();
        if(size <= 0)
        {
            return null;
        }
        
        int [] raw=new int[size];
        int count=0;
        
        lock.lock();
        
        try {
            for (Map.Entry<Integer, CSAsset> entryAsset : map.entrySet())
            {
                if(entryAsset.getKey() > 0)
                {
                    raw[count]=entryAsset.getKey();
                    count++;
                }
            }
        } finally {
            lock.unlock();
        }
        
        if(count==size)
        {
            return raw;
        }
        
        return Arrays.copyOf(raw, count);
    }

/**
 * Sets refresh flag for specific asset
 * @param AssetID 
 * @param pg PeerGroup object
 */
    
    public void refreshAsset(int AssetID,PeerGroup pg)
    {
        CSAsset asset=getAsset(AssetID);
        
        if(asset != null)
        {
            asset.setRefreshState();
        
            boolean updateRequired=false;
        
            updateRequired |= asset.validateAssetRef(pg);
            
            updateRequired |= asset.validate(dirName,pg,true);

            if(updateRequired)
            {
                updateAsset(asset);
            }
        }
        
    }
    
    
    
    private boolean validationInProgress=false;
    
/**
 * Validates all assets in the database.
 * @param pg PeerGroup object
 * @param ForceRefresh validate asset regardless of validation interval
 */
    
    public void validateAssets(PeerGroup pg,boolean ForceRefresh)
    {
        if(validationInProgress){
            return;            
        }
        
        validationInProgress=true;

        HashMap<Integer, CSAsset> mapCopy=(HashMap<Integer, CSAsset>)map.clone();
                                
        for (Map.Entry<Integer, CSAsset> entryAsset : mapCopy.entrySet()) 
        {
            CSAsset asset=entryAsset.getValue();
            
            boolean updateRequired=false;
                    
            if(asset.getAssetState() != CSAsset.CSAssetState.DUPLICATE)
            {
                updateRequired |= asset.validateAssetRef(pg);

                updateRequired |= asset.validate(dirName,pg,ForceRefresh);

                if(updateRequired)
                {
                    updateAsset(asset);
                    CSEventBus.INSTANCE.postAsyncEvent(CSEventType.ASSET_VALIDATION_COMPLETED, asset.getAssetID());
                }
            }
        }        

        validationInProgress=false;
    }
    
/**
 * Validates all assets in the database.
 * @param pg  PeerGroup object
 */
    
    public void validateAssets(PeerGroup pg)
    {
        validateAssets(pg,false);
    }

}
