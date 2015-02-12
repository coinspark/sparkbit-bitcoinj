/*
 * Copyright 2014 Coin Sciences Ltd.
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
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.stmt.PreparedQuery;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;
import java.io.File;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.coinspark.core.CSLogger;
import org.coinspark.core.CSUtils;
import org.coinspark.protocol.CoinSparkMessage;
import org.coinspark.protocol.CoinSparkMessagePart;
import org.coinspark.protocol.CoinSparkPaymentRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.h2.mvstore.*;

/**
 * 
 */
public class CSMessageDatabase {
 
    private static final Logger log = LoggerFactory.getLogger(CSAssetDatabase.class);
    private CSLogger csLog;
    
//    protected final ReentrantLock lock = Threading.lock("assetdb");
    
    // Set this to true and all messages will enable testnet=true JSON parameter.
    public static boolean testnet3 = false;
    
    // Suffix to add to filename
    private static final String TESTNET3_FILENAME_SUFFIX = "-testnet3";
    
    // Suffix to use for messages folder, appeneded to name of wallet
    private static final String MESSAGE_DIR_SUFFIX = ".csmessages";
    
    private static final String MESSAGE_DATABASE_FILENAME = "messages";
    
    private static final String MESSAGE_META_KEYSTORE_FILENAME = "meta";
    
    private static final String MESSAGE_BLOB_KEYSTORE_FILENAME = "sparkbit-blobs";
    
    private static final String MESSAGE_MVSTORE_FILE_EXTENSION = ".mv.db";
   
    private static final String MESSAGE_PART_TO_BLOB_MAP_NAME = "mesagepart_blob";
    private static final String MESSAGE_TXID_TO_META_MAP_NAME = "txid_meta";
    private static final String MESSAGE_TXID_TO_ERROR_MAP_NAME = "txid_error";
    
    private String fileName;
    private String dirName;
    private Wallet wallet;
    
    // H2 database
    ConnectionSource connectionSource;
    Dao<CSMessage, String> messageDao;
    Dao<CSMessagePart, Long> messagePartDao;
	    
    private MVStore kvStore;
    private MVMap<String, Object> defMap;
    private MVMap<String, Integer> errorMap;
    
    // A H2 MVStore for Blobs
    private static MVStore blobStore;
    
    // A map for message blobs: txid:partid --> BLOB
    private static MVMap<String, byte[]> blobMap;
    
    /**
     * Initialize the blob map once, to be shared by all CSMessageDatabases
     * @param path
     * @return true if blob map exists.
     */
    private static synchronized boolean initBlobMap(String path) {
	if (blobMap != null) {
	    return true;
	}
	
	blobStore = MVStore.open(path);
	if (blobStore != null) {
	    blobMap = blobStore.openMap(MESSAGE_PART_TO_BLOB_MAP_NAME);
	}
	return (blobMap != null);
    }
    
    public CSMessageDatabase(String FilePrefix,CSLogger CSLog,Wallet ParentWallet)
    {
        dirName = FilePrefix + MESSAGE_DIR_SUFFIX + File.separator;
        fileName = dirName + MESSAGE_DATABASE_FILENAME + ((CSMessageDatabase.testnet3) ? TESTNET3_FILENAME_SUFFIX : ""); // H2 will add .mv.db extension itself
        csLog=CSLog;
        wallet=ParentWallet;
	
	
	String folder = FilenameUtils.getFullPath(FilePrefix);
	String name = MESSAGE_BLOB_KEYSTORE_FILENAME + ((CSMessageDatabase.testnet3) ? TESTNET3_FILENAME_SUFFIX : "") + MESSAGE_MVSTORE_FILE_EXTENSION;
	String blobPath = FilenameUtils.concat(folder, name);
	boolean b = CSMessageDatabase.initBlobMap(blobPath);
	if (!b) {
	    log.error("Message DB: Could not create BLOB storage map at: " + blobPath);
	    return;
	}
	
	File dir = new File(dirName);
	if (!dir.exists()) {
	    // Files.createDirectory(Paths.get(dirName));
	    try {
		dir.mkdir();
	    } catch (SecurityException ex) {
		log.error("Message DB: Cannot create files directory" + ex.getClass().getName() + " " + ex.getMessage());
		return;
	    }
	}
	
	
	String kvStoreFileName = dirName + MESSAGE_META_KEYSTORE_FILENAME + ((CSMessageDatabase.testnet3) ? TESTNET3_FILENAME_SUFFIX : "") + MESSAGE_MVSTORE_FILE_EXTENSION;
	kvStore = MVStore.open(kvStoreFileName);
	if (kvStore != null) {
	    defMap = kvStore.openMap(MESSAGE_TXID_TO_META_MAP_NAME);
	    errorMap = kvStore.openMap(MESSAGE_TXID_TO_ERROR_MAP_NAME);
	}
	
	// TODO?: This database URL could be passed in via constructor
	String databaseUrl = "jdbc:h2:file:" + fileName + ";USER=sa;PASSWORD=sa;AUTO_SERVER=TRUE";

	try {
	    connectionSource = new JdbcConnectionSource(databaseUrl);
	    messageDao = DaoManager.createDao(connectionSource, CSMessage.class);
	    TableUtils.createTableIfNotExists(connectionSource, CSMessage.class);

	    messagePartDao = DaoManager.createDao(connectionSource, CSMessagePart.class);
	    TableUtils.createTableIfNotExists(connectionSource, CSMessagePart.class);

	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }
    
    // FIXME: make sure we free resources
    public void shutdown() {
	if (connectionSource != null) {
	    connectionSource.closeQuietly();
	}
	if (kvStore != null) {
	    kvStore.commit();
	    kvStore.close();
	}
    }
    
    public static synchronized void shutdownBlobStore() {
	if (blobStore != null) {
	    blobStore.commit();
	    blobStore.close();
	}
    }
    
    // FIXME: we want to have a connection pool for a central databaes
    public ConnectionSource getConnectionSource() {
	return connectionSource;
    }
    
    public Dao<CSMessage, String> getMessageDao() {
	return messageDao;
    }

    public Dao<CSMessagePart, Long> getMessagePartDao() {
	return messagePartDao;
    }

    public MVMap getDefMap() {
	return defMap;
    }
    
    public MVMap getErrorMap() {
	return errorMap;
    }
    
    public Integer getServerErrorCode(String txid) {
	if (errorMap==null || txid==null) return null;
	Integer result = errorMap.get(txid);
	return result;
    }
    
    public void putServerError(String txid, CSUtils.CSServerError errorCode) {
	errorMap.put(txid, errorCode.getCode());
	kvStore.commit();
    }

    
    public CSMessagePart getMessagePart(String txid, int partID) {
	QueryBuilder<CSMessagePart, Long> queryBuilder = messagePartDao.queryBuilder();
	try {
	    queryBuilder.where().eq(CSMessagePart.MESSAGE_ID_FIELD_NAME, txid).and().eq(CSMessagePart.PART_ID_FIELD_NAME, partID);
	    PreparedQuery<CSMessagePart> pq = queryBuilder.prepare();
	    List<CSMessagePart> list = messagePartDao.query(pq);
	    if (list.size()==1) {
		return list.get(0);
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return null;
    }
    
//    public List<CSMessagePart> getMessagePartsOrdered(String txid) {
//	QueryBuilder<CSMessagePart, Long> queryBuilder = messagePartDao.queryBuilder();
//	try {
//	    queryBuilder.where().eq(CSMessagePart.MESSAGE_ID_FIELD_NAME, txid);
//	    queryBuilder.orderBy(CSMessagePart.PART_ID_FIELD_NAME, true);
//	    PreparedQuery<CSMessagePart> pq = queryBuilder.prepare();
//	    List<CSMessagePart> list = messagePartDao.query(pq);
//	    return list;
//	} catch (SQLException e) {
//	    e.printStackTrace();
//	}
//	return null;
//    }   
    
    
    
    public boolean insertReceivedMessage(String TxID,int countOutputs,CoinSparkPaymentRef PaymentRef,CoinSparkMessage Message,String [] Addresses)
    {
	log.debug(">>>> insertReceivedMessage() for wallet " + wallet.getDescription());

        if((Message == null) && (PaymentRef == null))
        {
            return false;
        }
        
	if (messageExists(TxID)) {
            log.info("Message DB: TxID " + TxID + " already in the database");                            
            return true;
        }
        
        boolean result=true;
        CSMessage message=new CSMessage(this);
        
        try {                 
            
            result=message.set(TxID, countOutputs, PaymentRef, Message,  Addresses);
            
            if(result)
            {
		messageDao.createIfNotExists(message);
		
                log.info("Message DB: Tx " + TxID + " inserted");                

            }
            else
            {
                log.info("Message DB: Cannot insert message for Tx " + TxID + " inserted");                                
            }

	} catch (SQLException e) {
	    e.printStackTrace();
        }

        if(result)
        {
            csLog.info("Message DB: Inserted new Tx: " + TxID + ", State: " + message.getMessageState());
        }
        
        return result;
    }

    public boolean insertSentMessage(String TxID,int countOutputs,CoinSparkPaymentRef PaymentRef,CoinSparkMessage Message,CoinSparkMessagePart [] MessageParts,CSMessage.CSMessageParams MessageParams)
    {
	log.debug(">>>> insertSentMessage() for wallet " + wallet.getDescription());
	
	
        if((Message == null) && (PaymentRef == null))
        {
            return false;
        }

	if (messageExists(TxID))
        {
            log.info("Message DB: TxID " + TxID + " already in the database");                            
            return true;
        }
        
        boolean result=true;
        CSMessage message=new CSMessage(this);
        
        try {                 
            
            result=message.set(TxID, countOutputs, PaymentRef, Message, MessageParts,MessageParams);
            
            if(result)
            {
		messageDao.createIfNotExists(message);
		log.info("Message DB: Tx " + TxID + " inserted");            
		
//		 persistMessageParts(message.getMessageParts());

            }
            else
            {
                log.info("Message DB: Cannot insert message for Tx " + TxID + " inserted");                                
            }

	} catch (SQLException e) {
	    e.printStackTrace();
        }
	
        if(result)
        {
            csLog.info("Message DB: Inserted new Tx: " + TxID + ", State: " + message.getMessageState());
        }
        
        return result;
    }

    public boolean updateMessage(String TxID,CSMessage Message)
    {
        if(Message == null)
        {
            return false;
        }
        
	if (messageExists(TxID)==false)
        {
            log.info("Message DB: TxID " + TxID + " not in the database");                            
            return false;
        }
        
        boolean result=true;
        
        try {
    
	    messageDao.update(Message);
            log.info("Message DB: Tx " + TxID + " updated");                

	} catch (SQLException e) {
	    e.printStackTrace();
        }
	
        csLog.info("Message DB: Updated Tx: " + TxID + ", State: " + Message.getMessageState());
        return result;
    }
    
    public boolean messageExists(String txid) {
	boolean b = false;
	try {
	     b = messageDao.idExists(txid);
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return b;
    }
    
    public CSMessage getMessage(String txid) {
	CSMessage msg = null;
	try {
	     msg = messageDao.queryForId(txid);
	     if (msg!=null) msg.init(this);
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return msg;
    }


    private boolean retrievalInProgress=false;
    
    public void retrieveMessages()
    {
	if (!getConnectionSource().isOpen()) {
	    return;
	}
		log.debug(">>>> retrieveMessages() for wallet " + wallet.getDescription());

        if(retrievalInProgress){
            return;            
        }
        
        retrievalInProgress=true;

        // query for all items in the database
	try {
	    
	    QueryBuilder<CSMessage, String> queryBuilder = messageDao.queryBuilder();
	    queryBuilder.where().ne(CSMessage.MESSAGE_STATE_FIELD_NAME, CSMessage.CSMessageState.PAYMENTREF_ONLY)
		    .and().ne(CSMessage.MESSAGE_STATE_FIELD_NAME, CSMessage.CSMessageState.SELF)
		    .and().ne(CSMessage.MESSAGE_STATE_FIELD_NAME, CSMessage.CSMessageState.VALID);
	    PreparedQuery<CSMessage> pq = queryBuilder.prepare();
	    List<CSMessage> messages = messageDao.query(pq); //queryForAll();
	    // NOTE: could also be CSMessage message : messageDao (which can act as iterator)
	    for (CSMessage message : messages) {
		
		log.debug(">>>> mayBeRetrieve() for wallet " + wallet.getDescription());

		// Initialise message with MessageDatabase
		// Other things may need to be set e.g. CSMessageParams bean, or CSMessageMeta bean.
		message.init(this);
		
		// Ignore messages we don't need to retrieve, here, or via query
		/*
		if (message.getMessageState()==CSMessage.CSMessageState.PAYMENTREF_ONLY || message.getMessageState()==CSMessage.CSMessageState.SELF || message.getMessageState()==CSMessage.CSMessageState.VALID) {
		    continue;
		}
		*/
		
		
		log.debug(">>>> Invoke mayBeRetrieve for " + message.getTxID());
		
		if(message.mayBeRetrieve(wallet)) {

		    log.debug(">>>> mayBeRetrieve returned TRUE");
		    // TODO: See ormlite docs about a better way? e.g.  account.orders.update(order);
		    
//		    persistMessageParts(message.getMessageParts());

		    // query for id below will refresh message object
				    
		    updateMessage(message.getTxID(), message);
		    CSEventBus.INSTANCE.postAsyncEvent(CSEventType.MESSAGE_RETRIEVAL_COMPLETED, message.getTxID());
		}
		else {
		    // update fields even if parts not saved.
		     updateMessage(message.getTxID(), message);
		}
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	}     

        retrievalInProgress=false;
    }
    
    /**
     * Message parts are only persisted when they are created by DAO
     * @param parts
     * @throws SQLException 
     */
    public void persistMessageParts(List<CSMessagePart> parts) throws SQLException {
	if (parts!=null) {
	    for (CSMessagePart part : parts) {
		Dao.CreateOrUpdateStatus status = messagePartDao.createOrUpdate(part);
		log.debug(">>>> Invoke messagePartDao.createOrUpdate() for part:");
		log.debug(">>>>  partID = " + part.partID);
		log.debug(">>>>  fileName = " + part.fileName);
		log.debug(">>>>  contentSize =" + part.contentSize);
		log.debug(">>> status created? = " + status.isCreated());
		log.debug(">>> status updated? = " + status.isUpdated());
		log.debug(">>> status lines changed  = " + status.getNumLinesChanged());
	    }
	}
    }
    
    public static byte[] getBlobForMessagePart(String txid, int partID) {
	return blobMap.get(txid + ":" + partID);
    }

    public static void putIfAbsentBlobForMessagePart(String txid, int partID, byte[] blob) {
	blobMap.put(txid + ":" + partID, blob);
	blobStore.commit();
    }

    public static MVMap getBlobMap() {
	return blobMap;
    }
//    public void putBlobForMessageParts(List<CSMessagePart> parts)  {
//	if (parts!=null) {
//	    for (CSMessagePart part : parts) {
//		part.getClass()
//	    }	    
//	}
//    }
}
