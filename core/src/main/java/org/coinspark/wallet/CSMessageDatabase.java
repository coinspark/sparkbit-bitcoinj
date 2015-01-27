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
import com.google.bitcoin.utils.Threading;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.coinspark.core.CSLogger;
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
    
    private static final String MESSAGE_DB_SUFFIX = "messages.csdb";
    private static final String MESSAGE_DIR_SUFFIX = ".csmessages";
    private static final String MESSAGE_H2_DB_SUFFIX = "messages.h2";
    private static final String MESSAGE_H2_KVSTORE_SUFFIX = "messages.h2.kvstore";
    private static final String MESSAGE_TX_DEF_MAP_NAME = "tx_def";
    
    private String fileName;
    @Deprecated
    private String dirName;
    private int fileSize;
    private Wallet wallet;
    
    // H2 database
    ConnectionSource connectionSource;
    Dao<CSMessage, String> messageDao;
    Dao<CSMessagePart, Long> messagePartDao;
    
    private MVStore kvStore;
    private MVMap<String, Object> defMap;
	    
    public CSMessageDatabase(String FilePrefix,CSLogger CSLog,Wallet ParentWallet)
    {
        dirName = FilePrefix + MESSAGE_DIR_SUFFIX+File.separator;
        fileName = dirName + MESSAGE_H2_DB_SUFFIX;
        csLog=CSLog;
        wallet=ParentWallet;
	
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
	
	
	String kvStoreFileName = dirName + MESSAGE_H2_KVSTORE_SUFFIX;
	kvStore = MVStore.open(kvStoreFileName);
	if (kvStore != null) {
	    defMap = kvStore.openMap(MESSAGE_TX_DEF_MAP_NAME);
	}
	
	// FIXME: This database URL could be passed in via constructor
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
	connectionSource.closeQuietly();
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
		
		 updateMessageParts(message);

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
	     msg.init(this);
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return msg;
    }


    private boolean retrievalInProgress=false;
    
    public void retrieveMessages()
    {
		log.debug(">>>> retrieveMessages() for wallet " + wallet.getDescription());

        if(retrievalInProgress){
            return;            
        }
        
        retrievalInProgress=true;

        // query for all items in the database
	try {
	    // TODO: Query where not SELF or VALID state, order etc...
	    List<CSMessage> messages = messageDao.queryForAll();
	    // NOTE: could also be CSMessage message : messageDao (which can act as iterator)
	    for (CSMessage message : messages) {
		
		log.debug(">>>> mayBeRetrieve() for wallet " + wallet.getDescription());

		// Initialise message with MessageDatabase
		// Other things may need to be set e.g. CSMessageParams bean, or CSMessageMeta bean.
		message.init(this);
		
		log.debug(">>>> Invoke mayBeRetrieve for " + message.getTxID());
		
		if(message.mayBeRetrieve(wallet)) {

		    log.debug(">>>> mayBeRetrieve returned TRUE");
		    // TODO: See ormlite docs about a better way? e.g.  account.orders.update(order);
		    
		    updateMessageParts(message);

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
    
    public void updateMessageParts(CSMessage message) throws SQLException {
	HashSet<CSMessagePart> set = message.retrievedMessageParts;
	if (set != null) {
	    log.debug(">>>> retrievedMessageParts number = " + set.size());
	    for (CSMessagePart part : set) {
		log.debug(">>>> Invoke messagePartDao.createOrUpdate() for part:");
		log.debug(">>>>  fileName = " + part.fileName);
		log.debug(">>>>  contentSize =" + part.contentSize);
		log.debug(">>>>  partID = " + part.partID);

		Dao.CreateOrUpdateStatus status = messagePartDao.createOrUpdate(part);
		log.debug(">>> status created? = " + status.isCreated());
		log.debug(">>> status updated? = " + status.isUpdated());
		log.debug(">>> status lines changed  = " + status.getNumLinesChanged());
	    }
	}
    }
}
