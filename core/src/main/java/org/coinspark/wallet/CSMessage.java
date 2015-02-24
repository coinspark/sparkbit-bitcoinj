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

import com.google.bitcoin.core.Address;
import com.google.bitcoin.core.AddressFormatException;
import com.google.bitcoin.core.ECKey;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.Utils;
import com.google.bitcoin.core.Wallet;
import com.google.bitcoin.crypto.KeyCrypterException;
import com.google.bitcoin.crypto.TransactionSignature;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.beanutils.BeanUtils;
import org.coinspark.core.CSExceptions;
import org.coinspark.core.CSUtils;
import org.coinspark.protocol.CoinSparkMessage;
import org.coinspark.protocol.CoinSparkMessagePart;
import org.coinspark.protocol.CoinSparkPaymentRef;
import org.h2.mvstore.MVMap;
import org.slf4j.LoggerFactory;
import org.spongycastle.crypto.params.KeyParameter;
import org.apache.commons.lang3.tuple.*;
import org.apache.commons.codec.binary.Base64;

/**
 * CSMessage is persisted via ORMLite to a database.
 */
@DatabaseTable(tableName = "messages")
public class CSMessage {
    
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(CSMessage.class);
    
    // Store password here when trying to retrieve, Txid->Password
    // TODO: Use expiring hashmap in future
    private static ConcurrentHashMap<String, KeyParameter> txidWalletPasswordMap = new ConcurrentHashMap<String, KeyParameter>();
    public static void addTxidWalletPassword(String txid, KeyParameter aesKey) {
	txidWalletPasswordMap.put(txid, aesKey);
    }
    
    /**
     * Message state.  Instead of an enumerated type, we use int which can be persisted to database.
     */
    public class CSMessageState {

	public static final int PAYMENTREF_ONLY = 15;                      // Message contain only payment reference
	public static final int NEVERCHECKED = 0;                                                          // Message is just created, not retrieved yet
	public static final int NOT_FOUND = 2;                                                              // Message is not found on delivery server
	public static final int PENDING = 3;                                                             // Message is found on delivery server, but its validity is not confirmed yet
	public static final int EXPIRED = 4;                                                                // Message is expired and cannot be retrieved
	public static final int INVALID = 5;                                                                // Invalid for some reason, not downloaded completely, for example
	public static final int HASH_MISMATCH = 6;                                                          // Hash of retrieved message down't match encoded in metadata        
	public static final int REFRESH = 7;                                                                // We should try to retrieve this message again
	public static final int VALID = 1;                                                                 // Message is fully retrieved and hash is verified
	public static final int SELF = 8;                                                                   // Message was creted by us
	public static final int SERVER_NOT_RESPONDING = 9;                                                  // Message server not found
	public static final int SERVER_ERROR = 10;                                                           // HTTPError on message server 
	public static final int ENCRYPTED_KEY = 11;                                                          // The keys in this wallet is encrypted, message cannot be retrieved without aesKey
	public static final int ADDRESSES_NOT_ACCEPTED = 12;                                                 // All addresses are not accepted
	public static final int ADDRESSES_SUSPENDED = 13;                                                    // All addresses are either not accepted or suspended
	public static final int DELETED = 14;                                                                // Message should be deleted from the database
    }

    /**
     * Java bean containing parameters used to send message and attachments to the delivery servers.
     * These parameters are copied to CSMessageMeta for persisting to a per-wallet txid->meta map.
     */
    public class CSMessageParams {

//	public boolean isTestnet() {
//	    return testnet;
//	}
//
//	public void setTestnet(boolean testnet) {
//	    this.testnet = testnet;
//	}

	public String getSender() {
	    return sender;
	}

	public void setSender(String sender) {
	    this.sender = sender;
	}

	public String getSalt() {
	    return salt;
	}

	public void setSalt(String salt) {
	    this.salt = salt;
	}

	public boolean isIsPublic() {
	    return isPublic;
	}

	public void setIsPublic(boolean isPublic) {
	    this.isPublic = isPublic;
	}

	public String[] getRecipients() {
	    return recipients;
	}

	public void setRecipients(String[] recipients) {
	    this.recipients = recipients;
	}

	public int getKeepseconds() {
	    return keepseconds;
	}

	public void setKeepseconds(int keepseconds) {
	    this.keepseconds = keepseconds;
	}

	public boolean isIsSent() {
	    return isSent;
	}

	public void setIsSent(boolean isSent) {
	    this.isSent = isSent;
	}
	
	// FIXME: testnet is temporary fudge
//	public boolean testnet = true;

	public String sender = null;
	public String salt = null;
	public boolean isPublic = false;
	public String[] recipients = null;
	public int keepseconds = 0;
	public boolean isSent = false;
    }

    /**
     * Metadata concerning this message, from the perspective of a wallet.
     * Subclass of CSMessageParams so you can copy/set parameters here.
     * Metadata is stored in a txid->metadata map.
     */
    public class CSMessageMetadata extends CSMessageParams {

	public String getHash() {
	    return this.hash;
	}

	public void setHash(String hash) {
	    this.hash = hash;
	}

	public int getHashLen() {
	    return hashLen;
	}

	public void setHashLen(int hashLen) {
	    this.hashLen = hashLen;
	}

	public String getEncoded() {
	    return encoded;
	}

	public void setEncoded(String encoded) {
	    this.encoded = encoded;
	}

	public String getServerURL() {
	    return serverURL;
	}

	public void setServerURL(String serverURL) {
	    this.serverURL = serverURL;
	}

	public int getCountOutputs() {
	    return countOutputs;
	}

	public void setCountOutputs(int countOutputs) {
	    this.countOutputs = countOutputs;
	}

	public String[] getAddresses() {
	    return addresses;
	}

	public void setAddresses(String[] addresses) {
	    this.addresses = addresses;
	}
	
	public String hash;	// lowercase hex string
	public int hashLen;
	public String encoded;
	public String serverURL;
	public int countOutputs;
	public String[] addresses;
    }

    //
    // ORMLITE
    //
    
    // Ormlite column names    
    public static final String TXID_FIELD_NAME = "txid";
    public static final String MESSAGE_STATE_FIELD_NAME = "state";
    public static final String PAYMENT_REFERENCE_FIELD_NAME = "paymentRef";
    public static final String LAST_CHECKED_FIELD_NAME = "lastChecked";
    public static final String FAILURES_FIELD_NAME = "failures";

    @DatabaseField(id = true, columnName = TXID_FIELD_NAME, canBeNull = false)
    private String txID;

    @DatabaseField(columnName = PAYMENT_REFERENCE_FIELD_NAME, canBeNull = true)
    private long paymentRefValue;

    @DatabaseField(columnName = LAST_CHECKED_FIELD_NAME, canBeNull = true, dataType = DataType.DATE_LONG)
    private Date lastChecked;

    // If lastChecked is null, then failures is 0.
    @DatabaseField(columnName = FAILURES_FIELD_NAME, canBeNull = false)
    private int failures;

    // To persist a collection, you must first get a Dao for the class and then create each of the objects using the Dao
    // ForeignCollection is abstract and cannot be instantiated.
    // When a successful query to the database returns, this collection will be available for populating.
    @ForeignCollectionField(eager = false)
    ForeignCollection<CSMessagePart> messageParts;

    @DatabaseField(columnName = MESSAGE_STATE_FIELD_NAME)
    private int messageState;

    private CSMessageDatabase db;

    private int messageRetrievalState;
    
    // Convenience function to hide that hash byte[] is stored as string.
    public byte[] getHashBytes() {
	return CSUtils.hex2Byte(meta.hash);
    }

    public void setHashBytes(byte[] bytes) {
	meta.setHash(CSUtils.byte2Hex(bytes));
    }

    private int numParts;

    private boolean corrupted = false;

    private KeyParameter aesKey = null;

    private CoinSparkPaymentRef paymentRef = null;

    private CSMessageMetadata meta = null;
    
    // The actual server URL we will use to connect to delivery servers.
    // If we converted hostname to IP successfully, this will be the IPv4 address.
    private String actualServerURL = null; 
    
    public String getActualServerURL() {
	return actualServerURL;
    }
    
    // Set isRetrieving to true when actually making JSON queries etc.
    private boolean isRetrieving = false;
    public boolean getIsRetrieving() {
	return isRetrieving;
    }

    public String getTxID() {
	return txID;
    }

    public Date getLastChecked() {
	return lastChecked;
    }

    public int getFailures() {
	return failures;
    }

    public boolean isPublic() {
	return (this.meta == null) ? false : this.meta.isPublic;
    }

    public int getMessageState() {
	return this.messageState;
    }

    public boolean isCorrupted() {
	return this.corrupted;
    }

    public ForeignCollection<CSMessagePart> getMessagePartsForeignCollection() {
	return this.messageParts;
    }
    
    public List<CSMessagePart> getMessageParts() {
	if (this.messageParts != null) {
	    return new ArrayList(this.messageParts);
	}
	return null;
    }
    
    public List<CSMessagePart> getMessagePartsSortedByPartID() {
	if (this.messageParts != null) {
	    List sorted = new ArrayList(this.messageParts);
	    Collections.sort(sorted);
	    return sorted;
	}
	return null;
    }
    
    public String getServerURL() {
	return meta.getServerURL();
    }

    public long getPaymentRefValue() {
	return paymentRefValue;
    }

    /**
     * Return payment reference object, creating it if necessary in the case of
     * CSMessage being instantiated from database with value in paymentRefValue.
     *
     * @return
     */
    public CoinSparkPaymentRef getPaymentRef() {

	if (paymentRef == null && paymentRefValue > 0) {
	    paymentRef = new CoinSparkPaymentRef(paymentRefValue);
	}

	return paymentRef;
    }

    /**
     * Set the CoinSpark payment reference instance variable and also set the
     * reference value (long) which is to be persisted to database.
     */
    public void setPaymentRef(CoinSparkPaymentRef paymentRef) {
	this.paymentRef = paymentRef;
	if (paymentRef != null) {
	    this.paymentRefValue = paymentRef.getRef();
	}
    }

    public boolean hasAesKey(String txid) {
	return (txidWalletPasswordMap.get(txid) != null);
    }
    
    public void setAesKey(KeyParameter AesKey) {
	aesKey = AesKey;
    }

    /**
     * ORMLite
     * all persisted classes must define a no-arg constructor with at least package visibility
     */
    CSMessage() {
    }

    /**
     * Create a new CSMessage manually
     * @param db 
     */
    public CSMessage(CSMessageDatabase db) {
	init(db);
	clear(); // not instantiated by ORMLite, could use isORMLite boolean.
    }

    /**
     * Manually initialize CSMessage when created by ORMLite
     */
    public void init(CSMessageDatabase db) {
	if (this.db==null) this.db = db;
	if (this.meta==null) this.meta = new CSMessageMetadata();
    }

    /**
     * Create a subset of the message metadata, populating a CSMessageParams bean
     * @return CSMessageParams
     */
    public CSMessageParams getMessageParams() {
	CSMessageParams mp = new CSMessageParams();
	try {
	    BeanUtils.copyProperties(mp, meta);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return mp;
    }

    /**
     * Set message parameters by copying them into the metadata bean
     * @param mp 
     */
    public void setMessageParams(CSMessage.CSMessageParams mp) {
	try {
	    BeanUtils.copyProperties(meta, mp);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    /**
     * Set the server URL (by copying straight into metadata)
     * @param ServerURL 
     */
    public void setServerURL(String ServerURL) {
	meta.setServerURL(ServerURL);
    }

    public void setTxID(String TxID) {
	txID = TxID;
    }

    /**
     * Insert message that we sent...
     * @param TxID
     * @param countOutputs
     * @param paymentRef
     * @param Message
     * @param MessageParts
     * @param MessageParams
     * @return 
     */
    protected boolean set(String TxID, int countOutputs, CoinSparkPaymentRef paymentRef, CoinSparkMessage Message, CoinSparkMessagePart[] MessageParts, CSMessage.CSMessageParams MessageParams) {
	txID = TxID;

	setPaymentRef(paymentRef);

	if (Message == null) {
	    messageState = CSMessageState.PAYMENTREF_ONLY;
	    return true;
	}

	setMessageParams(MessageParams);
	if (meta.isSent) {
	    messageState = CSMessageState.SELF;
	}
	
	// Set state and perform any required operations
	setState(messageState);

	// When sent to self, we don't retrive messagaes so we must store our data in DB.
	// Message parts are not persisted in DB unless the DAO is updated.
	if (!saveMessageParts(MessageParts)) {
	    return false;
	}

	return saveMetadata(TxID, countOutputs, Message);
    }

    /**
     * Set up a message that we received...
     * @param TxID
     * @param countOutputs
     * @param paymentRef
     * @param Message
     * @param Addresses
     * @return 
     */
    protected boolean set(String TxID, int countOutputs, CoinSparkPaymentRef paymentRef, CoinSparkMessage Message, String[] Addresses)
    {
	txID = TxID;
	meta.addresses = Addresses;

	setPaymentRef(paymentRef);

	if (Message == null) {
	    messageState = CSMessageState.PAYMENTREF_ONLY;
	    return true;
	}

	setState(CSMessageState.NEVERCHECKED);

	// Since never checked, no message parts have been saved yet.
	return saveMetadata(TxID, countOutputs, Message);
    }


    /**
     * Set state and perform associated operations.
     * @param State 
     */
    private void setState(int State) {
	messageState = State;
	switch (State) {
	    case CSMessageState.NEVERCHECKED:
		lastChecked = new Date();
		failures = 0;
		break;
	    case CSMessageState.NOT_FOUND:
	    case CSMessageState.PENDING:
	    case CSMessageState.EXPIRED:
	    case CSMessageState.INVALID:
	    case CSMessageState.HASH_MISMATCH:
	    case CSMessageState.SERVER_NOT_RESPONDING:
	    case CSMessageState.SERVER_ERROR:
	    case CSMessageState.ENCRYPTED_KEY:
		lastChecked = new Date();
		failures++;
		break;
	    case CSMessageState.SELF:
	    case CSMessageState.VALID:
	    case CSMessageState.PAYMENTREF_ONLY:
		lastChecked = new Date();
		failures = 0;
		break;
	    case CSMessageState.REFRESH:
	    case CSMessageState.DELETED:
		lastChecked = null;
		failures = 0;
		break;
	}
    }

    
    /**
     * Clear instance variables, reset state of message.
     * Default values of new CSMessageMetadata should be 0, null, etc.
     */
    private void clear() {
	lastChecked = null;
	failures = 0;
	messageState = CSMessageState.NEVERCHECKED;
	txID = "";
	meta = new CSMessageMetadata();
	meta.isSent = false;
    }

    /**
     * @return 
     */
    public String metadataToString() {
	
	// Update with load
	loadMetadata();
	
	StringBuilder sb = new StringBuilder();

	sb.append("TxID=").append(this.txID).append("\n");
	sb.append("HashLen=").append(meta.hashLen).append("\n");
	sb.append("Hash=").append(meta.hash).append("\n");
	sb.append("SentByThisWallet=").append(meta.isSent ? "1" : "0").append("\n");
	sb.append("Public=").append(meta.isPublic ? "1" : "0").append("\n");
	if (meta.salt != null) {
	    sb.append("Salt=").append(meta.salt).append("\n");
	}
	sb.append("KeepSeconds=").append(meta.keepseconds).append("\n");
	if (meta.sender != null) {
	    sb.append("Sender=").append(meta.sender).append("\n");
	}
	sb.append("Server=").append(meta.serverURL).append("\n");
	sb.append("Outputs=").append(meta.countOutputs).append("\n");
	sb.append("Encoded=").append(meta.encoded).append("\n");
	if (this.messageParts != null) {
	    sb.append("Parts=").append(numParts).append("\n");
	    for (CSMessagePart part : this.messageParts) {
		sb.append("Part=").append(part.partID).append("\n");
		sb.append("MimeType=").append(part.mimeType).append("\n");
		if (part.fileName != null) {
		    sb.append("FileName=").append(part.fileName).append("\n");
		}
		sb.append("Size=").append(part.contentSize).append("\n");
		sb.append("Content=").append("attachment " + part.partID).append("\n");
	    }
	}
	if (meta.addresses != null) {
	    sb.append("Addresses=").append(meta.addresses.length).append("\n");
	    int count = 0;
	    for (String address : meta.addresses) {
		sb.append("AddressID=").append(count).append("\n");
		sb.append("Address=").append(address).append("\n");
		count++;
	    }
	}
	if (meta.recipients != null) {
	    sb.append("Recipients=").append(meta.recipients.length).append("\n");
	    int count = 0;
	    for (String recipient : meta.recipients) {
		sb.append("RecipientID=").append(count).append("\n");
		sb.append("Recipient=").append(recipient).append("\n");
		count++;
	    }
	}
	sb.append("\n");
//	 if(message != null)
//	 {
//	 sb.append(message.toString()).append("\n");
//	 }
	return sb.toString();
    }

    
    /**
     * Store metadata about this message in a map
     * @param txid
     * @param countOutputs
     * @param message
     * @return 
     */
    private boolean saveMetadata(String txid, int countOutputs, CoinSparkMessage message) {
	// Map txid -> metadata
	MVMap<String, Object> map = this.db.getDefMap();
	if (map == null) {
	    return false;
	}

	if (message != null) {
	    meta.hashLen = message.getHashLen();
	    this.setHashBytes(message.getHash());
	    meta.serverURL = message.getFullURL();
	    meta.encoded = message.encodeToHex(countOutputs, 65536);

	    // When retrieving, message is null and countOutputs is zero, which overwrites
	    // previous countOutputs value obtained when message was inserted when received.
	    // We want to retain that figure.
	    meta.countOutputs = countOutputs;
	}

	try {
	    Map<String, String> m = BeanUtils.describe(meta);
	    map.put(txid, m);
	    log.debug(">>>> saveDef() inserted = " + Arrays.toString(m.entrySet().toArray()));

	    for (Map.Entry<String, String> entrySet : m.entrySet()) {
		String key = entrySet.getKey();
		String value = entrySet.getValue();
		log.debug(">>>> " + key + " : " + value);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
	return true;
    }

    /**
     * Load metadata about this message from map
     * @return 
     */
    private boolean loadMetadata() {
	MVMap<String, Object> map = this.db.getDefMap();
	if (map == null) {
	    return false;
	}

	Map<String, String> m = (Map) map.get(this.txID);
	if (m == null) {
	    return false;
	}

	try {
	    BeanUtils.populate(meta, m);

	    Map<String, String> m2 = BeanUtils.describe(meta);
	    log.debug(">>>> loaddef = " + Arrays.toString(m2.entrySet().toArray()));

//	    if (key.equals("hash")) {
//    meta.by=CSUtils.hex2Byte(meta.hash);
//		    
//		}
	    for (Map.Entry<String, String> entrySet : m.entrySet()) {
		String key = entrySet.getKey();
		String value = entrySet.getValue();
		log.debug(">>>> " + key + " : " + value);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    return false;
	}

	return true;
    }
    

    protected CSMessage load() {
	if (messageState == CSMessageState.PAYMENTREF_ONLY) {
	    return this;
	}
	if (!loadMetadata()) {
	    corrupted = true;
	}

	return this;
    }

    /**
     * Save retrieved message parts from servers to our database.
     * DAO will persist to database.
     *
     * @param MessageParts Have already passed messaging hash test so they are good.
     * @return
     */
    private boolean saveMessageParts(CoinSparkMessagePart[] MessageParts) {

	log.debug(">>>> saveMessageParts() invoked with []MessageParts of length " + MessageParts.length);

	if (MessageParts == null) {
	    numParts = 0;
	    return true;
	}

	numParts = MessageParts.length;

	List<CSMessagePart> parts = new ArrayList<CSMessagePart>();
	for (int i = 0; i < numParts; i++) {
	    CoinSparkMessagePart c = MessageParts[i];
	    log.debug(">>>> MESSAGE PART: " + c.mimeType + " , " + c.content.length + " , name=" + c.fileName);
	    CSMessagePart p = new CSMessagePart(i + 1, c.mimeType, c.fileName, c.content.length);
	    p.message = this; // set foreign reference
	    parts.add(p);
	    
	    // Insert BLOB into blob store.  It has already passed the hash test.
	    CSMessageDatabase.putIfAbsentBlobForMessagePart(txID, i+1, c.content);
	}
	
	try {
	    this.db.persistMessageParts(parts);
	} catch (Exception e) {
	    e.printStackTrace();
	    return false;
	}
	
	return true;
    }

    public long nextRetrievalInterval() {
	long interval = 0;
	long never = 864000000;

	if (lastChecked == null) {
	    return 0;
	}

	switch (messageRetrievalState) {
	    case CSMessageState.VALID:
	    case CSMessageState.SELF:
	    case CSMessageState.EXPIRED:
	    case CSMessageState.PAYMENTREF_ONLY:
		interval = never;
		break;
	    case CSMessageState.ENCRYPTED_KEY:
		if (aesKey == null) {
		    interval = never;
		}
		break;
	    case CSMessageState.NEVERCHECKED:
	    case CSMessageState.INVALID:
	    case CSMessageState.REFRESH:
		break;
	    default:
		if (failures < 60) {
		    interval = 0;
		} else {
		    if (failures < 120) {
			interval = 60;
		    } else {
			if (failures < 180) {
			    interval = 3600;
			} else {
			    interval = 86400;
			}
		    }
		}
		interval -= (new Date().getTime() - lastChecked.getTime()) / 1000;
		break;
	}

	if (interval < 0) {
	    interval = 0;
	}

	return interval;
    }

    private class JResponse {

	public CSUtils.CSServerError error = CSUtils.CSServerError.UNKNOWN;
	public String errorMessage = "";
	public JsonObject result = null;
	public JsonElement resultAsElement = null;
    }

    private JResponse jsonQuery(JRequest request) {
	JResponse response = new JResponse();
	try {
	    // Use Google GSON library for Java Object <--> JSON conversions
	    Gson gson = new Gson();

	    // convert java object to JSON format,
	    String json = gson.toJson(request);
	    response.error = CSUtils.CSServerError.NOERROR;
	    try {
		JsonElement jelement;
		JsonObject jobject = null;

		Map<String, String> headers = new HashMap<String, String>();
		headers.put("Content-Type", "application/json");

		CSUtils.CSDownloadedURL downloaded = CSUtils.postMessagingURL(meta.getServerURL(), 15, null, json, headers);
		// Record the urlString we are using for the actual connection
		actualServerURL = downloaded.urlString;
		
		String prefix = "MSG # " + this.hashCode() + " : ";
		log.debug(prefix + "----------------------BEGIN QUERY------------------------");
		log.debug(prefix + "JSON Query posted to " + meta.getServerURL() + " / actual " + actualServerURL);
		log.debug(prefix + "Headers = " + headers);
		String s1 = json.replaceAll("\\\"content\\\":\\\".*?\\\"", "\\\"content\\\":......");
		log.debug(prefix + "JSON Request = " + s1);
		log.debug(prefix + "HTTP Response Error = " + downloaded.error);
		log.debug(prefix + "HTTP Response Message = " + downloaded.ResponseMessage);
		String s2 = downloaded.contents.replaceAll("\\\"content\\\":\\\".*?\\\"", "\\\"content\\\":......");
		log.debug(prefix + "JSON Response = " + s2);
		log.debug(prefix + "---------------------END QUERY---------------------------");
		
		if (downloaded.error != null) {
		    response.errorMessage = downloaded.error;
		    response.error = CSUtils.CSServerError.SERVER_CANNOT_CONNECT;
		    if (downloaded.responseCode >= 300) {
			response.error = CSUtils.CSServerError.SERVER_REDIRECT;
		    }
		    if (downloaded.responseCode >= 400) {
			response.error = CSUtils.CSServerError.SERVER_HTTP_ERROR;
		    }
		    if (downloaded.responseCode >= 500) {
			response.error = CSUtils.CSServerError.SERVER_FATAL_ERROR;
		    }
		} else {
		    jelement = new JsonParser().parse(downloaded.contents);
		    jobject = jelement.getAsJsonObject();
		}

			
		// !!!
		if (CSMessageDatabase.debugWithCustomError && request.method.equals(CSMessageDatabase.debugCustomErrorMethod)) {
		    response.errorMessage = CSUtils.getHumanReadableServerError(CSMessageDatabase.debugCustomErrorCode);
		    response.error = CSUtils.CSServerError.fromCode(CSMessageDatabase.debugCustomErrorCode);
		    log.debug(prefix + "DEBUG CUSTOM ERROR SET: " + response.errorMessage + " , code=" + response.error);
		}
		// !!!
		
		
		if (response.error == CSUtils.CSServerError.NOERROR) {
		    if (jobject != null) {
			if ((jobject.get("id") == null) || jobject.get("id").getAsInt() != request.id) {
			    response.errorMessage = "id doesn't match " + request.id;
			    response.error = CSUtils.CSServerError.RESPONSE_WRONG_ID;
			} else {
			    if ((jobject.get("error") != null)) {
				if (jobject.get("error").isJsonObject()) {
				    JsonObject jerror = jobject.get("error").getAsJsonObject();
				    if (jerror.get("code") != null) {
					response.errorMessage = "Error code: " + jerror.get("code").getAsInt();
					response.error = CSUtils.CSServerError.fromCode(jerror.get("code").getAsInt());
				    }
				    if (jerror.get("message") != null) {
					response.errorMessage = jerror.get("message").getAsString();
				    }
				} else {
				    response.errorMessage = "Parse error";
				    response.error = CSUtils.CSServerError.RESPONSE_PARSE_ERROR;
				}
			    } else {
				if (jobject.get("result") != null) {
				    if (!jobject.get("result").isJsonObject()) {
					response.errorMessage = "Result object is not array";
					response.result = null;
					response.error = CSUtils.CSServerError.RESPONSE_RESULT_NOT_OBJECT;
					response.resultAsElement = jobject.get("result");
				    } else {
					response.result = jobject.getAsJsonObject("result");
				    }
				} else {
				    response.error = CSUtils.CSServerError.RESPONSE_RESULT_NOT_FOUND;
				}
			    }
			}
		    } else {
			response.error = CSUtils.CSServerError.RESPONSE_NOT_OBJECT;
		    }
		}
	    } catch (JsonSyntaxException ex) {
		response.errorMessage = "JSON syntax " + ex.getClass().getName() + " " + ex.getMessage();
		response.error = CSUtils.CSServerError.RESPONSE_PARSE_ERROR;
	    } catch (Exception ex) {
		response.errorMessage = "Exception " + ex.getClass().getName() + " " + ex.getMessage();
		response.error = CSUtils.CSServerError.INTERNAL_ERROR;
	    }
	} catch (Exception ex) {
	    response.errorMessage = "Exception " + ex.getClass().getName() + " " + ex.getMessage();
	    response.error = CSUtils.CSServerError.INTERNAL_ERROR;
	}

	return response;
    }

    private class JRequest {

	public int id;
	public String jsonrpc;
	public String method;
	public Object params;
	public int timeout;

	public JRequest(JRequestPreCreateParams Params) {
	    id = (int) (new Date().getTime() / 1000);
	    jsonrpc = "2.0";
	    method = "coinspark_message_pre_create";
	    params = Params;
	    timeout = 15;
	}

	public JRequest(JRequestCreateParams Params) {
	    id = (int) (new Date().getTime() / 1000);
	    jsonrpc = "2.0";
	    method = "coinspark_message_create";
	    params = Params;
	    timeout = 30;
	}

	public JRequest(JRequestPreRetrieveParams Params) {
	    id = (int) (new Date().getTime() / 1000);
	    jsonrpc = "2.0";
	    method = "coinspark_message_pre_retrieve";
	    params = Params;
	    timeout = 15;
	}

	public JRequest(JRequestRetrieveParams Params) {
	    id = (int) (new Date().getTime() / 1000);
	    jsonrpc = "2.0";
	    method = "coinspark_message_retrieve";
	    params = Params;
	    timeout = 30;
	}
    }

    @Deprecated
    private String getSignature_old(Wallet wallet, String address, CSNonce Nonce) {
	Address pubKeyHashAddress;
	try {
	    pubKeyHashAddress = new Address(wallet.getNetworkParameters(), address);
	} catch (AddressFormatException ex) {
	    Nonce.error = CSUtils.CSServerError.CANNOT_SIGN;
	    return null;
	}

	ECKey key = wallet.findKeyFromPubHash(pubKeyHashAddress.getHash160());
	if (key == null) {
	    Nonce.error = CSUtils.CSServerError.CANNOT_SIGN;
	    return null;
	}

	Sha256Hash hashForSignature = Sha256Hash.create(Nonce.nonce.getBytes());
	TransactionSignature signature = new TransactionSignature(key.sign(hashForSignature, aesKey), Transaction.SigHash.ALL, true);

	byte[] encodedSignature = signature.encodeToBitcoin();

	byte[] sigScript = new byte[encodedSignature.length + key.getPubKey().length + 2];

	sigScript[0] = (byte) encodedSignature.length;
	System.arraycopy(encodedSignature, 0, sigScript, 1, encodedSignature.length);
	sigScript[encodedSignature.length + 1] = (byte) key.getPubKey().length;
	System.arraycopy(key.getPubKey(), 0, sigScript, encodedSignature.length + 2, key.getPubKey().length);

	return Base64.encodeBase64String(sigScript);
    }
    
    /**
     * Return pub key bytes as hexadecimal string
     * @param wallet
     * @param address
     * @param Nonce
     * @return hexadecimal string
     */
    private String getPubKey(Wallet wallet, String address, CSNonce Nonce) {
	Address pubKeyHashAddress;
	try {
	    pubKeyHashAddress = new Address(wallet.getNetworkParameters(), address);
	} catch (AddressFormatException ex) {
	    Nonce.error = CSUtils.CSServerError.CANNOT_SIGN;
	    return null;
	}

	ECKey key = wallet.findKeyFromPubHash(pubKeyHashAddress.getHash160());
	if (key == null) {
	    Nonce.error = CSUtils.CSServerError.CANNOT_SIGN;
	    return null;
	}
    
	byte[] bytes = key.getPubKey();
	String s = Utils.bytesToHexString(bytes);
	return s;
    }
    
    
    private String getSignature(Wallet wallet, String address, CSNonce Nonce) {
	Address pubKeyHashAddress;
	try {
	    pubKeyHashAddress = new Address(wallet.getNetworkParameters(), address);
	} catch (AddressFormatException ex) {
	    Nonce.error = CSUtils.CSServerError.CANNOT_SIGN;
	    return null;
	}

	ECKey key = wallet.findKeyFromPubHash(pubKeyHashAddress.getHash160());
	if (key == null) {
	    Nonce.error = CSUtils.CSServerError.CANNOT_SIGN;
	    return null;
	}

	Sha256Hash hashForSignature = Sha256Hash.create(Nonce.nonce.getBytes());
	TransactionSignature signature = new TransactionSignature(key.sign(hashForSignature, aesKey), Transaction.SigHash.ALL, true);

	byte[] encodedSignature = signature.encodeToBitcoin();
	return Base64.encodeBase64String(encodedSignature);
    }

    private class JRequestPreCreateMessagePart {

	public boolean testnet = CSMessageDatabase.testnet3;

	public String mimetype;
	public String filename;
	public int bytes;

	JRequestPreCreateMessagePart(String MimeType, String FileName, int Bytes) {
	    mimetype = MimeType;
	    filename = FileName;
	    bytes = Bytes;
	}
    }

    private class JRequestPreCreateParams {

	public boolean testnet = CSMessageDatabase.testnet3;

	public String sender;
	public boolean ispublic;
	public String[] recipients;
	public int keepseconds;
	public String salt;
	public JRequestPreCreateMessagePart[] message;
    }

    public CSNonce getCreateNonce(CoinSparkMessagePart[] MessageParts) {
	CSNonce nonce = new CSNonce();

	JRequestPreCreateParams params = new JRequestPreCreateParams();

	params.sender = meta.sender;
	params.ispublic = meta.isPublic;
	params.salt = meta.salt;
	params.keepseconds = meta.keepseconds;
	params.recipients = meta.recipients;
	params.message = new JRequestPreCreateMessagePart[MessageParts.length];
	int count = 0;
	for (CoinSparkMessagePart part : MessageParts) {
	    params.message[count] = new JRequestPreCreateMessagePart(part.mimeType, part.fileName, part.content.length);
	    count++;
	}

	JResponse response = jsonQuery(new JRequest(params));

	nonce.error = response.error;
	if (nonce.error != CSUtils.CSServerError.NOERROR) {
	    nonce.error = response.error;
	    nonce.errorMessage = response.errorMessage;
	}

	if (nonce.error == CSUtils.CSServerError.NOERROR) {
	    if ((response.result.get("sender") == null)) {
		nonce.errorMessage = "Sender not found in pre_create query";
		nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
	    } else {
		if (!params.sender.equals(response.result.get("sender").getAsString())) {
		    nonce.errorMessage = "Sender in response doesn't match";
		    nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
		}
	    }
	}

	if (nonce.error == CSUtils.CSServerError.NOERROR) {
	    if ((response.result.get("nonce") == null)) {
		nonce.errorMessage = "Nonce not found in pre_retrieve query";
		nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
	    } else {
		nonce.nonce = response.result.get("nonce").getAsString();
	    }
	}

	if (nonce.error != CSUtils.CSServerError.NOERROR) {
	    log.error("Delivery: Code: " + nonce.error + ": " + nonce.errorMessage);
	}

	return nonce;
    }

    private class JRequestCreateMessagePart {

	public boolean testnet = CSMessageDatabase.testnet3;

	public String mimetype;
	public String filename;
	public String content;

	JRequestCreateMessagePart(String MimeType, String FileName, byte[] Content) {
	    mimetype = MimeType;
	    filename = FileName;
	    content = Base64.encodeBase64String(Content);
	}
    }

    private class JRequestCreateParams {

	public boolean testnet = CSMessageDatabase.testnet3;

	public String sender;
	public String nonce;
	public String signature;
	public String pubkey;
	public String txid;
	public boolean ispublic;
	public String[] recipients;
	public int keepseconds;
	public String salt;
	public JRequestCreateMessagePart[] message;
    }

    public boolean create(Wallet wallet, CoinSparkMessagePart[] MessageParts, CSNonce Nonce) throws CSExceptions.CannotEncode {
	if (Nonce.error != CSUtils.CSServerError.NOERROR) {
	    String s = Nonce.errorMessage;
	    s = s.replaceAll("^(\\w+\\.)+\\w+\\s", "");  // strip exception class name
	    throw new CSExceptions.CannotEncode(s + " (Error code " + Nonce.error.getCode() + ")");
//	    return false;
	}

	JRequestCreateParams params = new JRequestCreateParams();

	params.sender = meta.sender;
	params.txid = txID;
	params.nonce = Nonce.nonce;
	params.signature = getSignature(wallet, meta.sender, Nonce);
	params.pubkey = getPubKey(wallet, meta.sender, Nonce);
	params.sender = meta.sender;
	params.ispublic = meta.isPublic;
	params.salt = meta.salt;
	params.keepseconds = meta.keepseconds;
	params.recipients = meta.recipients;
	params.message = new JRequestCreateMessagePart[MessageParts.length];
	int count = 0;
	for (CoinSparkMessagePart part : MessageParts) {
	    params.message[count] = new JRequestCreateMessagePart(part.mimeType, part.fileName, part.content);
	    count++;
	}

	JResponse response = jsonQuery(new JRequest(params));

	Nonce.error = response.error;
	if (Nonce.error != CSUtils.CSServerError.NOERROR) {
	    Nonce.errorMessage = response.errorMessage;
	    String s = Nonce.errorMessage;
	    s = s.replaceAll("^(\\w+\\.)+\\w+\\s", "");  // strip exception class name
	    throw new CSExceptions.CannotEncode(s + " (Error code " + Nonce.error.getCode() + ")");
	    //return false;
	}

	if (Nonce.error == CSUtils.CSServerError.NOERROR) {
	    if ((response.result.get("txid") == null)) {
		Nonce.errorMessage = "TxID not found in create query";
		Nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
	    } else {
		if (!params.txid.equals(response.result.get("txid").getAsString())) {
		    Nonce.errorMessage = "TxID in response doesn't match";
		    Nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
		}
	    }
	}

	if (Nonce.error != CSUtils.CSServerError.NOERROR) {
	    String s = Nonce.errorMessage;
	    s = s.replaceAll("^(\\w+\\.)+\\w+\\s", "");  // strip exception class name
	    throw new CSExceptions.CannotEncode(s + " (Error code " + Nonce.error.getCode() + ")");
	}
	
	return (Nonce.error == CSUtils.CSServerError.NOERROR);
    }

    private class JRequestPreRetrieveParams {

	public boolean testnet = CSMessageDatabase.testnet3;

	public String txid;
	public String recipient;
    }

    private CSNonce getRetrieveNonce(String AddressToCheck) {
	CSNonce nonce = new CSNonce();

	JRequestPreRetrieveParams params = new JRequestPreRetrieveParams();

	params.txid = txID;
	params.recipient = AddressToCheck;

	JResponse response = jsonQuery(new JRequest(params));

	nonce.error = response.error;
	if (nonce.error != CSUtils.CSServerError.NOERROR) {
	    nonce.error = response.error;
	    nonce.errorMessage = response.errorMessage;
	    switch (nonce.error) {
//		case RECIPIENT_IP_IS_SUSPENDED:
//		    nonce.suspended = true;
//		    break;
		case RECIPIENT_IS_SUSPENDED:
		    nonce.suspended = true;
		    nonce.mayBeOtherAddressIsBetter = true;
		    break;
		case RECIPIENT_NOT_ACCEPTED:
		    nonce.mayBeOtherAddressIsBetter = true;
		    break;
	    }
	}

	if (nonce.error == CSUtils.CSServerError.NOERROR) {
	    if ((response.result.get("recipient") == null)) {
		nonce.errorMessage = "Recipient not found in pre_retrieve query";
		nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
	    } else {
		if (!AddressToCheck.equals(response.result.get("recipient").getAsString())) {
		    nonce.errorMessage = "Recipient in response doesn't match";
		    nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
		}
	    }
	}

	if (nonce.error == CSUtils.CSServerError.NOERROR) {
	    if ((response.result.get("nonce") == null)) {
		nonce.errorMessage = "Nonce not found in pre_retrieve query";
		nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
	    } else {
		nonce.nonce = response.result.get("nonce").getAsString();
	    }
	}

	if (nonce.error != CSUtils.CSServerError.NOERROR) {
	    log.error("Delivery: Code: " + nonce.error + ": " + nonce.errorMessage);
	}

	return nonce;
    }

    private class JRequestRetrieveParams {

	public boolean testnet = CSMessageDatabase.testnet3;

	public String txid;
	public String recipient;
	public String nonce;
	public String signature;
	public String pubkey;
    }

    /**
     * Try to retrieve message
     * @param wallet
     * @param acceptedAddress
     * @param Nonce
     * @return Tuple of (boolean update, error code, error msg)
     */
    private ImmutableTriple<Boolean, CSUtils.CSServerError, String> retrieve(Wallet wallet, String acceptedAddress, CSNonce Nonce) {
	if (Nonce.error != CSUtils.CSServerError.NOERROR) {
	    return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(false, Nonce.error, "Error retrieving message");
	}

	JRequestRetrieveParams params = new JRequestRetrieveParams();

	params.txid = txID;
	params.recipient = acceptedAddress;
	params.nonce = Nonce.nonce;
	params.signature = getSignature(wallet, acceptedAddress, Nonce);
	params.pubkey = getPubKey(wallet, acceptedAddress, Nonce);

	if (Nonce.error != CSUtils.CSServerError.NOERROR) {
	    return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(false, Nonce.error, "Error retrieving message");
	}

	JResponse response = jsonQuery(new JRequest(params));

	Nonce.error = response.error;
	if (Nonce.error != CSUtils.CSServerError.NOERROR) {
	    return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(false, Nonce.error, "Error retrieving message");
	}

	if (Nonce.error == CSUtils.CSServerError.NOERROR) {
	    if ((response.result.get("salt") == null)) {
		Nonce.errorMessage = "salt not found in retrieve query";
		Nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
	    } else {
		meta.setSalt(response.result.get("salt").getAsString());
//                messageParams.salt=response.result.get("salt").getAsString();
	    }
	}

	CoinSparkMessagePart[] receivedParts = null;
	if (Nonce.error == CSUtils.CSServerError.NOERROR) {
	    if ((response.result.get("message") == null)) {
		Nonce.errorMessage = "message not found in retrieve query";
		Nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
	    } else {
		if (!response.result.get("message").isJsonArray()) {
		    Nonce.errorMessage = "message is not json array";
		    Nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
		} else {
		    JsonArray jarray = response.result.getAsJsonArray("message");
		    numParts = jarray.size();
		    receivedParts = new CoinSparkMessagePart[numParts];
		    for (int j = 0; j < jarray.size(); j++) {
			JsonObject jentry = jarray.get(j).getAsJsonObject();
			CoinSparkMessagePart onePart = new CoinSparkMessagePart();
			if (jentry.get("mimetype") != null) {
			    onePart.mimeType = jentry.get("mimetype").getAsString();
			} else {
			    Nonce.errorMessage = "mimetype not found in one of message parts";
			    Nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
			}
			onePart.fileName = null;
			if (jentry.get("filename") != null) {
			    if (!jentry.get("filename").isJsonNull()) {
				onePart.fileName = jentry.get("filename").getAsString();
			    }
			}
			if (jentry.get("content") != null) {
			    String jsonData = jentry.get("content").getAsString();
			    if (jsonData!=null) {
				onePart.content = Base64.decodeBase64(jsonData);
			    }
			} else {
			    Nonce.errorMessage = "content not found in one of message parts";
			    Nonce.error = CSUtils.CSServerError.RESPONSE_INVALID;
			}
			if (Nonce.error == CSUtils.CSServerError.NOERROR) {
			    receivedParts[j] = onePart;
			}
		    }
		}
	    }
	}

	if (Nonce.error == CSUtils.CSServerError.NOERROR) {
	    byte[] receivedHash = CoinSparkMessage.calcMessageHash(Base64.decodeBase64(meta.getSalt()), receivedParts);
	    if (receivedHash==null || !Arrays.equals(Arrays.copyOf(this.getHashBytes(), meta.getHashLen()), Arrays.copyOf(receivedHash, meta.getHashLen()))) {
		Nonce.errorMessage = "message hash doesn't match encoded in metadata";
		Nonce.error = CSUtils.CSServerError.RESPONSE_HASH_MISMATCH;
	    }
	}

	// Message received, so save message parts, and save the metadata
	if (Nonce.error == CSUtils.CSServerError.NOERROR) {
	    if (!saveMessageParts(receivedParts)) {
		return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(true, CSUtils.CSServerError.NOERROR, null);
		// Internal error - no change in state
	    }

	    if (!saveMetadata(txID, 0, null)) // Internal error - no change in state
	    {
		return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(true, CSUtils.CSServerError.NOERROR, null);
	    }
	}

	if (Nonce.error != CSUtils.CSServerError.NOERROR) {
	    log.error("Delivery: Code: " + Nonce.error + ": " + Nonce.errorMessage);
	}

	switch (Nonce.error) {
	    case NOERROR:
		messageRetrievalState = CSMessageState.VALID;
		break;
//	    case RECIPIENT_IP_IS_SUSPENDED:
	    case RECIPIENT_IS_SUSPENDED:
		messageRetrievalState = CSMessageState.ADDRESSES_SUSPENDED;
		break;
//	    case RECIPIENT_IP_NOT_ACCEPTED:
	    case RECIPIENT_NOT_ACCEPTED:
		messageRetrievalState = CSMessageState.ADDRESSES_NOT_ACCEPTED;
		break;
	    case TX_MESSAGE_UNKNOWN:
		messageRetrievalState = CSMessageState.NOT_FOUND;
		break; //return true;
	    case TX_MESSAGE_PENDING:
		messageRetrievalState = CSMessageState.PENDING;
		break; //return true;
	    case TX_MESSAGE_EXPIRED:
		messageRetrievalState = CSMessageState.EXPIRED;
		break; //return true;
	    case NONCE_NOT_FOUND:                                               // Internal error - no change in state
	    case SIGNATURE_INCORRECT:
		break; //return true;
	    default:
		messageRetrievalState = CSMessageState.SERVER_ERROR;
		//return true;
	}


	if (Nonce.error != CSUtils.CSServerError.NOERROR) {
	    return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(true, Nonce.error, "Error retrieving message");
	}	
	
	return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(true, CSUtils.CSServerError.NOERROR, null);
	}

    
    private ImmutableTriple<Boolean, CSUtils.CSServerError, String> retrieve(Wallet wallet) {
	String acceptedAddress = null;
	CSNonce nonce = null;
	boolean suspended = false;

	// Null exception check.
	if (meta.addresses != null) {
	    for (String address : meta.addresses) {
		if (acceptedAddress == null) {
		    nonce = getRetrieveNonce(address);
		    suspended |= nonce.suspended;
		    if (!nonce.mayBeOtherAddressIsBetter) {
			acceptedAddress = address;
		    }
		}
	    }
	}

	if (acceptedAddress == null) {
	    for (ECKey key : wallet.getKeys()) {
		if (acceptedAddress == null) {
		    Address pubKeyHash = new Address(wallet.getNetworkParameters(), key.getPubKeyHash());
		    String address = pubKeyHash.toString();
		    boolean found = false;

		    if (meta.addresses != null) {
			for (String addressToCheck : meta.addresses) {
			    if (!found) {
				if (addressToCheck.equals(address)) {
				    found = true;
				}
			    }
			}
		    }

		    if (!found) {
			nonce = getRetrieveNonce(address);
			suspended |= nonce.suspended;
			if (!nonce.mayBeOtherAddressIsBetter) {
			    acceptedAddress = address;
			}
		    }
		}
	    }
	}

	if (acceptedAddress == null) {
	    if (suspended) {
		messageRetrievalState = CSMessageState.ADDRESSES_SUSPENDED;
		return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(true, CSUtils.CSServerError.RECIPIENT_IS_SUSPENDED, "Error retrieving message");
	    } else {
		messageRetrievalState = CSMessageState.ADDRESSES_NOT_ACCEPTED;
		return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(true, CSUtils.CSServerError.RECIPIENT_NOT_ACCEPTED, "Error retrieving message");
	    }
	}

	if (nonce == null) {
	    messageRetrievalState = CSMessageState.ADDRESSES_NOT_ACCEPTED;
	    return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(true, CSUtils.CSServerError.NONCE_NOT_FOUND, "Error retrieving message");
	}

	switch (nonce.error) {
	    case NOERROR:
		break;
	    case TX_MESSAGE_UNKNOWN:
		messageRetrievalState = CSMessageState.NOT_FOUND;
		break; //return true;
	    case TX_MESSAGE_PENDING:
		messageRetrievalState = CSMessageState.PENDING;
		break; //return true;
	    case TX_MESSAGE_EXPIRED:
		messageRetrievalState = CSMessageState.EXPIRED;
		break; //return true;
	    default:
		messageRetrievalState = CSMessageState.SERVER_ERROR;
		break; //return true;
	}
	
	if (nonce.error != CSUtils.CSServerError.NOERROR) {
	    return new ImmutableTriple<Boolean, CSUtils.CSServerError, String>(true, nonce.error, "Error retrieving message");
	}

	return retrieve(wallet, acceptedAddress, nonce);
    }

    protected boolean mayBeRetrieve(Wallet wallet) {
	boolean updateRequired = false;

	messageRetrievalState = messageState;

	// aeskey must be set, if required, before nextRetrievalInterval() is invoke
	// In future, perhaps use expiring map, by time or by count of usage
	setAesKey(txidWalletPasswordMap.get(txID));

	if (nextRetrievalInterval() == 0) {
	    try {
		this.isRetrieving = true;
		CSEventBus.INSTANCE.postAsyncEvent(CSEventType.MESSAGE_RETRIEVAL_STARTED, txID);
		load();
		ImmutableTriple<Boolean, CSUtils.CSServerError, String> triplet = retrieve(wallet);
		this.isRetrieving = false;
		this.db.putServerError(txID, triplet.getMiddle());
		updateRequired |= triplet.getLeft(); // replaced --> updateRequired |= retrieve(wallet);
	    } catch (KeyCrypterException e) {
		messageRetrievalState = CSMessageState.ENCRYPTED_KEY;
		this.isRetrieving = false;
	    }
	}

	updateRequired |= (messageState != messageRetrievalState);

	if (updateRequired) {
	    setState(messageRetrievalState);
	}
	
	// If set, clear the AESKey when done
	if (messageRetrievalState == CSMessageState.VALID) {
	    setAesKey(null);
	    txidWalletPasswordMap.remove(txID);
	}


	return updateRequired;
    }

}
