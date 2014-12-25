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
import com.google.bitcoin.core.Wallet;
import com.google.bitcoin.crypto.TransactionSignature;
import com.google.bitcoin.script.ScriptBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.coinspark.core.CSUtils;
import org.coinspark.protocol.CoinSparkMessage;
import org.coinspark.protocol.CoinSparkMessagePart;
import org.slf4j.LoggerFactory;
import org.spongycastle.crypto.params.KeyParameter;

/**
 *
 * @author mike
 */
public class CSMessage {
 
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(CSMessage.class);

    
    
    public enum CSMessageState{
        NEVERCHECKED,                                                           // Message is just created, not retrieved yet
        NOT_FOUND,                                                              // Message is not found on delivery server
        PENDING,                                                                // Message is found on delivery server, but its validity is not confirmed yet
        EXPIRED,                                                                // Message is expired and cannot be retrieved
        INVALID,                                                                // Invalid for some reason, not downloaded completely, for example
        HASH_MISMATCH,                                                          // Hash of retrieved message down't match encoded in metadata        
        REFRESH,                                                                // We should try to retrieve this message again
        VALID,                                                                  // Message is fully retrieved and hash is verified
        SELF,                                                                   // Message was creted by us
        SERVER_NOT_RESPONDING,                                                  // Message server not found
        SERVER_ERROR,                                                           // HTTPError on message server 
        ENCRYPTED_KEY,                                                          // The keys in this wallet is encrypted, message cannot be retrieved without aesKey
        ADDRESSES_NOT_ACCEPTED,                                                 // All addresses are not accepted
        ADDRESSES_SUSPENDED,                                                    // All addresses are either not accepted or suspended
        DELETED,                                                                // Message should be deleted from the database
    }

    public class CSMessagePart
    {    
        public String mimeType;
        public String fileName;
        public String contentFileName;                
        public int contentSize;

        CSMessagePart(String MimeType,String FileName,String ContentFileName,int ContentSize)
        {
            mimeType=MimeType;
            fileName=FileName;
            contentFileName=ContentFileName;
            contentSize=ContentSize;            
        }
    }
    
    public class CSNonce
    {    
        public String errorMessage;
        public String nonce;
        public CSUtils.CSServerError error;        
        public boolean mayBeOtherAddressIsBetter=false;
        public boolean suspended=false;
    }

    public class CSMessageParams
    {
        public String sender=null;
        public String seed=null;
        public boolean isPublic=false;
        public String [] recipients=null;
        public int keepseconds=0;
        public boolean isSent=false;
    }
    
    private int offsetInDB;
    private String txID;
    private byte[] hash;
    protected Date checked;
    private int failures;
    private int hashLen;
    protected CSMessage.CSMessageState messageState;
    protected CSMessage.CSMessageState messageRetrievalState;
    private int size;
    private int parts;
    private String encoded=null;
    private String serverURL=null;
    private String defFileName=null;
    private String dirName=null;        
    private boolean corrupted=false;
    private CSMessagePart [] messageParts=null;
    private KeyParameter aesKey;
    private String [] addresses; 
    private CSMessageParams messageParams=null;

    protected int getOffsetInDB(){return offsetInDB;}
            
    public String  getTxID(){return txID;}
    public Date    getValidChecked(){return checked;}
    public int     getValidFailures(){return failures;}
    public boolean getIsPublic(){return (messageParams == null) ? false : messageParams.isPublic;}
    public CSMessageState getState(){return messageState;}
    public boolean isCorrupted(){return corrupted;}
    public CSMessagePart [] getMessageParts(){return messageParts;}
    public String getServerURL(){return serverURL;}
    public CSMessageParams getMessageParams(){return messageParams;} 
    
    public void setAesKey(KeyParameter AesKey)
    {
        aesKey=AesKey;
    }
    
    protected static final int serializedSize=48;
    
    public CSMessage()
    {
        clear();
    }
    
    public void setMessageParams(CSMessage.CSMessageParams MessageParams)
    {
        messageParams=MessageParams;
    }

    public void setServerURL(String ServerURL)
    {
        serverURL=ServerURL;
    }
    
    public void setTxID(String TxID)
    {
        txID=TxID;
    }
    
    protected boolean set(String TxID,int countOutputs,CoinSparkMessage Message,CoinSparkMessagePart [] MessageParts,CSMessage.CSMessageParams MessageParams,int OffsetInDB,String DirName)
    {
        offsetInDB=OffsetInDB;            
        txID=TxID;        
        dirName=DirName + txID + File.separator;
        defFileName=dirName + "message.def";
        messageParams=MessageParams;
        if(messageParams.isSent)
        {
            messageState=CSMessageState.SELF;
        }
        setState(messageState);
        if(!saveMessageParts(MessageParts))
        {
            return false;
        }    
        
        return saveDef(TxID, countOutputs, Message);
    }
    
    protected boolean set(String TxID,int countOutputs,CoinSparkMessage Message,String [] Addresses,int OffsetInDB,String DirName)
    {
        offsetInDB=OffsetInDB;            
        txID=TxID;        
        dirName=DirName + txID + File.separator;
        defFileName=dirName + "message.def";
        addresses=Addresses;        
        
        setState(CSMessageState.NEVERCHECKED);
        return saveDef(TxID, countOutputs, Message);
    }
    
    protected boolean set(byte[] Serialized,int off,int OffsetInDB,String DirName)
    {
        offsetInDB=OffsetInDB;            
        txID=CSUtils.byte2Hex(Arrays.copyOfRange(Serialized, off, off+32));
        dirName=DirName + txID + File.separator;
        defFileName=dirName + "message.def";
        parts=CSUtils.littleEndianToInt(Serialized, off+32);
        size=CSUtils.littleEndianToInt(Serialized, off+36);
        checked=null;
        checked=new Date((long)CSUtils.littleEndianToInt(Serialized, off+40)*1000);
        failures=CSUtils.littleEndianToInt(Serialized, off+44) & 0xFFFFFF;

        switch(Serialized[off+47])
        {
            case 0: messageState= CSMessage.CSMessageState.NEVERCHECKED;break;
            case 1: messageState= CSMessage.CSMessageState.VALID;break;
            case 2: messageState= CSMessage.CSMessageState.NOT_FOUND;break;
            case 3: messageState= CSMessage.CSMessageState.PENDING;break;
            case 4: messageState= CSMessage.CSMessageState.EXPIRED;break;
            case 5: messageState= CSMessage.CSMessageState.INVALID;break;
            case 6: messageState= CSMessage.CSMessageState.HASH_MISMATCH;break;
            case 7: messageState= CSMessage.CSMessageState.REFRESH;break;
            case 8: messageState= CSMessage.CSMessageState.SELF;break;
            case 9: messageState= CSMessage.CSMessageState.SERVER_NOT_RESPONDING;break;
            case 10: messageState= CSMessage.CSMessageState.SERVER_ERROR;break;
            case 11: messageState= CSMessage.CSMessageState.ENCRYPTED_KEY;break;
            case 12: messageState= CSMessage.CSMessageState.ADDRESSES_NOT_ACCEPTED;break;
            case 13: messageState= CSMessage.CSMessageState.ADDRESSES_SUSPENDED;break;
            case 14: messageState= CSMessage.CSMessageState.DELETED;break;
                
        }
        
        return true;
    }
    
    private void setState(CSMessage.CSMessageState State)
    {
        messageState=State;            
        switch(State)
        {
            case NEVERCHECKED:
                checked=new Date();
                failures=0;
                break;
            case NOT_FOUND:
            case PENDING:
            case EXPIRED:
            case INVALID:
            case HASH_MISMATCH:
            case SERVER_NOT_RESPONDING:
            case SERVER_ERROR:
            case ENCRYPTED_KEY:
                checked=new Date();
                failures++;
                break;
            case SELF:
            case VALID:
                checked=new Date();
                failures=0;
                break;
            case REFRESH:
            case DELETED:
                checked=null;
                failures=0;
                break;
        }
    }
        
    public byte[] serialize()
    {
        int messageCode=0; 
        int ts=0;
        if(checked != null)
        {
            ts=(int)(checked.getTime()/1000);
        }

        switch(messageState){
            case NEVERCHECKED:   messageCode=0;break;
            case VALID:          messageCode=1;break;
            case NOT_FOUND:      messageCode=2;break;
            case PENDING:        messageCode=3;break;
            case EXPIRED:        messageCode=4;break;
            case INVALID:        messageCode=5;break;
            case HASH_MISMATCH:  messageCode=6;break;
            case REFRESH:        messageCode=7;break;
            case SELF:           messageCode=8;break;
            case SERVER_NOT_RESPONDING: messageCode=9;break;
            case SERVER_ERROR:   messageCode=10;break;
            case ENCRYPTED_KEY:  messageCode=11;break;
            case ADDRESSES_NOT_ACCEPTED:messageCode=12;break;
            case ADDRESSES_SUSPENDED:   messageCode=13;break;
            case DELETED:        messageCode=14;break;
        }            

        byte[] s=new byte[48];
        System.arraycopy(CSUtils.hex2Byte(txID), 0, s, 0, 32);
        CSUtils.littleEndianByteArray(parts, s, 32);
        CSUtils.littleEndianByteArray(size, s, 36);
        CSUtils.littleEndianByteArray(ts, s, 40);
        CSUtils.littleEndianByteArray(CSUtils.codedSize(failures,messageCode), s, 44);
        return s;
    }
        
    private void clear()
    {
        offsetInDB=0;
        checked=null;
        failures=0;
        messageState= CSMessage.CSMessageState.NEVERCHECKED;
        txID="";
        hash=new byte[32];
        size=0;
        hashLen=0;   
        messageParams=new CSMessageParams();
        messageParams.isSent=false;
    }
    
    private boolean saveDef(String TxID,int countOutputs,CoinSparkMessage Message)
    {
        if(dirName == null)
        {
            return false;
        }
        if(defFileName == null)
        {
            return false;
        }
        
        File theDir = new File(dirName);

        if (!theDir.exists()) 
        {
            try{
                theDir.mkdir();
             } catch(SecurityException ex){
                log.error("Message DB: Cannot create files directory" + ex.getClass().getName() + " " + ex.getMessage());                
                return false;
             }        
        }        
        
        File bdbFile = new File(defFileName + ".new");
                        
        if(bdbFile.exists())
        {
            if(!bdbFile.delete())
            {
                log.error("Message DB: Cannot deleted temporary file");                
                return false;
            }            
        }
             
        
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(defFileName + ".new", "rw");
        } catch (FileNotFoundException ex) {
            log.info("Message DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }

        if(Message != null)
        {
            hashLen=Message.getHashLen();
            hash=Message.getHash();
            serverURL=Message.getFullURL();
            encoded=Message.encodeToHex(countOutputs, 65536);
        }
        
        StringBuilder sb = new StringBuilder();

        sb.append("TxID=").append(txID).append("\n");
        sb.append("HashLen=").append(hashLen).append("\n");
        sb.append("Hash=").append(CSUtils.byte2Hex(hash)).append("\n");
        sb.append("SentByThisWallet=").append(messageParams.isSent ? "1" : "0").append("\n");
        sb.append("Public=").append(messageParams.isPublic ? "1" : "0").append("\n");
        if(messageParams.seed != null)sb.append("Seed=").append(messageParams.seed).append("\n");
        sb.append("KeepSeconds=").append(messageParams.keepseconds).append("\n");
        if(messageParams.sender != null)sb.append("Sender=").append(messageParams.sender).append("\n");
        sb.append("Server=").append(serverURL).append("\n");
        sb.append("Outputs=").append(countOutputs).append("\n");
        sb.append("Encoded=").append(encoded).append("\n");
        if(messageParts != null)
        {
            sb.append("Parts=").append(parts).append("\n");
            int count=0;
            for(CSMessagePart part : messageParts)
            {
                sb.append("Part=").append(count).append("\n");                
                sb.append("MimeType=").append(part.mimeType).append("\n");                
                if(part.fileName != null)
                {
                    sb.append("FileName=").append(part.fileName).append("\n");
                }                
                sb.append("Size=").append(part.contentSize).append("\n");                
                sb.append("Content=").append(part.contentFileName).append("\n");                                
                count++;
            }
        }
        if(addresses != null)
        {
            sb.append("Addresses=").append(addresses.length).append("\n");
            int count=0;
            for(String address : addresses)
            {
                sb.append("AddressID=").append(count).append("\n");                
                sb.append("Address=").append(address).append("\n");                
                count++;
            }            
        }
        if(messageParams.recipients != null)
        {
            sb.append("Recipients=").append(messageParams.recipients.length).append("\n");
            int count=0;
            for(String recipient : messageParams.recipients)
            {
                sb.append("RecipientID=").append(count).append("\n");                
                sb.append("Recipient=").append(recipient).append("\n");                
                count++;
            }            
        }
        sb.append("\n");
        if(Message != null)
        {
            sb.append(Message.toString()).append("\n");
        }
                
        try {
            aFile.writeBytes(sb.toString());
        } catch (IOException ex) {
            log.info("Message DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
        }
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Message DB: Cannot close def file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }        
        
        bdbFile = new File(defFileName + ".new");
                        
        if(bdbFile.exists())
        {
            try {
                Files.move( bdbFile.toPath(), new File(defFileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                log.error("Asset DB: Cannot rename temporary file: " + ex.getMessage());                
                return false;
            }
        }
        
        return true;
    }
    
    private boolean loadDef()
    {
        if(dirName == null)
        {
            return false;
        }
        if(defFileName == null)
        {
            return false;
        }
        
        RandomAccessFile aFile;
        
        try {
            aFile = new RandomAccessFile(defFileName, "r");
        } catch (FileNotFoundException ex) {
            log.info("Message DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        String line="Start";
        boolean result=true;
        int part=-1;
        int addrID=-1;
        int addrCount=0;
        
        while(!line.isEmpty())
        {
            int pos=line.indexOf("=");
            if(pos>0)
            {
                String prefix=line.substring(0, pos);
                String suffix=line.substring(pos+1);
                if("TxID".equals(prefix))
                {
                    if(!txID.equals(suffix))
                    {
                        result=false;
                    }
                }
                if("HashLen".equals(prefix))
                {
                    hashLen=Integer.parseInt(suffix);
                }
                if("Hash".equals(prefix))
                {
                    hash=CSUtils.hex2Byte(suffix);
                }
                if("SentByThisWallet".equals(prefix))
                {
                    messageParams.isSent=true;
                    if(Integer.parseInt(suffix) == 0)
                    {
                        messageParams.isSent=false;
                    }
                }                
                if("Public".equals(prefix))
                {
                    messageParams.isPublic=true;
                    if(Integer.parseInt(suffix) == 0)
                    {
                        messageParams.isPublic=false;
                    }
                }
                if("Sender".equals(prefix))
                {
                    messageParams.sender=suffix;
                }
                if("Seed".equals(prefix))
                {
                    messageParams.seed=suffix;
                }
                if("KeepSeconds".equals(prefix))
                {
                    messageParams.keepseconds=Integer.parseInt(suffix);
                }
                if("Server".equals(prefix))
                {
                    serverURL=suffix;
                }
                if("Encoded".equals(prefix))
                {
                    encoded=suffix;
                }
                if("Parts".equals(prefix))
                {
                    parts=Integer.parseInt(suffix);
                    if(parts<=0)
                    {
                        result=false;                        
                    }
                    else
                    {
                        messageParts=new CSMessagePart[parts];
                    }
                }
                if("Part".equals(prefix))
                {
                    part=Integer.parseInt(suffix);
                    if(part>=parts)
                    {
                        result=false;
                    }
                }
                if("MimeType".equals(prefix))
                {
                    if((messageParts != null) && (part>=0) && (parts<messageParts.length))
                    {
                        messageParts[part].mimeType=suffix;
                    }
                }
                if("FileName".equals(prefix))
                {
                    if((messageParts != null) && (part>=0) && (parts<messageParts.length))
                    {
                        messageParts[part].fileName=suffix;
                    }
                }
                if("Size".equals(prefix))
                {
                    if((messageParts != null) && (part>=0) && (parts<messageParts.length))
                    {
                        messageParts[part].contentSize=Integer.parseInt(suffix);
                    }
                }
                if("Content".equals(prefix))
                {
                    if((messageParts != null) && (part>=0) && (parts<messageParts.length))
                    {
                        messageParts[part].contentFileName=suffix;
                    }
                }                
                if("Addresses".equals(prefix))
                {
                    addrCount=Integer.parseInt(suffix);
                    if(addrCount<=0)
                    {
                        result=false;                        
                    }
                    else
                    {
                        addresses=new String[addrCount];
                    }
                }
                if("AddressID".equals(prefix))
                {
                    addrID=Integer.parseInt(suffix);
                    if(addrID>=addrCount)
                    {
                        result=false;
                    }
                }
                if("Address".equals(prefix))
                {
                    if((addresses != null) && (addrID>=0) && (addrID<addresses.length))
                    {
                        addresses[addrID]=suffix;
                    }
                }
                if("Recipients".equals(prefix))
                {
                    addrCount=Integer.parseInt(suffix);
                    if(addrCount<=0)
                    {
                        result=false;                        
                    }
                    else
                    {
                        messageParams.recipients=new String[addrCount];
                    }
                }
                if("RecipientID".equals(prefix))
                {
                    addrID=Integer.parseInt(suffix);
                    if(addrID>=addrCount)
                    {
                        result=false;
                    }
                }
                if("Recipient".equals(prefix))
                {
                    if((messageParams.recipients != null) && (addrID>=0) && (addrID<messageParams.recipients.length))
                    {
                        messageParams.recipients[addrID]=suffix;
                    }
                }
            }
            
            try {
                line=aFile.readLine();
            } catch (IOException ex) {
                Logger.getLogger(CSMessage.class.getName()).log(Level.SEVERE, null, ex);
            }
            if(line == null)
            {
                line="";
            }
        }
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Message DB: Cannot close def file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }        
        
        return result;
    }
    
    protected CSMessage load()
    {
        if(!loadDef())
        {
            corrupted=true;
        }
        
        return this; 
    }

    private boolean saveMessageParts(CoinSparkMessagePart [] MessageParts)
    {
        if(dirName == null)
        {
            return false;
        }
        
        File theDir = new File(dirName);

        if (!theDir.exists()) 
        {
            try{
                theDir.mkdir();
             } catch(SecurityException ex){
                log.error("Message DB: Cannot create files directory" + ex.getClass().getName() + " " + ex.getMessage());                
                return false;
             }        
        }        
        
        size=0;
        if(MessageParts == null)
        {
            parts=0;
            messageParts=null;
            return true;
        }
        
        messageParts=new CSMessagePart[MessageParts.length];
        parts=MessageParts.length;
        
        int count=0;
        boolean result=true;
        
        for(CoinSparkMessagePart part : MessageParts)
        {
            String contentFileName=part.fileName;
            int pos;
            while((contentFileName != null) && (pos=contentFileName.indexOf(File.separator))>=0)
            {
                if(pos+1<contentFileName.length())
                {
                    contentFileName=contentFileName.substring(pos+1);
                }
                else
                {
                    contentFileName=null;
                }
            }
            
            if((contentFileName != null) && contentFileName.trim().length() == 0)
            {
                contentFileName=null;
            }
            
            if(contentFileName == null)
            {
                contentFileName=String.format("messagepart%06d", count+1);
                CSUtils.CSMimeType mimeType=CSUtils.CSMimeType.fromType(part.mimeType);
                if(mimeType != null)
                {
                    contentFileName+=mimeType.getExtension();
                }
            }
            
            contentFileName=dirName + contentFileName;
            
            RandomAccessFile aFile;
            try {
                aFile = new RandomAccessFile(contentFileName, "rw");
            } catch (FileNotFoundException ex) {
                log.info("Message DB: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
                return false;
            }

            size+=part.content.length;
            
            try {
                aFile.write(part.content);
            } catch (IOException ex) {
                log.info("Message DB: Cannot write to file " + ex.getClass().getName() + " " + ex.getMessage());                
                result=false;
            }

            try {
                aFile.close();
            } catch (IOException ex) {
                log.error("Message DB: Cannot close file " + ex.getClass().getName() + " " + ex.getMessage());                
                return false;
            }        
            
            if(!result)
            {
                return false;
            }
            
            messageParts[count]=new CSMessagePart(part.mimeType, part.fileName, contentFileName, part.content.length);
            
            count++;
        }
        
        return true;
    }
    
    
    public long nextRetrievalInterval()
    {
        long interval=0;
        long never=864000000;

        if(checked == null)
        {
            return 0;
        }
        
        switch(messageRetrievalState)
        {
            case VALID:
            case SELF:
            case EXPIRED:
                interval=never;
                break;
            case ENCRYPTED_KEY:
                if(aesKey == null)
                {
                    interval=never;                    
                }
                break;
            case NEVERCHECKED:
            case INVALID:
            case REFRESH:
                break;
            default:
                if(failures<60)
                {
                    interval=0;
                }
                else
                {
                    if(failures<120)
                    {
                        interval=60;
                    }
                    else
                    {
                        if(failures<180)
                        {
                            interval=3600;
                        }
                        else
                        {
                            interval=86400;
                        }
                    }
                }
                interval-=(new Date().getTime()-checked.getTime())/1000;
                break;
        }
        
        if(interval<0)
        {
            interval=0;
        }
        
        return interval;
    }

    private class JResponse
    {
        public CSUtils.CSServerError error=CSUtils.CSServerError.UNKNOWN;
        public String errorMessage="";
        public JsonObject result=null;
        public JsonElement resultAsElement=null;
    }    
    
    private JResponse jsonQuery(JRequest request)
    {
        JResponse response=new JResponse();
        try
        {
            // Use Google GSON library for Java Object <--> JSON conversions
            Gson gson = new Gson();

            // convert java object to JSON format,
            String json = gson.toJson(request);
            response.error=CSUtils.CSServerError.NOERROR;
            try
            {
                JsonElement jelement;
                JsonObject jobject = null;                        

                Map <String,String> headers=new HashMap<String, String>();
                headers.put("Content-Type", "application/json");

                CSUtils.CSDownloadedURL downloaded=CSUtils.postURL(serverURL, 15, null, json, headers);
                if(downloaded.error != null)
                {
                    response.errorMessage=downloaded.error;
                    response.error=CSUtils.CSServerError.SERVER_CANNOT_CONNECT;
                    if(downloaded.responseCode>=300)
                    {
                        response.error=CSUtils.CSServerError.SERVER_REDIRECT;
                    }
                    if(downloaded.responseCode>=400)
                    {
                        response.error=CSUtils.CSServerError.SERVER_HTTP_ERROR;
                    }
                    if(downloaded.responseCode>=500)
                    {
                        response.error=CSUtils.CSServerError.SERVER_FATAL_ERROR;
                    }
                }
                else
                {
                    jelement = new JsonParser().parse(downloaded.contents);
                    jobject = jelement.getAsJsonObject();                                                    
                }

                if(response.error == CSUtils.CSServerError.NOERROR)
                {
                    if(jobject != null)
                    {
                        if((jobject.get("id") == null) || jobject.get("id").getAsInt() != request.id)
                        {
                            response.errorMessage="id doesn't match " + request.id;
                            response.error=CSUtils.CSServerError.RESPONSE_WRONG_ID;
                        }
                        else
                        {
                            if((jobject.get("error") != null))
                            {
                                if(jobject.get("error").isJsonObject())
                                {
                                    JsonObject jerror=jobject.get("error").getAsJsonObject();
                                    if(jerror.get("code") != null)
                                    {
                                        response.errorMessage="Error code: "+jerror.get("code").getAsInt();
                                        response.error=CSUtils.CSServerError.fromCode(jerror.get("code").getAsInt());
                                    }
                                    if(jerror.get("message") != null)
                                    {
                                        response.errorMessage=jerror.get("message").getAsString();
                                    }                                
                                }
                                else
                                {
                                    response.errorMessage="Parse error";     
                                    response.error=CSUtils.CSServerError.RESPONSE_PARSE_ERROR;
                                }
                            }
                            else
                            {
                                if(jobject.get("result") != null)
                                {
                                    if(!jobject.get("result").isJsonObject())
                                    {
                                        response.errorMessage="Result object is not array";     
                                        response.result=null;
                                        response.error=CSUtils.CSServerError.RESPONSE_RESULT_NOT_OBJECT;
                                        response.resultAsElement=jobject.get("result");
                                    }
                                    else
                                    {
                                        response.result = jobject.getAsJsonObject("result");                                                            
                                    }
                                }
                                else                                
                                {
                                    response.error=CSUtils.CSServerError.RESPONSE_RESULT_NOT_FOUND;
                                }
                            }
                        }
                    }
                    else
                    {
                        response.error=CSUtils.CSServerError.RESPONSE_NOT_OBJECT;
                    }
                }
            }
            catch(JsonSyntaxException ex)
            {
                response.errorMessage="JSON syntax " + ex.getClass().getName() + " " + ex.getMessage();     
                response.error=CSUtils.CSServerError.RESPONSE_PARSE_ERROR;
            }
            catch(Exception ex)
            {
                response.errorMessage="Exception " + ex.getClass().getName() + " " + ex.getMessage();     
                response.error=CSUtils.CSServerError.INTERNAL_ERROR;
            }
        }

        catch (Exception ex)
        {
            response.errorMessage="Exception " + ex.getClass().getName() + " " + ex.getMessage();     
            response.error=CSUtils.CSServerError.INTERNAL_ERROR;
        }
        
        return response;
    }
    
    private class JRequest
    {
        public int id;
        public String jsonrpc;
        public String method;
        public Object params;
        public int timeout;
        
        public JRequest(JRequestPreCreateParams Params)
        {
            id = (int)(new Date().getTime()/1000);
            jsonrpc = "2.0";
            method="coinspark_message_pre_create";
            params=Params;
            timeout=15;
        }
        public JRequest(JRequestCreateParams Params)
        {
            id = (int)(new Date().getTime()/1000);
            jsonrpc = "2.0";
            method="coinspark_message_create";
            params=Params;
            timeout=30;
        }
        public JRequest(JRequestPreRetrieveParams Params)
        {
            id = (int)(new Date().getTime()/1000);
            jsonrpc = "2.0";
            method="coinspark_message_pre_retrieve";
            params=Params;
            timeout=15;
        }
        public JRequest(JRequestRetrieveParams Params)
        {
            id = (int)(new Date().getTime()/1000);
            jsonrpc = "2.0";
            method="coinspark_message_retrieve";
            params=Params;
            timeout=30;
        }
    }
    
    
    private String getSignature(Wallet wallet,String address,CSNonce Nonce) 
    {
        Address pubKeyHashAddress;
        try {
            pubKeyHashAddress=new Address(wallet.getNetworkParameters(), address);
        } catch (AddressFormatException ex) {
            Nonce.error=CSUtils.CSServerError.CANNOT_SIGN;
            return  null;
        }
        
        ECKey key=wallet.findKeyFromPubHash(pubKeyHashAddress.getHash160());
        if(key == null)
        {
            Nonce.error=CSUtils.CSServerError.CANNOT_SIGN;
            return  null;            
        }
        
        Sha256Hash hashForSignature=Sha256Hash.create(Nonce.nonce.getBytes());
        TransactionSignature signature=new TransactionSignature(key.sign(hashForSignature, aesKey), Transaction.SigHash.ALL, true);
        
        byte [] encodedSignature=signature.encodeToBitcoin();
        
        byte [] sigScript=new byte[encodedSignature.length+key.getPubKey().length+2];
        
        sigScript[0]=(byte)encodedSignature.length;
        System.arraycopy(encodedSignature, 0, sigScript, 1, encodedSignature.length);
        sigScript[encodedSignature.length+1]=(byte)key.getPubKey().length;
        System.arraycopy(key.getPubKey(), 0, sigScript, encodedSignature.length+2, key.getPubKey().length);
        
        
        return Base64.encode(sigScript);
    }
    
    private class JRequestPreCreateMessagePart
    {
        public String mimetype;
        public String filename;
        public int bytes;
        
        JRequestPreCreateMessagePart(String MimeType,String FileName,int Bytes)
        {
            mimetype=MimeType;
            filename=FileName;
            bytes=Bytes;
        }
    }
    
    private class JRequestPreCreateParams
    {
        public String sender;
        public boolean ispublic;
        public String [] recipients;
        public int keepseconds;
        public String seed;
        public JRequestPreCreateMessagePart [] message;
    }

    public CSNonce getCreateNonce(CoinSparkMessagePart [] MessageParts)
    {
        CSNonce nonce=new CSNonce();
                
        JRequestPreCreateParams params=new JRequestPreCreateParams();
        
        params.sender=messageParams.sender;
        params.ispublic=messageParams.isPublic;
        params.seed=messageParams.seed;
        params.keepseconds=messageParams.keepseconds;
        params.recipients=messageParams.recipients;
        params.message=new JRequestPreCreateMessagePart[MessageParts.length];
        int count=0;
        for(CoinSparkMessagePart part : MessageParts)
        {
            params.message[count]=new JRequestPreCreateMessagePart(part.mimeType,part.fileName,part.content.length);
            count++;
        }
        
        JResponse response=jsonQuery(new JRequest(params));
        
        nonce.error=response.error;
        if(nonce.error != CSUtils.CSServerError.NOERROR)
        {
            nonce.error=response.error;
            nonce.errorMessage=response.errorMessage;
        }
        
        if(nonce.error == CSUtils.CSServerError.NOERROR)
        {
            if((response.result.get("sender") == null))
            {
                nonce.errorMessage="Sender not found in pre_create query";
                nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;
            }
            else
            {   
                if(!params.sender.equals(response.result.get("sender").getAsString()))
                {
                    nonce.errorMessage="Sender in response doesn't match";
                    nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;                   
                }
            }            
        }
        
        if(nonce.error == CSUtils.CSServerError.NOERROR)
        {
            if((response.result.get("nonce") == null))
            {
                nonce.errorMessage="Nonce not found in pre_retrieve query";
                nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;
            }
            else
            {   
                nonce.nonce=response.result.get("nonce").getAsString();
            }            
        }
        
        if(nonce.error != CSUtils.CSServerError.NOERROR)
        {
            log.error("Delivery: Code: " + nonce.error + ": " + nonce.errorMessage);
        }
        
        return nonce;
    }
    
    private class JRequestCreateMessagePart
    {
        public String mimetype;
        public String filename;
        public String content;
        JRequestCreateMessagePart(String MimeType,String FileName,byte[] Content)
        {
            mimetype=MimeType;
            filename=FileName;
            content=Base64.encode(Content);
        }
    }
    
    private class JRequestCreateParams
    {
        public String sender;
        public String nonce;
        public String signature;
        public String txid;
        public boolean ispublic;
        public String [] recipients;
        public int keepseconds;
        public String seed;
        public JRequestCreateMessagePart [] message;
    }

    public boolean create(Wallet wallet,CoinSparkMessagePart [] MessageParts,CSNonce Nonce)
    {
        if(Nonce.error != CSUtils.CSServerError.NOERROR)
        {
            return false;
        }
        
        JRequestCreateParams params=new JRequestCreateParams();
        
        params.sender=messageParams.sender;
        params.txid=txID;
        params.nonce=Nonce.nonce;
        params.signature=getSignature(wallet, messageParams.sender, Nonce);
        params.sender=messageParams.sender;
        params.ispublic=messageParams.isPublic;
        params.seed=messageParams.seed;
        params.keepseconds=messageParams.keepseconds;
        params.recipients=messageParams.recipients;
        params.message=new JRequestCreateMessagePart[MessageParts.length];
        int count=0;
        for(CoinSparkMessagePart part : MessageParts)
        {
            params.message[count]=new JRequestCreateMessagePart(part.mimeType,part.fileName,part.content);
            count++;
        }
        
        JResponse response=jsonQuery(new JRequest(params));

        Nonce.error=response.error;
        if(Nonce.error != CSUtils.CSServerError.NOERROR)
        {
            Nonce.errorMessage=response.errorMessage;
            return false;
        }
        
        if(Nonce.error == CSUtils.CSServerError.NOERROR)
        {
            if((response.result.get("txid") == null))
            {
                Nonce.errorMessage="TxID not found in create query";
                Nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;
            }
            else
            {   
                if(!params.txid.equals(response.result.get("txid").getAsString()))
                {
                    Nonce.errorMessage="TxID in response doesn't match";
                    Nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;                   
                }
            }            
        }        
        
        return (Nonce.error == CSUtils.CSServerError.NOERROR);
    }
    
    
    private class JRequestPreRetrieveParams
    {
        public String txid;
        public String recipient;
    }

    private CSNonce getRetrieveNonce(String AddressToCheck)
    {
        CSNonce nonce=new CSNonce();
                
        JRequestPreRetrieveParams params=new JRequestPreRetrieveParams();
        
        params.txid=txID;
        params.recipient=AddressToCheck;
        
        JResponse response=jsonQuery(new JRequest(params));
        
        nonce.error=response.error;
        if(nonce.error != CSUtils.CSServerError.NOERROR)
        {
            nonce.error=response.error;
            nonce.errorMessage=response.errorMessage;
            switch(nonce.error)
            {
                case RECIPIENT_IP_IS_SUSPENDED:
                    nonce.suspended=true;
                    break;
                case RECIPIENT_IS_SUSPENDED:
                    nonce.suspended=true;
                    nonce.mayBeOtherAddressIsBetter=true;
                    break;
                case RECIPIENT_NOT_ACCEPTED:
                    nonce.mayBeOtherAddressIsBetter=true;
                    break;
            }
        }
        
        if(nonce.error == CSUtils.CSServerError.NOERROR)
        {
            if((response.result.get("recipient") == null))
            {
                nonce.errorMessage= "Recipient not found in pre_retrieve query";
                nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;
            }
            else
            {   
                if(!AddressToCheck.equals(response.result.get("recipient").getAsString()))
                {
                    nonce.errorMessage="Recipient in response doesn't match";
                    nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;                   
                }
            }            
        }
        
        if(nonce.error == CSUtils.CSServerError.NOERROR)
        {
            if((response.result.get("nonce") == null))
            {
                nonce.errorMessage="Nonce not found in pre_retrieve query";
                nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;
            }
            else
            {   
                nonce.nonce=response.result.get("nonce").getAsString();
            }            
        }
        
        if(nonce.error != CSUtils.CSServerError.NOERROR)
        {
            log.error("Delivery: Code: " + nonce.error + ": " + nonce.errorMessage);
        }
        
        return nonce;
    }

    private class JRequestRetrieveParams
    {
        public String txid;
        public String recipient;
        public String nonce;
        public String signature;
    }
    
    private boolean retrieve(Wallet wallet,String acceptedAddress,CSNonce Nonce)
    {
        if(Nonce.error != CSUtils.CSServerError.NOERROR)
        {
            return false;
        }
        
        JRequestRetrieveParams params=new JRequestRetrieveParams();
        
        params.txid=txID;
        params.recipient=acceptedAddress;
        params.nonce=Nonce.nonce;
        params.signature=getSignature(wallet, acceptedAddress, Nonce);
        
        if(Nonce.error != CSUtils.CSServerError.NOERROR)
        {
            return false;
        }        
        
        JResponse response=jsonQuery(new JRequest(params));
        
        Nonce.error=response.error;
        if(Nonce.error != CSUtils.CSServerError.NOERROR)
        {
            return false;
        }
        
        if(Nonce.error == CSUtils.CSServerError.NOERROR)
        {
            if((response.result.get("seed") == null))
            {
                Nonce.errorMessage="seed not found in retrieve query";
                Nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;
            }
            else
            {   
                messageParams.seed=response.result.get("seed").getAsString();
            }            
        }
        
        CoinSparkMessagePart [] receivedParts=null;
        if(Nonce.error == CSUtils.CSServerError.NOERROR)
        {
            if((response.result.get("message") == null))
            {
                Nonce.errorMessage="message not found in retrieve query";
                Nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;
            }
            else
            {   
                if(!response.result.get("message").isJsonArray())
                {
                    Nonce.errorMessage="message is not json array";
                    Nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;                   
                }
                else
                {
                    JsonArray jarray = response.result.getAsJsonArray("message");   
                    parts=jarray.size();
                    receivedParts=new CoinSparkMessagePart[parts];
                    for (int j = 0; j < jarray.size(); j++)
                    {
                        JsonObject jentry = jarray.get(j).getAsJsonObject();
                        CoinSparkMessagePart onePart=new CoinSparkMessagePart();
                        if(jentry.get("mimetype") != null)
                        {
                            onePart.mimeType=jentry.get("mimetype").getAsString();
                        }
                        else
                        {
                            Nonce.errorMessage="mimetype not found in one of message parts";
                            Nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;                   
                        }                            
                        onePart.fileName=null;
                        if(jentry.get("filename") != null)
                        {
                            if(!jentry.get("filename").isJsonNull())
                            {
                                onePart.fileName=jentry.get("filename").getAsString();
                            }
                        }                        
                        if(jentry.get("content") != null)
                        {
                            onePart.content=Base64.decode(jentry.get("content").getAsString());
                        }
                        else
                        {
                            Nonce.errorMessage="content not found in one of message parts";
                            Nonce.error=CSUtils.CSServerError.RESPONSE_INVALID;                   
                        }                            
                        if(Nonce.error == CSUtils.CSServerError.NOERROR)
                        {
                            receivedParts[j]=onePart;
                        }
                    }                    
                }
            }            
        }
        
        if(Nonce.error == CSUtils.CSServerError.NOERROR)
        {
            byte [] receivedHash = CoinSparkMessage.calcMessageHash(Base64.decode(messageParams.seed), receivedParts);
            
            if(!Arrays.equals(Arrays.copyOf(hash, hashLen), Arrays.copyOf(receivedHash, hashLen)))
            {
                Nonce.errorMessage="message hash doesn't match encoded in metadata";
                Nonce.error=CSUtils.CSServerError.RESPONSE_HASH_MISMATCH;                                   
            }
        }   
        
        if(Nonce.error == CSUtils.CSServerError.NOERROR)                        
        {
            if(!saveMessageParts(receivedParts))
            {
                return true;                                                    // Internal error - no change in state
            }
            if(!saveDef(txID, offsetInDB, null))                                // Internal error - no change in state
            {
                return true;
            }
        }
        
        if(Nonce.error != CSUtils.CSServerError.NOERROR)
        {
            log.error("Delivery: Code: " + Nonce.error + ": " + Nonce.errorMessage);
        }
        
        switch(Nonce.error)
        {            
            case NOERROR:
                messageRetrievalState=CSMessageState.VALID;
                break;
            case RECIPIENT_IP_IS_SUSPENDED:
            case RECIPIENT_IS_SUSPENDED:
                messageRetrievalState=CSMessageState.ADDRESSES_SUSPENDED;
                break;
            case RECIPIENT_IP_NOT_ACCEPTED:
            case RECIPIENT_NOT_ACCEPTED:                
                messageRetrievalState=CSMessageState.ADDRESSES_NOT_ACCEPTED;
                break;
            case TX_MESSAGE_UNKNOWN:
                messageRetrievalState=CSMessageState.NOT_FOUND;
                return true;
            case TX_MESSAGE_PENDING:
                messageRetrievalState=CSMessageState.PENDING;
                return true;
            case TX_MESSAGE_EXPIRED:
                messageRetrievalState=CSMessageState.EXPIRED;
                return true;
            case NONCE_NOT_FOUND:                                               // Internal error - no change in state
            case SIGNATURE_INCORRECT:
                return true;
            default:
                messageRetrievalState=CSMessageState.SERVER_ERROR;
                return true;
        }
        
        return true;
    }
    
    private boolean retrieve(Wallet wallet)
    {
        String acceptedAddress=null;
        CSNonce nonce=null;
        boolean suspended=false;
        
        for(String address : addresses)
        {
            if(acceptedAddress == null)
            {
                nonce=getRetrieveNonce(address);
                suspended|=nonce.suspended;
                if(!nonce.mayBeOtherAddressIsBetter)
                {
                    acceptedAddress=address;
                }
            }
        }
        
        if(acceptedAddress == null)
        {
            for(ECKey key : wallet.getKeys())
            {
                if(acceptedAddress == null)
                {
                    Address pubKeyHash=new Address(wallet.getNetworkParameters(), key.getPubKeyHash());
                    String address=pubKeyHash.toString();
                    boolean found=false;
                    for(String addressToCheck : addresses)
                    {
                        if(!found)
                        {
                            if(addressToCheck.equals(address))
                            {
                                found=true;
                            }
                        }
                    }
                    if(!found)
                    {
                        nonce=getRetrieveNonce(address);
                        suspended |= nonce.suspended;
                        if(!nonce.mayBeOtherAddressIsBetter)
                        {
                            acceptedAddress=address;
                        }                       
                    }
                }
            }
        }
        
        if(acceptedAddress == null)
        {
            if(suspended)
            {
                messageRetrievalState=CSMessageState.ADDRESSES_SUSPENDED;
            }
            else
            {
                messageRetrievalState=CSMessageState.ADDRESSES_NOT_ACCEPTED;
            }
            return true;
        }
        
        if(nonce==null)
        {
            messageRetrievalState=CSMessageState.ADDRESSES_NOT_ACCEPTED;
            return true;
        }
        
        switch(nonce.error)
        {
            case NOERROR:
                break;
            case TX_MESSAGE_UNKNOWN:
                messageRetrievalState=CSMessageState.NOT_FOUND;
                return true;
            case TX_MESSAGE_PENDING:
                messageRetrievalState=CSMessageState.PENDING;
                return true;
            case TX_MESSAGE_EXPIRED:
                messageRetrievalState=CSMessageState.EXPIRED;
                return true;
            default:
                messageRetrievalState=CSMessageState.SERVER_ERROR;
                return true;
        }
        
        return retrieve(wallet, acceptedAddress,nonce);
    }
    
    protected boolean mayBeRetrieve(Wallet wallet)
    {        
        boolean updateRequired=false;
 
        messageRetrievalState=messageState;
               
        if(nextRetrievalInterval() == 0)
        {
            CSEventBus.INSTANCE.postAsyncEvent(CSEventType.MESSAGE_RETRIEVAL_STARTED, txID);
            load();
            updateRequired |= retrieve(wallet);
        }
        
        updateRequired |= (messageState != messageRetrievalState);
                
        if(updateRequired)
        {    
            setState(messageRetrievalState);
        }
                
        return updateRequired;        
    }
    
}
