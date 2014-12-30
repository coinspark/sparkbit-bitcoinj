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

import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.PeerGroup;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.TransactionInput;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.coinspark.core.CSPDFParser;
import org.coinspark.core.CSUtils;
import org.coinspark.protocol.CoinSparkAssetRef;
import org.coinspark.protocol.CoinSparkGenesis;
import org.slf4j.LoggerFactory;

/**
 * Coinspark Asset class
 */

public class CSAsset {
    
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(CSAsset.class);

    /**
     * Validity state of the asset.
     */
    
    public enum CSAssetState{
        INVALID,                                                                // Invalid for some reason
        NO_KEY,                                                                 // Neither AssetRef, nor genesis is set
        ASSET_REF_ONLY,                                                         // Only Asset ref is set, but not validated yet. No genesis
        BLOCK_NOT_FOUND,                                                        // Block not found
        TX_NOT_FOUND,                                                           // Genesis transaction not found by AssetRef
        GENESIS_NOT_FOUND,                                                      // Genesis not found in AssetRef transaction
        NOT_VALIDATED_YET,                                                      // Genesis not validated yet
        ASSET_WEB_PAGE_NOT_FOUND,                                               // Asset web page not found        
        ASSET_SPECS_NOT_FOUND,                                                  // Asset JSON specs not found on Web page
        ASSET_SPECS_NOT_PARSED,                                                 // Asset JSON specs cannot be parsed
        REQUIRED_FIELD_MISSING,                                                 // Some required fields are missing in JSON specs
        CONTRACT_NOT_FOUND,                                                     // Contract cannot be cached
        CONTRACT_INVALID,                                                       // Contract file is invalid
        HASH_MISMATCH,                                                          // Hash of retrieved fields down't match encoded in genesis
        REFRESH,                                                                // Validity should be checked (manually set), if genesis is set - starts from Asset web page
        VALID,                                                                  // Asset passed validity check
        DUPLICATE,                                                              // There is Asset with the same keys, this one should be deleted after balances db updated
    }
    
    public enum CSAssetContractState{
        UNKNOWN,
        CANNOT_PARSE,                                                           // Cannot parse PDF
        POSSIBLE_EMBEDDED_URL,                                                  // Embedded file found in PDF - embedded URL can be hidden inside, or unclear FS key
        EMBEDDED_URL,                                                           // Embedded URL found
        OK,                                                                     // Contract looks OK
    }
    
    private CSAssetState assetState;                                
    private CSAssetState assetValidationState;
      
    // Internal Asset ID in the database
    private int assetID;            
    
    // The date and time when the asset began being tracked.
    private Date dateCreation;
    
    /**
     * Method used when inserting asset into database.
     */
    
    public enum CSAssetSource{
        GENESIS,
        TRANSFER,
        MANUAL
    }
    
    private CSAssetSource assetSource;
    private CSAssetContractState assetContractState=CSAssetContractState.UNKNOWN;                                
    
    // A flag indicating whether the asset should be displayed to the user.
    private boolean visible = true;
    
    // The full bitcoin txid of the asset's genesis transaction. [null?]
    private String genTxID;
    
    // Decoded genesis structure
    private CoinSparkGenesis genesis;
    
    // Asset reference
    private CoinSparkAssetRef assetRef;
    
    // The date and time when the asset's validity was last checked. [null?]
    private Date validChecked;

    // Count of consecutive failures to verify the asset’s validity
    private int validFailures=0;
        
    // The contract underlying the asset, as retrieved from the URL in that JSON. [null?]
    private String contractPath;
    
    // The JSON of underlying the asset. [null?]
    private String jsonPath;
    
    // The last valid contract underlying the asset, as retrieved from the URL in that JSON. [null?]
    private String validContractPath;
    
    // The last valid JSON of underlying the asset. [null?]
    private String validJsonPath;

    private String validContractPathToSet;
    private String validJsonPathToSet;
    
    //The MIME type of that contract, as sent in the HTTP response headers when retrieving it. [null?]
    private CSUtils.CSMimeType contractMimeType;
    
    //The MIME type of that last valid contract, as sent in the HTTP response headers when retrieving it. [null?]
    private CSUtils.CSMimeType validContractMimeType;
    
    // Downloaded Asset icon
    private String iconPath;
    
    // Asset icon MIME type
    private CSUtils.CSMimeType iconMimeType;
   
    // Downloaded Asset image
    private String imagePath;
    
    // Asset image MIME type
    private CSUtils.CSMimeType imageMimeType;
    
    // Asset details as they are specified on Asset Web page
    
    private String firstSpentTxID=null;
    private long firstSpentVout=0;
    
    private String  name;
    private String  nameShort;
    private String  issuer;
    private String  description;
    private String  units;
    private String  contractUrl;
    private String selectedTrackerUrl;
    private String [] coinsparkTrackerUrls;
    private String  issueDate;
    private String  expiryDate;
    private Double  interestRate;
    private Double  multiple;
    private String  format;
    private String  format1;
    private String  iconUrl;
    private String  imageUrl;
    private String  feedUrl;
    private String  redemptionUrl;    
        
    
/**
 * Creates new Asset object.
 */
    
    public CSAsset()
    {
        dateCreation=new Date();
        assetState= CSAssetState.NO_KEY;
    }

/**
 * Creates asset object from JSON string stored in database
 * @param FilePrefix - prefix used for calculation of cached files directory 
 * @param AssetID - internal asset ID
 * @param Serialized - serialized JSON string stored in the databse
 */
    
    public CSAsset(String FilePrefix,int AssetID,String Serialized)
    {
        assetID=AssetID;
        parseJSONString(Serialized);
        
        if(assetID>0)
        {
            String prefix=FilePrefix+String.format("asset%06d", assetID);
            String validPrefix=FilePrefix+String.format("asset%06d_valid", assetID);
            
            jsonPath=prefix+".json";
            if(!(new File(jsonPath)).exists())
            {
                jsonPath=null;
            }

            validJsonPath=validPrefix+".json";
            if(!(new File(validJsonPath)).exists())
            {
                validJsonPath=null;
            }

            
            if(contractMimeType != null)
            {
                contractPath=prefix+"_contract"+contractMimeType.getExtension();
                if(!(new File(contractPath)).exists())
                {
                    contractMimeType=null;
                    contractPath=null;
                }
            }

            if(validContractMimeType != null)
            {
                validContractPath=validPrefix+"_contract"+validContractMimeType.getExtension();
                if(!(new File(validContractPath)).exists())
                {
                    validContractMimeType=null;
                    validContractPath=null;
                }
            }

            if(iconMimeType != null)
            {
                iconPath=prefix+"_icon"+iconMimeType.getExtension();
                if(!(new File(iconPath)).exists())
                {
                    iconMimeType=null;
                    iconPath=null;
                }
            }

            if(imageMimeType != null)
            {
                imagePath=prefix+"_image"+imageMimeType.getExtension();
                if(!(new File(imagePath)).exists())
                {
                    imagePath=null;
                    imagePath=null;
                }
            }
            
            if(assetState == CSAssetState.VALID)
            {
                if(contractMimeType == null)
                {
                    assetState = CSAssetState.CONTRACT_NOT_FOUND;
                    validChecked=new Date();
                    validFailures=1;
                }
            }
        }
    }
            
/**
 * Creates Asset object by genesis TxID and genesis object.
 * This asset should be created by wallet when it creates new asset.
 * This asset may be unconfirmed and so lack Asset reference.
 * @param GenTxID Genesis Transaction ID 
 * @param Genesis Genesis object
 * @param Block height of the block in blockchain. When Asset is created by the tx from blockchain only its block height is known, not offset 
 */ 
    
    public CSAsset(String GenTxID,CoinSparkGenesis Genesis,int Block)
    {
        assetID=0;
        genTxID=GenTxID;
        genesis=Genesis;
        assetSource=CSAssetSource.GENESIS;
        assetState=CSAssetState.NOT_VALIDATED_YET;
        dateCreation=new Date();
        if(Block >0)
        {
            assetRef=new CoinSparkAssetRef();
            assetRef.setBlockNum(Block);
            assetRef.setTxOffset(0);
            assetRef.setTxIDPrefix(Arrays.copyOf(CSUtils.hex2Byte(GenTxID),CoinSparkAssetRef.COINSPARK_ASSETREF_TXID_PREFIX_LEN));
        }
    }
    
/**
 * Creates Asset object by Asset reference.
 * This asset should be created by wallet when it become aware of new asset either from transfer list or manually inserting by the user.
 * This asset is created without genesis information. It should be retrieved asynchronously by background process.
 * @param AssetRef Asset reference
 * @param AssetSource Asset source - TRANSFER or MANUAL 
 */ 
    
    public CSAsset(CoinSparkAssetRef AssetRef,CSAsset.CSAssetSource AssetSource)
    {
        assetID=0;
        assetRef=AssetRef;
        assetSource=AssetSource;
        assetState= CSAssetState.ASSET_REF_ONLY;
        dateCreation=new Date();
    }
    
    
    public CSAsset.CSAssetState getAssetState(){return assetState;}                
    public CSAsset.CSAssetSource getAssetSource(){return assetSource;}
    public CSAsset.CSAssetContractState getAssetContractState(){return assetContractState;}
    public int     getAssetID(){return assetID;}                
    public Date    getDateCreation(){return dateCreation;}
    public boolean isVisible(){return visible;}
    public String  getGenTxID(){return genTxID;}
    public CoinSparkGenesis getGenesis(){return genesis;};
    public CoinSparkAssetRef getAssetReference(){return assetRef;}
    public Date    getValidChecked(){return validChecked;}
    public int     getValidFailures(){return validFailures;}
    public String  getJsonPath(){return jsonPath;}
    public String  getContractPath(){return contractPath;}
    public CSUtils.CSMimeType getContractMimeType(){return contractMimeType;}
    public String  getValidJsonPath(){return validJsonPath;}
    public String  getValidContractPath(){return validContractPath;}
    public CSUtils.CSMimeType getValidContractMimeType(){return validContractMimeType;}
    public String  getIconPath(){return iconPath;}    
    public String  getImagePath(){return imagePath;}    
    public String [] getCoinsparkTrackerUrls(){return coinsparkTrackerUrls;}

    
    public String  getName(){return name;}
    public String  getNameShort(){return nameShort;}
    public String  getIssuer(){return issuer;}
    public String  getDescription(){return description;}
    public String  getUnits(){return units;}
    public String  getContractUrl(){return contractUrl;}
    
/**
 * Selects one of the Tracker URLs and fixes it for getCoinsparkTrackerUrl.
 * @return selected tracker URL
 */    
    public String  selectCoinsparkTrackerUrl()
    {
        selectedTrackerUrl=null;
        
        if(coinsparkTrackerUrls == null){            
            return null;
        }
        
        if(coinsparkTrackerUrls.length <= 0){
            return null;
        }
        
        selectedTrackerUrl=coinsparkTrackerUrls[new Random().nextInt(coinsparkTrackerUrls.length)];
        
        return selectedTrackerUrl;
    }
    
    public String  getCoinsparkTrackerUrl()
    {
        return selectedTrackerUrl;        
    }
    
    public Date    getIssueDate(){return CSUtils.iso86012date(issueDate);}
    public Date    getExpiryDate(){return CSUtils.iso86012date(expiryDate);}
    public double  getInterestRate()
    {
        if(interestRate == null){
            return 0.0;
        }
        return interestRate;
    }
    
    public double  getMultiple()            
    {
        if(multiple == null){
            return 1.0;
        }
        return multiple;
    }
    
    public String  getFormat(){return format;}
    public String  getFormat1(){return format1;}
    public String  getIconUrl(){return iconUrl;}
    public String  getImageUrl(){return imageUrl;}
    public String  getFeedUrl(){return feedUrl;}
    public String  getRedemptionUrl(){return redemptionUrl;}    

    
   
    public void setName(String Name){name=Name;}
    public void setNameShort(String NameShort){nameShort=NameShort;}
    public void setIssuer(String Issuer){issuer=Issuer;}
    public void setDescription(String Description){description=Description;}
    public void setUnits(String Units){units=Units;}
    public void setContractUrl(String ContractUrl){contractUrl=ContractUrl;}
    public void setIssueDate(String IssueDate){issueDate=IssueDate;}
    public void setExpiryDate(String ExpiryDate){expiryDate=ExpiryDate;}
    public void setInterestRate(double InterestRate){interestRate=InterestRate;}
    public void setMultiple(double Multiple){multiple=Multiple;}
    public void setFormat(String Format){format=Format;}
    public void setFormat1(String Format1){format1=Format1;}
    public void setIconUrl(String IconUrl){iconUrl=IconUrl;}
    public void setImageUrl(String ImageUrl){imageUrl=ImageUrl;}
    public void setFeedUrl(String FeedUrl){feedUrl=FeedUrl;}
    public void setRedemptionUrl(String RedemptionUrl){redemptionUrl=RedemptionUrl;}    
    public void setVisibility(boolean Visibility){visible=Visibility;}
    
    protected void setAssetState(CSAssetState State)
    {
        assetState=State;
    }
    
    protected void setFirstSpentInput(String FirstSpentTxID,long FirstSpentVout)
    {
        firstSpentTxID=FirstSpentTxID;
        firstSpentVout=FirstSpentVout;
    }
    
    
/**
 * Sets AssetID, This method should be used only by Asset Database, to ensure thread-safety.
 * This function is called by Asset Database when asset is inserted to the database
 * @param AssetID Asset ID in database
 */    
    
    public void setAssetID(int AssetID)
    {
        assetID=AssetID;
    }
    
/**
 * Sets Asset reference for asset created by genesis, This method should be used only by Asset Database, to ensure thread-safety.
 * When wallets sees confirmed genesis in relevant transaction, it should find it in database and set Asset reference.
 * If the asset was not found (somebody else sent us genesis) wallet should calculate genesis, insert asset into database and set Asset reference 
 * @param AssetRef Asset reference
 */    
    
    public void setAssetRef(CoinSparkAssetRef AssetRef)
    {
        assetRef=AssetRef;
    }
    
/**
 * Validates asset reference, This function should be called by Asset Database to ensure thread-safety;
 * @param pg
 * @return true if asset info was updated in this function
 */
    
    public boolean validateAssetRef(PeerGroup pg)
    {
        if(pg == null)
        {
            return false;
        }
        
        if(genesis == null)
        {
            return false; 
        }
        
        if(assetRef == null)
        {
            return false; 
        }
        
        if(assetRef.getTxOffset() > 0)
        {
            return false; 
        }
        
        if(assetRef.getBlockNum()== 0)
        {
            return false; 
        }
        
        log.info("Asset: Retrieving asset reference information for asset " + assetID + ", AssetRef " + assetRef.toString());
        
        Block block=pg.getBlock((int)assetRef.getBlockNum());    

        if(block == null)
        {
            log.info("Asset: Cannot find block with height " + (int)assetRef.getBlockNum());
            return false;
        }

        int offset=block.getOffsetByTransaction(new Sha256Hash(genTxID));
        
        if(offset == 0)
        {
            log.info("Asset: Cannot find transaction with hash " + genTxID + " in block " +
                    block.getHashAsString());
            return false;
        }
        
        assetRef.setTxOffset(offset);
        
        return true;
    }
    
    private boolean validateGenesis(PeerGroup pg) 
    {
        CSAssetState initialState=assetValidationState;
        
        if(pg == null)
        {
            return false;
        }
        
        if(genesis != null)
        {
            if(firstSpentTxID != null)
            {
                return false; 
            }
        }
        
        if(assetRef == null)
        {
            assetValidationState = CSAssetState.NO_KEY;
            return (assetValidationState != initialState);
        }
        
        CSEventBus.INSTANCE.postAsyncEvent(CSEventType.ASSET_VALIDATION_STARTED, assetID);
        
        assetValidationState= CSAssetState.NOT_VALIDATED_YET;
        log.info("Asset: Retrieving genesis information for asset " + assetID + ", AssetRef " + assetRef.toString());
        
        Block block=pg.getBlock((int)assetRef.getBlockNum());    

        if(block == null)
        {
            assetValidationState= CSAssetState.BLOCK_NOT_FOUND;
            log.info("Asset: Cannot find block with height " + (int)assetRef.getBlockNum());
            return (assetValidationState != initialState);
        }

        Transaction tx=block.getTransactionByOffset((int)assetRef.getTxOffset());
        if(tx == null)
        {
            assetValidationState= CSAssetState.TX_NOT_FOUND;
            log.info("Asset: Cannot find transaction with offset " + (int)assetRef.getTxOffset() + " in block " +
                    block.getHashAsString());
            return (assetValidationState != initialState);
        }
        
        if(!Arrays.equals(Arrays.copyOf(assetRef.getTxIDPrefix(),CoinSparkAssetRef.COINSPARK_ASSETREF_TXID_PREFIX_LEN), 
                          Arrays.copyOf(tx.getHash().getBytes(),CoinSparkAssetRef.COINSPARK_ASSETREF_TXID_PREFIX_LEN)))
        {
            assetValidationState= CSAssetState.TX_NOT_FOUND;
            log.info("Asset: TxID prefix doesn't match, need " + CSUtils.byte2Hex(assetRef.getTxIDPrefix()) + ", found " +
                    CSUtils.byte2Hex(Arrays.copyOf(tx.getHash().getBytes(),CoinSparkAssetRef.COINSPARK_ASSETREF_TXID_PREFIX_LEN)));
            return (assetValidationState != initialState);           
        }
        
        genTxID=tx.getHash().toString();
        genesis=new CSTransactionAssets(tx).getGenesis();   
        
        TransactionInput firstInput=tx.getInput(0);
        if(firstInput != null)
        {            
            firstSpentTxID=firstInput.getOutpoint().getHash().toString();
            firstSpentVout=firstInput.getOutpoint().getIndex();
        }
        
        if(firstInput == null)
        {
            genesis=null;
        }
        
        assetValidationState= CSAssetState.NOT_VALIDATED_YET;
        if(genesis==null)
        {
            assetValidationState=CSAssetState.GENESIS_NOT_FOUND;
        }
        
        return (assetValidationState != initialState);
    }

/**
 * Returns Asset Web Page URL.
 * @return Asset Web Page URL
 */    
    
    public String getAssetWebPageURL()
    {
        if(genesis == null)
        {
            return null;
        }
        
        if(firstSpentTxID == null)
        {
            return null;
        }
        
        return genesis.calcAssetURL(firstSpentTxID, firstSpentVout);
    }
    
/**
 * Returns Home page of the issuer as it is calculated from genesis domain information.
 * @return URL based on genesis domain information
 */    
    
    public String getDomainURL()
    {
        if(genesis == null)
        {
            return null;
        }
        
        return genesis.getDomainURL();
    }
    
    
    
    private String fetchDetailsJSON(String FilePrefix)
    {
        if(genesis == null)
        {
            return null;
        }
        
        if(firstSpentTxID == null)
        {
            return null;
        }
        
        String jsonString=null;
        
        String response="";
        
        String webPageAddress=genesis.calcAssetURL(firstSpentTxID, firstSpentVout);
        log.info("Asset: Fetching asset details for asset " + assetID + " from " + webPageAddress);
        
        CSUtils.CSDownloadedURL downloaded=CSUtils.getURL(webPageAddress, 15, null);
        
        if(downloaded.error != null)
        {
            assetValidationState= CSAssetState.ASSET_WEB_PAGE_NOT_FOUND;
            log.info("Asset: Cannot fetch URL: " + downloaded.error);                
        }
        else
        {
            response=downloaded.contents;
        }
        
        String stringToFind="_bitcoin_asset_specification_(";
        int posStart=response.indexOf(stringToFind);
        
        if(posStart>=0)
        {
            posStart+=stringToFind.length();
            
            byte [] tail=response.substring(posStart).getBytes();
            
            boolean inEscape=false;
            boolean inQuotes=false;
            int depth=1;
            int pos=0;
            
            int posEnd=-1;
            while((depth > 0) && pos<tail.length)
            {
                if(inEscape)
                {
                    inEscape=false;                                             // We don't care about more than 1 character
                }
                else
                {
                    if(tail[pos] == '"')
                    {
                        inQuotes = !inQuotes;
                    }
                    if(inQuotes)
                    {
                        if(tail[pos] == '\\')
                        {
                            inEscape=true;
                        }
                    }
                    else
                    {
                        if(tail[pos] == '(')
                        {
                            depth++;
                        }
                        if(tail[pos] == ')')
                        {
                            depth--;
                        }
                    }
                }
                if(depth == 0)
                {
                    posEnd=pos;
                }
                pos++;
            }
            
            if(posEnd>0)
            {
                jsonString=new String(Arrays.copyOfRange(tail, 0, posEnd));
                String prefix=FilePrefix+String.format("asset%06d", assetID);
                String validPrefix=FilePrefix+String.format("asset%06d_valid", assetID);

                jsonPath=prefix+".json";
                validJsonPathToSet=validPrefix+".json";
                
                RandomAccessFile aFile=null;
                try {
                    aFile = new RandomAccessFile(jsonPath, "rw");
                    aFile.write(jsonString.getBytes("UTF-8"));
                    aFile.close();
                    
                } catch (FileNotFoundException ex) {
                    log.info("Asset DB: Cannot save json " + ex.getClass().getName() + " " + ex.getMessage());    
                    jsonString=null;
                } catch (UnsupportedEncodingException ex) {
                    log.info("Asset DB: Cannot save json " + ex.getClass().getName() + " " + ex.getMessage());    
                    jsonString=null;
                } catch (IOException ex) {
                    log.info("Asset DB: Cannot save json " + ex.getClass().getName() + " " + ex.getMessage());    
                    jsonString=null;
                }        
                
            }
        }
        else
        {
            assetValidationState= CSAssetState.ASSET_SPECS_NOT_FOUND;
            log.info("Asset: Asset specification not found");    
        }
        return jsonString;
    }
    
    private CSUtils.CSMimeType downloadFile(String URLString,String FileName)   
    {   
        log.info("Asset: Downloading " + FileName + " from " + URLString);
        
        CSUtils.CSDownloadedURL downloaded=CSUtils.getURL(URLString, 30, FileName);
        
        if(downloaded.error != null)
        {
            log.info("Asset: Cannot download " + downloaded.error);    
            return null;
        }
        
        return downloaded.mimeType;
    }
    
    private byte [] readContract() 
    {
        if(contractPath == null)
        {
            return null;
        }
        
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(contractPath, "r");
        } catch (FileNotFoundException ex) {
            log.info("Asset: Cannot open file " + ex.getClass().getName() + " " + ex.getMessage());                
            return null;
        }
        
        int fileSize;
        try {  
            fileSize = (int)aFile.length();                    
        } catch (IOException ex) {
            log.error("Asset: Cannot get file size " + ex.getClass().getName() + " " + ex.getMessage());                
            return null;
        }
        
        if(fileSize <= 0)
        {
            return null;
        }
        
        byte[] raw=new byte[fileSize];

        if(!CSUtils.readFromFileToBytes(aFile,raw))
        {
            log.error("Asset: Cannot read contract file");                
            return null;            
        }

        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Asset: Cannot close file " + ex.getClass().getName() + " " + ex.getMessage());                
        }    
        
        return raw;
    }
    
    private boolean validateContract(byte [] contractContent)
    {
        if(contractContent == null)
        {
            assetContractState=CSAssetContractState.CANNOT_PARSE;
            return false;                                    
        }
        
        CSPDFParser parser=new CSPDFParser(contractContent);

        assetContractState=CSAssetContractState.OK;

        int next=0;
        while(next<contractContent.length)
        {
            CSPDFParser.CSPDFObject obj;
            try 
            {
                obj = parser.getObject(next);
                if(obj != null)
                {
                    next=obj.offsetNext;
                    if(obj.type == CSPDFParser.CSPDFObjectType.STRANGE)
                    {
                        assetContractState=CSAssetContractState.CANNOT_PARSE;
                        return false;                        
                    }
                    else
                    {
                        CSPDFParser.CSPDFObjectEmbedded embedded=obj.hasEmbeddedFileOrURL();
                        switch(embedded)
                        {
                            case NONE:
                                break;
                            case STRANGE:
                                assetContractState=CSAssetContractState.CANNOT_PARSE;
                                return false;                        
                            case UNCLEAR:
                            case FILE:
                                assetContractState=CSAssetContractState.POSSIBLE_EMBEDDED_URL;
                                return false;
                            case URL:
                                assetContractState=CSAssetContractState.EMBEDDED_URL;
                                return false;
                        }                                    
                    }
                }
                else
                {
                    next=contractContent.length;                        
                }

            } catch (Exception ex) {
                Logger.getLogger(CSAsset.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        
        return true;
    }
    
    private boolean checkContractAndAssetHash()
    {
        if(genesis == null)
        {
            return false;
        }
        
        if(!checkRequiredFields())
        {
            return false;
        }
        
        byte [] contractContent=readContract();
    
        if(!validateContract(contractContent))
        {            
            assetValidationState = CSAssetState.CONTRACT_INVALID;
            return false;
        }
        
        byte [] assetHash=CoinSparkGenesis.calcAssetHash(name, issuer, description, units, issueDate,
                (expiryDate!=null) ? expiryDate : "",interestRate, multiple, contractContent);

        if(!genesis.validateAssetHash(assetHash))
        {
            assetValidationState = CSAssetState.HASH_MISMATCH;
            return false;
        }

        
                        
        try {
            Files.copy(new File(jsonPath).toPath(), new File(validJsonPathToSet).toPath(), REPLACE_EXISTING);
            validJsonPath=validJsonPathToSet;
            Files.copy(new File(contractPath).toPath(), new File(validContractPathToSet).toPath(), REPLACE_EXISTING);
            validContractPath=validContractPathToSet;
            validContractMimeType=contractMimeType;
        } catch (IOException ex) {
            Logger.getLogger(CSAsset.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return true;
    }

    private boolean checkRequiredFields()
    {
        if(genesis == null)
        {
            return false;
        }
        if(name == null){
            return false;
        }
        if(name.trim().length() == 0){
            return false;
        }
        if(nameShort == null){
            return false;
        }
        if(nameShort.trim().length() == 0){
            return false;
        }
        if(issuer == null){
            return false;
        }
        if(issuer.trim().length() == 0){
            return false;
        }
        if(description == null){
            return false;
        }
        if(description.trim().length() == 0){
            return false;
        }
        
        if(units == null){
            return false;
        }
        if(units.trim().length() == 0){
            return false;
        }

        if(contractUrl == null){
            return false;
        }
        try {
            final URL url = new URL(contractUrl);
            url.toURI();
        } catch (MalformedURLException ex) {
            log.info("Asset details: malformed contract URL " + contractUrl);
            return false;
        } catch (URISyntaxException ex) {
            log.info("Asset details: URL syntax error " + contractUrl);
            return false;
        }
        if(coinsparkTrackerUrls == null){
            return false;
        }
        for(String trackerUrl : coinsparkTrackerUrls)
        {
            try {
                final URL url = new URL(trackerUrl);
                url.toURI();
            } catch (MalformedURLException ex) {
                log.info("Asset details: malformed tracker URL " + trackerUrl);
                return false;
            } catch (URISyntaxException ex) {
                log.info("Asset details: URL syntax error " + trackerUrl);
                return false;
            }
        }
        return true;
    }
    
    private boolean checkAssetDetails(String FilePrefix)
    {
        if(!checkRequiredFields())
        {
            return false;
        }
        
        String prefix=FilePrefix+String.format("asset%06d", assetID);
        String validPrefix=FilePrefix+String.format("asset%06d_valid", assetID);
        
        contractMimeType=downloadFile(contractUrl, prefix+"_contract");
        if(contractMimeType != null)
        {
            contractPath=prefix+"_contract"+contractMimeType.getExtension();
            validContractPathToSet=validPrefix+"_contract"+contractMimeType.getExtension();
        }
        else
        {
            assetValidationState= CSAssetState.CONTRACT_NOT_FOUND;
            return false;
        }
        
        if(iconUrl!= null && !iconUrl.isEmpty())
        {
            iconMimeType=downloadFile(iconUrl, prefix+"_icon");
            if(iconMimeType != null)
            {
                iconPath=prefix+"_icon"+iconMimeType.getExtension();
            }
        }
        
        if(imageUrl!= null && !imageUrl.isEmpty())
        {
            imageMimeType=downloadFile(imageUrl, prefix+"_image");
            if(imageMimeType != null)
            {
                imagePath=prefix+"_image"+imageMimeType.getExtension();
            }
        }
        
        boolean hashValid=checkContractAndAssetHash();        
                
        return hashValid;
    }
    
    private boolean parseJSONString(String JSONString) 
    {
        if(JSONString == null)
        {
            return false;
        }
        
        if(JSONString.length()>0)
        {
            try
            {
                JsonElement jelement = new JsonParser().parse(JSONString);
                JsonObject jresult = jelement.getAsJsonObject();                        
                JsonElement jvalue;
                
                // All JSON strings - from asset web page and from asset database 
                jvalue=jresult.get("name");                 
                if(jvalue != null){
                    name=jvalue.getAsString();
                }
                jvalue=jresult.get("name_short");           
                if(jvalue != null){
                    nameShort=jvalue.getAsString();
                }
                jvalue=jresult.get("issuer");               
                if(jvalue != null){
                    issuer=jvalue.getAsString();
                }
                jvalue=jresult.get("description");          
                if(jvalue != null){
                    description=jvalue.getAsString();
                }
                jvalue=jresult.get("units");          
                if(jvalue != null){
                    units=jvalue.getAsString();
                }
                jvalue=jresult.get("contract_url");         
                if(jvalue != null){
                    contractUrl=jvalue.getAsString();
                }
                jvalue=jresult.get("coinspark_tracker_url");
                if(jvalue != null)
                {
                    if(jvalue.isJsonArray())
                    {
                        JsonArray jarray=jvalue.getAsJsonArray();
                        if(jarray.size() > 0)
                        {
                            coinsparkTrackerUrls=new String[jarray.size()];
                            for(int i=0;i<jarray.size();i++)
                            {
                                JsonElement jvalueUrl=jarray.get(i);
                                if(jvalueUrl != null)
                                {
                                    coinsparkTrackerUrls[i]=CSUtils.addHttpIfMissing(jvalueUrl.getAsString());
                                }
                            }
                        }
                    }
                    else
                    {
                        coinsparkTrackerUrls=new String[]{CSUtils.addHttpIfMissing(jvalue.getAsString())};
                    }
                }
                jvalue=jresult.get("issue_date");           
                if(jvalue != null){
                    issueDate=jvalue.getAsString();
                }
                jvalue=jresult.get("expiry_date");          
                if(jvalue != null){
                    expiryDate=jvalue.getAsString();
                }
                jvalue=jresult.get("interest_rate");        
                if(jvalue != null){
                    interestRate=jvalue.getAsDouble();
                }
                jvalue=jresult.get("multiple");             
                if(jvalue != null){
                    multiple=jvalue.getAsDouble();
                }
                jvalue=jresult.get("format");               
                if(jvalue != null){
                    format=jvalue.getAsString();
                }
                jvalue=jresult.get("format_1");             
                if(jvalue != null){
                    format1=jvalue.getAsString();
                }
                jvalue=jresult.get("icon_url");             
                if(jvalue != null){
                    iconUrl=jvalue.getAsString();
                }
                jvalue=jresult.get("image_url");            
                if(jvalue != null){
                    imageUrl=jvalue.getAsString();
                }
                jvalue=jresult.get("feed_url");             
                if(jvalue != null){
                    feedUrl=jvalue.getAsString();
                }
                jvalue=jresult.get("redemption_irl");       
                if(jvalue != null){
                    redemptionUrl=jvalue.getAsString();
                }
                
                // From asset database only
                jvalue=jresult.get("asset_id");             
                if(jvalue != null){
                    assetID=jvalue.getAsInt();
                }
                jvalue=jresult.get("date_created");         
                if(jvalue != null){
                    dateCreation=CSUtils.iso86012date(jvalue.getAsString());
                }
                jvalue=jresult.get("asset_state");          
                if(jvalue != null){
                    assetState=CSAssetState.valueOf(jvalue.getAsString());
                }
                jvalue=jresult.get("asset_source");         
                if(jvalue != null){
                    assetSource=CSAssetSource.valueOf(jvalue.getAsString());
                }
                jvalue=jresult.get("asset_contract_state");          
                if(jvalue != null){
                    assetContractState=CSAssetContractState.valueOf(jvalue.getAsString());
                }
                jvalue=jresult.get("gen_txid");             
                if(jvalue != null){
                    genTxID=jvalue.getAsString();
                }
                jvalue=jresult.get("visible");              
                if(jvalue != null){
                    if(jvalue.getAsInt()==0){
                        visible=false;
                    }
                    else {
                        visible=true;
                    }
                }
                jvalue=jresult.get("fsi_txid");             
                if(jvalue != null){
                    firstSpentTxID=jvalue.getAsString();
                }
                jvalue=jresult.get("fsi_vout");             
                if(jvalue != null){
                    firstSpentVout=jvalue.getAsInt();
                }
                jvalue=jresult.get("genesis");             
                if(jvalue != null)
                {
                    genesis=new CoinSparkGenesis();
                    if(!genesis.decode(jvalue.getAsString()))
                    {
                        genesis=null;
                    }                    
                }
                jvalue=jresult.get("asset_ref_block");             
                if(jvalue != null)
                {
                    assetRef=new CoinSparkAssetRef();
                    assetRef.setBlockNum(jvalue.getAsInt());
                    jvalue=jresult.get("asset_ref_offset");             
                    if(jvalue != null){
                        assetRef.setTxOffset(jvalue.getAsInt());
                    }
                    jvalue=jresult.get("asset_ref_prefix");             
                    if(jvalue != null){
                        assetRef.setTxIDPrefix(CSUtils.hex2Byte(jvalue.getAsString()));
                    }                    
                }
                jvalue=jresult.get("valid_checked");         
                if(jvalue != null){
                    validChecked=CSUtils.iso86012date(jvalue.getAsString());
                }
                jvalue=jresult.get("valid_failures");        
                if(jvalue != null){
                    validFailures=jvalue.getAsInt();
                }
                jvalue=jresult.get("contract_path");         
                if(jvalue != null){
                    contractPath=jvalue.getAsString();
                }
                jvalue=jresult.get("valid_contract_path");         
                if(jvalue != null){
                    validContractPath=jvalue.getAsString();
                }
                jvalue=jresult.get("json_path");         
                if(jvalue != null){
                    jsonPath=jvalue.getAsString();
                }
                jvalue=jresult.get("valid_json_path");         
                if(jvalue != null){
                    validJsonPathToSet=jvalue.getAsString();
                }
                jvalue=jresult.get("contract_mime");         
                if(jvalue != null){
                    contractMimeType=CSUtils.CSMimeType.fromExtension(jvalue.getAsString());
                }
                jvalue=jresult.get("valid_contract_mime");         
                if(jvalue != null){
                    validContractMimeType=CSUtils.CSMimeType.fromExtension(jvalue.getAsString());
                }
                jvalue=jresult.get("image_path");            
                if(jvalue != null){
                    imagePath=jvalue.getAsString();
                }
                jvalue=jresult.get("image_mime");            
                if(jvalue != null){
                    imageMimeType=CSUtils.CSMimeType.fromExtension(jvalue.getAsString());
                }
                jvalue=jresult.get("icon_path");             
                if(jvalue != null){
                    iconPath=jvalue.getAsString();
                }
                jvalue=jresult.get("icon_mime");             
                if(jvalue != null){
                    iconMimeType=CSUtils.CSMimeType.fromExtension(jvalue.getAsString());
                }                
            }
            catch(JsonSyntaxException ex)
            {
                assetValidationState= CSAssetState.ASSET_SPECS_NOT_PARSED;
                log.info("Asset details: Error while parsing JSON String " + JSONString);
                return false;
            } 
        }
        else
        {
            return false;
        }
        
        if(!checkRequiredFields())
        {
            assetValidationState= CSAssetState.REQUIRED_FIELD_MISSING;
            return false;
        }
                
        return true;
    }
    
    private boolean validateDetails(String FilePrefix)
    {
        if(assetValidationState == CSAssetState.NO_KEY)
        {
            return false;
        }
        
        if(genesis == null)
        {
            validChecked=new Date();
            validFailures++;
            
            return true;
        }
        
        String jsonString=fetchDetailsJSON(FilePrefix);
        
        if(!parseJSONString(jsonString))
        {
            validChecked=new Date();
            validFailures++;
            
            return true;
        }
        
        if(checkAssetDetails(FilePrefix))
        {
            validChecked=new Date();
            validFailures=0;            
            assetValidationState= CSAssetState.VALID;
        }
        else
        {
            validChecked=new Date();
            validFailures++;            
        }
        
        return true;
    }

    /**
     * Sets refresh flag.
     */
    
    public void setRefreshState()
    {
        assetState=CSAssetState.REFRESH;
    }

    /**
     * Returns number of seconds from now when the asset will be validated. If 0 is returned asset will be validate next time CSAssetDatabase.validateAssets() is called.
     * @return interval until next validation.
     */    
    
    public long nextValidationInterval()
    {
        long interval=0;

        if(validChecked==null)
        {
            return 0;
        }
        
        if(genesis != null)
        {
            switch(assetValidationState)
            {
                case NOT_VALIDATED_YET:
                case REFRESH:
                case INVALID:                    
                    break;
                case VALID:
                    interval=86400;
                    interval-=(new Date().getTime()-validChecked.getTime())/1000;
                    break;
                default:
                    if(validFailures<60)
                    {
                        interval=0;
                    }
                    else
                    {
                        if(validFailures<200)
                        {
                            interval=1800;
                        }
                        else
                        {
                            interval=86400;
                        }
                    }
                    interval-=(new Date().getTime()-validChecked.getTime())/1000;
                    break;
            }
        }
        else
        {
            switch(assetValidationState)
            {
                case TX_NOT_FOUND:
                case GENESIS_NOT_FOUND:
                    if(validFailures<10)
                    {
                        interval=0;
                    }
                    else
                    {
                        if(validFailures<20)
                        {
                            interval=600;
                        }
                        else
                        {
                            interval=86400;
                        }
                    }
                    interval-=(new Date().getTime()-validChecked.getTime())/1000;
                    break;
            }
        }
        
        if(interval<0)
        {
            interval=0;
        }
        
        return interval;
    }
    
    /**
     * Asset can be sent only if this function return true and asset.getAssetState()=CSAssetState.VALID
     * @return validity flag of Asset reference
     */    
    
    public boolean isAssetRefValid()
    {
        if(assetRef == null){
            return false;
        }
        
        if(assetRef.getTxOffset() == 0){
            return false;
        }
        
        return true;
    }
    
    /**
     * Validate asset.Retrieves Genesis if only AssetRef is given
 Retrieves Asset details from JSON string if missing
 Validates Asset details and download contract, icon and image if needed
     * @param FilePrefix Full path to the directory (with possible prefix) where asset details files should be stored
     * @param pg PeerGroup used for network communications
     * @param ForceRefresh Validate asset even if nextValidationInterval is positive
     * @return true if update required
     */
    
    protected boolean validate(String FilePrefix,PeerGroup pg,boolean ForceRefresh)
    {       
        boolean updateRequired=false;
 
        assetValidationState=assetState;
       
        updateRequired |= validateGenesis(pg);
        

        boolean refreshDetails=updateRequired;

        if(nextValidationInterval() == 0)
        {
            refreshDetails=true;
        }

        if(ForceRefresh)
        {
            refreshDetails=true;
        }

        if(refreshDetails)
        {
            CSEventBus.INSTANCE.postAsyncEvent(CSEventType.ASSET_VALIDATION_STARTED, assetID);
            updateRequired |= validateDetails(FilePrefix);            
        }
        
        updateRequired |= (assetState != assetValidationState);
        
        assetState=assetValidationState;
                
        return updateRequired;        
    }    

    
    private JsonObject getJSONObject()
    {
        JsonObject jobject=new JsonObject();
        if(name != null){
            jobject.addProperty("name",                     name);
        }
        if(nameShort != null){
            jobject.addProperty("name_short",               nameShort);
        }
        if(issuer != null){
            jobject.addProperty("issuer",                   issuer);
        }
        if(description != null){
            jobject.addProperty("description",              description);
        }
        if(units != null){
            jobject.addProperty("units",                    units);
        }
        if(contractUrl != null){
            jobject.addProperty("contract_url",             contractUrl);
        }
        if(coinsparkTrackerUrls != null)
        {
            JsonArray jarray=new JsonArray();
            for(String trackerUrl : coinsparkTrackerUrls)
            {
                jarray.add(new JsonPrimitive(trackerUrl));                
            }
            jobject.add("coinspark_tracker_url", jarray);
        }
        if(issueDate != null){
            jobject.addProperty("issue_date",               issueDate);
        }
        if(expiryDate != null){
            jobject.addProperty("expiry_date",              expiryDate);
        }
        if(interestRate != null ){
            jobject.addProperty("interest_rate",            interestRate);
        }
        if(multiple != null ){
            jobject.addProperty("multiple",                 multiple);
        }
        if(format != null){
            jobject.addProperty("format",                   format);
        }
        if(format1 != null){
            jobject.addProperty("format_1",                 format1);
        }
        if(iconUrl != null){
            jobject.addProperty("icon_url",                 iconUrl);
        }
        if(imageUrl != null){
            jobject.addProperty("image_url",                imageUrl);
        }
        if(feedUrl != null){
            jobject.addProperty("feed_url",                 feedUrl);
        }
        if(redemptionUrl != null){
            jobject.addProperty("redemption_irl",           redemptionUrl);
        }

        return jobject;
    }
    
/**
 * Returns JSON string for this asset.
 * @return 
 */    
    
    private String getJSONString()
    {
        JsonObject jobject=getJSONObject();
        return new Gson().toJson(jobject);
    }    
    
    
/**
 * Serializes asset for storing in database.
 * @return Serialized JSON string.
 */    
    
    protected String serialize()
    {        
        JsonObject jobject=getJSONObject();
        if(dateCreation != null){
            jobject.addProperty("date_created",             CSUtils.date2iso8601(dateCreation));
        }
        if(assetState != null){
            jobject.addProperty("asset_state",              assetState.toString());
        }
        if(assetSource != null){
            jobject.addProperty("asset_source",             assetSource.toString());
        }
        if(assetContractState != null){
            jobject.addProperty("asset_contract_state",     assetContractState.toString());
        }
        if(genTxID != null){
            jobject.addProperty("gen_txid",                 genTxID);
        }
        if(visible){
            jobject.addProperty("visible",1);
        } else {
            jobject.addProperty("visible",0);
        }
        if(firstSpentTxID != null)      
        {
            jobject.addProperty("fsi_txid",                 firstSpentTxID);
            jobject.addProperty("fsi_vout",                 firstSpentVout);
        }
        if(genesis != null)             
        {
            jobject.addProperty("genesis",             genesis.encodeToHex(65536));// We don't care whether it fits OP_RETURN or not
        }
        if(assetRef != null)
        {
            jobject.addProperty("asset_ref_block",                assetRef.getBlockNum());
            jobject.addProperty("asset_ref_offset",               assetRef.getTxOffset());
            jobject.addProperty("asset_ref_prefix",               CSUtils.byte2Hex(assetRef.getTxIDPrefix()));
        }
        if(validChecked != null){
            jobject.addProperty("valid_checked",            CSUtils.date2iso8601(validChecked));
        }
        jobject.addProperty("valid_failures",validFailures);
        if(contractMimeType != null){
            jobject.addProperty("contract_mime",            contractMimeType.getExtension());
        }
        if(validContractMimeType != null){
            jobject.addProperty("valid_contract_mime",            contractMimeType.getExtension());
        }
        if(imageMimeType != null){
            jobject.addProperty("image_mime",               imageMimeType.getExtension());
        }
        if(iconMimeType != null){
            jobject.addProperty("icon_mime",            iconMimeType.getExtension());
        }
        
        
        return new Gson().toJson(jobject);
    }        
    
    /**
     * Returns short asset status.
     * @return short asset status.
     */
    
    public String status()
    {
        String s="";
        
        s+=assetState;
        if(assetRef != null)
        {
            s+=", Asset Ref.: " + assetRef.encode();
        }
        if(genTxID != null)
        {
            s+=", Gen. TxID: " + genTxID;
        }
                
        return s;
    }
    
/**
 * Returns full asset status for debugging.
 * @return full asset status.
 */    
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("AssetID:        ").append(assetID).append("\n");
        sb.append("Date created:   ").append(dateCreation == null ? "" : dateCreation.toString()).append("\n");
        sb.append("State:          ").append(assetState == null ? "" : assetState).append("\n");
        sb.append("Source:         ").append(assetSource == null ? "" : assetSource).append("\n");
        sb.append("Genesis IxID:   ").append(genTxID == null ? "" : genTxID).append("\n");
        sb.append("Genesis:        ").append(genesis == null ? "" : genesis.toString()).append("\n");
        sb.append("AssetRef:       ").append(assetRef == null ? "" : assetRef.encode()).append("\n");
        sb.append("JSONString:     ").append(getJSONString()).append("\n");
        sb.append("Date validated: ").append(validChecked == null ? "" : validChecked.toString()).append("\n");
        sb.append("Failure count:  ").append(validFailures).append("\n");
        sb.append("Contract path:  ").append((contractPath == null || contractMimeType == null) ? "" : (contractPath + "(" + contractMimeType.getExtension() +")")).append("\n");
        sb.append("Image path:     ").append(imagePath == null ? "" : (imagePath + "(" + imageMimeType.getExtension() +")")).append("\n");
        sb.append("Icon path:      ").append(iconPath == null ? "" : (iconPath + "(" + iconMimeType.getExtension() +")")).append("\n");

        return sb.toString();        
    }    
    
    
}
