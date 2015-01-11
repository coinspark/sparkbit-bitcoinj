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
import com.google.bitcoin.core.ECKey;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.TransactionInput;
import com.google.bitcoin.core.TransactionOutput;
import com.google.bitcoin.core.Wallet;
import com.google.bitcoin.script.Script;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.coinspark.protocol.CoinSparkAssetRef;
import org.coinspark.protocol.CoinSparkBase;
import org.coinspark.protocol.CoinSparkGenesis;
import org.coinspark.protocol.CoinSparkMessage;
import org.coinspark.protocol.CoinSparkPaymentRef;
import org.coinspark.protocol.CoinSparkTransfer;
import org.coinspark.protocol.CoinSparkTransferList;

public class CSTransactionAssets {
    
    private CoinSparkGenesis genesis = null;
    private CoinSparkTransferList transfers = null;
    private CoinSparkMessage message=null;
    private CoinSparkPaymentRef paymentRef=null;
    private Transaction parentTransaction;

    public CSTransactionAssets()
    {
        parentTransaction=null;
    }
    
    public CSTransactionAssets(Transaction ParentTx)
    {
        this.parentTransaction = ParentTx;
        
        byte [] txnMetaData = null;
        for (TransactionOutput output : ParentTx.getOutputs())
        {
            byte[] scriptBytes = output.getScriptBytes();

            if(!CoinSparkBase.scriptIsRegular(scriptBytes))
            {
                txnMetaData=CoinSparkBase.scriptToMetadata(scriptBytes);
            }
        }

        if(txnMetaData != null)
        {
            genesis = new CoinSparkGenesis();
            if (!genesis.decode(txnMetaData))
            {
                genesis=null;
            }

            transfers = new CoinSparkTransferList();
            if (!transfers.decode(txnMetaData, ParentTx.getInputs().size(), ParentTx.getOutputs().size()))
            {
                transfers=null;
            }
            
            message=new CoinSparkMessage();
            if(!message.decode(txnMetaData,  ParentTx.getOutputs().size()))
            {
                message=null;
            }
            
            paymentRef=new CoinSparkPaymentRef();
            if(!paymentRef.decode(txnMetaData))
            {
                paymentRef=null;
            }
            
        }
        
    }
    
    public CoinSparkGenesis getGenesis()
    {
        return genesis;
    }
    
    public CoinSparkTransferList getTransfers()
    {
        return transfers;
    }
    
    
    public boolean updateAssetBalances(Wallet wallet,int Block,Map<Integer, long []> inputBalances)
    {
        CSAssetDatabase assetDB=wallet.CS.getAssetDB();
        CSBalanceDatabase balanceDB=wallet.CS.getBalanceDB();
        CSMessageDatabase messageDB=wallet.CS.getMessageDB();
        
        if(assetDB == null)
        {
            return false;
        }
        
        if(balanceDB == null)
        {
            return false;
        }

        boolean [] defaultOutputs;
        boolean retrieveInputBalances=false;
        
        int output_id=0;
        int countOutputs=parentTransaction.getOutputs().size();
        int countInputs=parentTransaction.getInputs().size();
                
        ArrayList <Map <Integer,BigInteger>> outputBalanceMap = new ArrayList <Map <Integer,BigInteger>>();
        for(output_id=0;output_id<countOutputs;output_id++)
        {
            Map <Integer,BigInteger> map=new HashMap <Integer,BigInteger>();
            map.put(0,parentTransaction.getOutput(output_id).getValue());
            outputBalanceMap.add(map);
        }
        
        if(genesis != null)
        {
            boolean [] outputsRegular=new boolean[countOutputs];
            output_id=0;
            for (TransactionOutput output : parentTransaction.getOutputs())
            {
                outputsRegular[output_id] = CoinSparkBase.scriptIsRegular(output.getScriptBytes());
                output_id++;
            }
            long [] outputBalances;
            outputBalances=genesis.apply(outputsRegular);
            for(output_id=0;output_id<countOutputs;output_id++)
            {
                outputBalanceMap.get(output_id).put(-1,BigInteger.valueOf(outputBalances[output_id]));
            }                        
            retrieveInputBalances=true;
        }
        
        CoinSparkTransferList transfersToApply=transfers;
        if(transfersToApply == null)
        {
            transfersToApply=new CoinSparkTransferList();
        }
        
        boolean [] outputsRegular=new boolean[countOutputs];
        output_id=0;
        for (TransactionOutput output : parentTransaction.getOutputs())
        {
            outputsRegular[output_id] = CoinSparkBase.scriptIsRegular(output.getScriptBytes());
            output_id++;
        }
        defaultOutputs=transfersToApply.defaultOutputs(countInputs, outputsRegular);

        retrieveInputBalances=true;
        if(inputBalances != null)
        {
            if(inputBalances.containsKey(0))
            {
                retrieveInputBalances=false;
                long totalInput=0;
                long totalOutput=0;
                long [] outputsSatoshis=new long [countOutputs];
                for(int input_id=0;input_id<parentTransaction.getInputs().size();input_id++)
                {
                    totalInput+=inputBalances.get(0)[input_id];
                }
                for(output_id=0;output_id<countOutputs;output_id++)
                {
                    long value=parentTransaction.getOutput(output_id).getValue().longValue();
                    outputsSatoshis[output_id]=value;
                    totalOutput+=value;
                }

                long validFeeSatoshis=0;
                if(transfers != null)
                {
                    validFeeSatoshis=transfers.calcMinFee(countInputs, outputsSatoshis, outputsRegular);
                }
                long feeSatoshis=totalInput-totalOutput;
                for (Map.Entry<Integer, long []> entry : inputBalances.entrySet())                         
                {
                    int assetID=entry.getKey();
                    CoinSparkAssetRef assetRef;
                    CSAsset asset=assetDB.getAsset(assetID);
                    if(assetID != 0)
                    {
                        if(asset != null && asset.getGenesis() != null)
                        {
                            assetRef=asset.getAssetReference();
                            if(assetRef != null)
                            {
                                long [] outputBalances;
                                
                                if((transfers != null) && (feeSatoshis >= validFeeSatoshis))
                                {
                                    outputBalances=transfersToApply.apply(assetRef, asset.getGenesis(), entry.getValue(),outputsRegular);
                                }
                                else
                                {
                                    outputBalances=transfersToApply.applyNone(assetRef, asset.getGenesis(), entry.getValue(), outputsRegular);
                                }
                                
                                for(output_id=0;output_id<countOutputs;output_id++)
                                {
                                    outputBalanceMap.get(output_id).put(assetID,BigInteger.valueOf(outputBalances[output_id]));
                                }                                    
                            }
                        }
                    }
                }        
            }
        }
        
        Sha256Hash hash=parentTransaction.getHash();

        int [] inputAssetIDs=null;
        if(genesis != null)
        {
            inputAssetIDs=new int[1];
        }
        if(transfers != null)
        {
            inputAssetIDs=new int[transfers.count()];
            for(int i=0;i<transfers.count();i++)
            {
                inputAssetIDs[i]=0;
            }
        }
        CSAsset found=null;

        String [] messageAddresses=new String[parentTransaction.getOutputs().size()];
        int addressCount=0;
                
        output_id=0;
        for (TransactionOutput output : parentTransaction.getOutputs())
        {
            if(output.isMine(wallet))
            {
                if(genesis != null)
                {
                    if(found == null)
                    {
                        CSAsset asset=new CSAsset(hash.toString(), genesis,Block);
                        TransactionInput firstInput=parentTransaction.getInput(0);
                        if(firstInput != null)
                        {            
                            asset.setFirstSpentInput(firstInput.getOutpoint().getHash().toString(),firstInput.getOutpoint().getIndex());
                        }

                        found=assetDB.findAsset(asset);
                        if(found == null)
                        {
                            found=assetDB.insertAsset(asset);
                            if(found == null)
                            {
                                return false;
                            }
                        }            
                        else
                        {
                            if(Block>0)
                            {
                                CoinSparkAssetRef assetRef = new CoinSparkAssetRef();
                                assetRef.setBlockNum(Block);
                                assetRef.setTxOffset(0);
                                assetRef.setTxIDPrefix(Arrays.copyOf(hash.getBytes(),CoinSparkAssetRef.COINSPARK_ASSETREF_TXID_PREFIX_LEN));

                                if(!assetDB.setAssetReference(found,assetRef))
                                {
                                    return false;
                                }
                            }
                        }                            
                    }
                    if(!defaultOutputs[output_id])
                    {
                        Map <Integer,BigInteger> map=outputBalanceMap.get(output_id);
/*                        
                        if(!map.containsKey(found.getAssetID()))
                        {
                            if(map.containsKey(-1))
                            {
                                map.put(found.getAssetID(), BigInteger.valueOf((-1)*map.get(-1).longValue()));
                            }
                        }
*/        
                        balanceDB.insertTxOut(new CSTransactionOutput(hash, output_id),  new int [] {found.getAssetID()},outputBalanceMap.get(output_id));
                        inputAssetIDs[0]=found.getAssetID();
                    }
                }
                
                if(defaultOutputs[output_id]) 
                {
                    balanceDB.insertTxOut(new CSTransactionOutput(hash, output_id), null,outputBalanceMap.get(output_id));                    
                }
                else
                {
                    if(transfers != null)
                    {
                        Map <Integer,BigInteger> map=outputBalanceMap.get(output_id);
                        int [] assetIDs=new int[transfers.count()];
                        int size=0;
                        for(int i=0;i<transfers.count();i++)
                        {
                            CoinSparkTransfer transfer=transfers.getTransfer(i);
                            if(transfer != null)
                            {
                                int first=transfer.getOutputs().first;
                                int count=transfer.getOutputs().count;
                                if(first<=output_id)
                                {
                                    if(output_id<first+count)
                                    {
                                        CSAsset asset=new CSAsset(transfer.getAssetRef(), CSAsset.CSAssetSource.TRANSFER);
                                        found=assetDB.findAsset(asset);
                                        if(found == null)
                                        {
                                            found=assetDB.insertAsset(asset);
                                            if(found == null)
                                            {
                                                return false;
                                            }
                                            balanceDB.insertAsset(found.getAssetID());
                                        }            
                                        assetIDs[size]=found.getAssetID();
                                        inputAssetIDs[i]=found.getAssetID();
                                        if(inputBalances == null || !inputBalances.containsKey(inputAssetIDs[i]))                                            
                                        {
                                            retrieveInputBalances=true;
                                        }
                                        
                                        if(!map.containsKey(found.getAssetID()))
                                        {
                                            map.put(found.getAssetID(), BigInteger.valueOf((-1)*transfer.getQtyPerOutput()));
                                        }
        
                                        size++;
                                    }                            
                                }
                            }
                        }
                        if(size>0)
                        {
                            balanceDB.insertTxOut(new CSTransactionOutput(hash, output_id),  assetIDs,outputBalanceMap.get(output_id));                            
                        }
                    }
                }
                
                if(message != null)
//                if(false)
                {                    
                    if(message.hasOutput(output_id))
                    {
                        ECKey key=null;
                        Script connectedScript = output.getScriptPubKey();
                        if (connectedScript.isSentToAddress()) {
                            byte[] addressBytes = connectedScript.getPubKeyHash();
                            key=wallet.findKeyFromPubHash(addressBytes);
                        } 
                        else 
                        {
                            if (connectedScript.isSentToRawPubKey()) 
                            {
                                byte[] pubkeyBytes = connectedScript.getPubKey();
                                key=wallet.findKeyFromPubKey(pubkeyBytes);
                            }
                        }
                        if(key != null)
                        {
                            Address pubKeyHash=new Address(wallet.getNetworkParameters(), key.getPubKeyHash());
                            String address=pubKeyHash.toString();
                            boolean addressFound=false;
                            for(String addressToCheck : messageAddresses)
                            {
                                if(addressToCheck != null)
                                {
                                    if(!addressFound)
                                    {
                                        if(addressToCheck.equals(address))
                                        {
                                            addressFound=true;
                                        }
                                    }
                                }
                            }
                            if(!addressFound)
                            {
                                messageAddresses[addressCount]=address;
                                addressCount++;                                
                            }
                        }
                    }
                }
                
            }
            output_id++;
        }        
        
        if(addressCount == 0)
        {
            message=null;
        }
        
        if((addressCount>0) || (paymentRef != null))
        {            
            if(messageDB != null)
            {
                messageDB.insertReceivedMessage(hash.toString(), parentTransaction.getOutputs().size(), paymentRef, message, Arrays.copyOf(messageAddresses, addressCount));
            }
        }
        
/*        
        if(retrieveInputBalances)
        {
            balanceDB.addTrackedTransaction(parentTransaction);
        }
*/
        return true;
    }
    
    
}