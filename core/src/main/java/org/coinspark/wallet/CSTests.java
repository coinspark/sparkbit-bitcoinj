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
import org.coinspark.core.CSUtils;
import org.coinspark.protocol.CoinSparkAssetRef;
import org.coinspark.protocol.CoinSparkTransferList;

public class CSTests {
    
    private PeerGroup peerGroup;
    private String filePrefix;
    private CSAssetDatabase assetDB;
    private CSBalanceDatabase balanceDB;
    
    public CSTests(PeerGroup pg,String FilePrefix)
    {
        peerGroup=pg;
        filePrefix=FilePrefix;
        assetDB=new CSAssetDatabase(FilePrefix,null);
        balanceDB=new CSBalanceDatabase(FilePrefix, assetDB,null);
    }
    
    public void test()
    {
        test01();
    }
    
    private void test01()
    {        
        CoinSparkTransferList a=new CoinSparkTransferList();
                
        System.out.println("Test01 started");
        
        CoinSparkAssetRef assetRef=new CoinSparkAssetRef();
        assetRef.setBlockNum(208441);
        assetRef.setTxOffset(190);
        assetRef.setTxIDPrefix(CSUtils.hex2Byte("e0a8"));
        
        CSAsset asset=new CSAsset(assetRef, CSAsset.CSAssetSource.MANUAL);
        CSAsset found;
        
        found=assetDB.findAsset(asset);
        
        if(found != null)
        {
            System.out.println("Asset with reference " + asset.getAssetReference().toString() + " found with ID " + found.getAssetID());                    
        }
        else
        {
            found=assetDB.insertAsset(asset);            
            if(found != null)
            {
                System.out.println("Asset with reference " + asset.getAssetReference().toString() + " inserted with ID " + found.getAssetID());                    
            }
            else
            {
                System.out.println("Asset with reference " + asset.getAssetReference().toString() + " not inserted");                    
            }
        }

        if(found != null)
        {
            if(found.validate(assetDB.getDirName(), peerGroup,false))
            {
                assetDB.updateAsset(found);
            }
        }        
        
        System.out.println("Test01 completed");        
    }    
    
}
