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

import java.math.BigInteger;
import java.util.Date;

public class CSBalance {
    private CSTransactionOutput txOut;
    private int assetID;
    private BigInteger qty;
    private Date qtyChecked;
    private int qtyFailures;
    
    // Balance state
    public enum CSBalanceState{
        NEVERCHECKED,   // Balance not 
        UNKNOWN,        // Tracking server doesn't know about this txout or don't track this color
        SELF,           // This balance is unconfirmed, but the source of this value is our wallet, so we can be quite confident in tis value
        REFRESH,        // Balance should be checked (manually set)
        SPENT,          // TxOut is spent
        VALID,          // Balance value returned and is not 0
        ZERO,           // the value of this asset is 0
        DELETED,        // Asset deleted 
    }

    CSBalance.CSBalanceState balanceState;

    public CSBalance()
    {
    }
    
    public CSBalance(CSTransactionOutput TxOut,int AssetID)
    {
        txOut=TxOut; 
        assetID=AssetID;
        balanceState=CSBalance.CSBalanceState.NEVERCHECKED;
    }

    public CSBalance(CSTransactionOutput TxOut,int AssetID,BigInteger Qty,Date QtyChecked,int QtyFailures,CSBalance.CSBalanceState State)
    {
        txOut=TxOut; 
        assetID=AssetID;
        qty=Qty;
        qtyChecked=QtyChecked;
        qtyFailures=QtyFailures;                
        balanceState=State;
    }

    @Override
    public String toString()
    {
        return txOut.toString() + "-" + assetID + "-" + qty + "-" + qtyChecked + "-" + qtyFailures + "-" + balanceState;
    }
            
            
    public CSTransactionOutput getTxOut(){return txOut;}
    public int getAssetID(){return assetID;}
    public BigInteger getQty(){return qty;}
    public Date getQtyChecked(){return qtyChecked;}
    public int getQtyFailures(){return qtyFailures;}
    public CSBalance.CSBalanceState getState(){return balanceState;}
    
}
