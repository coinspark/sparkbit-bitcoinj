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
import com.google.bitcoin.core.TransactionConfidence;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class CSTransactionOutput {
    
    private Sha256Hash txID;
    private int index;
    private Transaction parentTransaction=null;
    BigInteger value=null;
    
    public Sha256Hash getTxID(){return txID;}
    public int getIndex(){return index;}
    
    CSTransactionOutput(){}
    
    public CSTransactionOutput(Sha256Hash TxID,int Index)
    {
        txID=TxID;
        index=Index;
    }

    public CSTransactionOutput(Transaction Parent,int Index)
    {
        parentTransaction=Parent;
        txID=parentTransaction.getHash();
        index=Index;
    }
    
    public CSTransactionOutput(String Str)
    {
        txID=new Sha256Hash(Str.substring(0, 64));
        index=Integer.parseInt(Str.substring(65), 16);
    }
    
    public String getStrValue()
    {
        return txID.toString() + "-" + java.lang.Integer.toString(index, 16);
    }
    
    public void setValue(BigInteger Value)
    {
        value=Value;
    }
    
    public BigInteger getValue()
    {
        return value;
    }
    
    public Transaction getParentTransaction()
    {
        return parentTransaction;
    }
    
    @Override
    public String toString()
    {
        return getStrValue();
    }
    
    public boolean equals(CSTransactionOutput TxOut)
    {
        if(TxOut == null)
        {
            return false;
        }
        if(txID.equals(TxOut.getTxID()))
        {
            if(index == TxOut.getIndex())
            {
                return true;
            }
        }
        return false;
    }
    
    public boolean equals(String TxID, int Index)
    {
        if(txID.equals(new Sha256Hash(TxID)))
        {
            if(index == Index)
            {
                return true;
            }
        }
        return false;
    }
    
    public static void sortOutputs(ArrayList<CSTransactionOutput> outputs) {
        Collections.sort(outputs, new Comparator<CSTransactionOutput>() {
            
            @Override
            public int compare(CSTransactionOutput a, CSTransactionOutput b) {
                
                int depth1 = 0;
                int depth2 = 0;
                TransactionConfidence conf1 = a.getParentTransaction().getConfidence();
                TransactionConfidence conf2 = b.getParentTransaction().getConfidence();
                if (conf1.getConfidenceType() == TransactionConfidence.ConfidenceType.BUILDING)
                {
                    depth1 = conf1.getDepthInBlocks();
                }
                if (conf2.getConfidenceType() == TransactionConfidence.ConfidenceType.BUILDING)
                {
                    depth2 = conf2.getDepthInBlocks();
                }
                BigInteger aValue = a.getValue();
                BigInteger bValue = b.getValue();
                BigInteger aCoinDepth = aValue.multiply(BigInteger.valueOf(depth1));
                BigInteger bCoinDepth = bValue.multiply(BigInteger.valueOf(depth2));
                int c1 = bCoinDepth.compareTo(aCoinDepth);
                if (c1 != 0){
                    return c1;
                }
                // The "coin*days" destroyed are equal, sort by value alone to get the lowest transaction size.
                int c2 = bValue.compareTo(aValue);
                if (c2 != 0){
                    return c2;
                }
                // They are entirely equivalent (possibly pending) so sort by hash to ensure a total ordering.
                BigInteger aHash = a.getParentTransaction().getHash().toBigInteger();
                BigInteger bHash = b.getParentTransaction().getHash().toBigInteger();
                return aHash.compareTo(bHash);
            }

        });
    }

}
