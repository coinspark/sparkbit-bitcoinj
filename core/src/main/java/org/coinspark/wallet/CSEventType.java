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

/**
 * If an event has a payload (it might not) detail in comments next to name of event.
 */
public enum CSEventType {
    ASSET_INSERTED,                                                             // payload: int assetid
    ASSET_UPDATED,                                                              // payload: int assetid
    ASSET_DELETED,                                                              // payload: int assetid
    ASSET_VALIDATION_STARTED,                                                   // payload: int assetid
    ASSET_VALIDATION_COMPLETED,                                                 // payload: int assetid
    BALANCE_VALID,                                                              // Valid non-zero balance calculated, payload CSBalance
    ASSET_VISIBILITY_CHANGED, // payload: int assetid
    MESSAGE_RETRIEVAL_STARTED,                                                  // payload: String TxID
    MESSAGE_RETRIEVAL_COMPLETED,                                                // payload: String TxID
}
