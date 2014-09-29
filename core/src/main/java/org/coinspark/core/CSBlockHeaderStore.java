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
package org.coinspark.core;

import com.google.bitcoin.core.NetworkParameters;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.StoredBlock;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Block store extension. 
 * Stores hashes of all blocks in the blockchain.
 * This class should be used only in bitcoinj.
 */

public class CSBlockHeaderStore {
    
    private static final Logger log = LoggerFactory.getLogger(CSBlockHeaderStore.class);
    
    private static final String FULL_BLOCK_HASHES_SUFFIX = ".fbhchain";
    private static final int FULL_BLOCK_HASHES_SIZE = 32;
    
    private String fileName;
    private boolean isCorrupted;
    private NetworkParameters networkParameters;
    
    /**
     * Initializes block store. Create new if needed.
     * @param NetworkParameters
     * @param FilePrefix .fbchain file prefix
     */
    
    public CSBlockHeaderStore(NetworkParameters NetworkParameters,String FilePrefix)
    {
        fileName = FilePrefix + FULL_BLOCK_HASHES_SUFFIX;
        networkParameters = NetworkParameters;
        
        isCorrupted=false;
        
        File fbhStoreFile = new File(fileName);
        boolean bfbhStoreCreatedNew = !fbhStoreFile.exists();
                        
        if(bfbhStoreCreatedNew)
        {
            Path path = Paths.get(fileName);
            try {
                Files.write(path, networkParameters.getGenesisBlock().getHash().getBytes());
            } catch (IOException ex) {
                isCorrupted=true;
                log.error("Cannot create headers file " + ex.getClass().getName() + " " + ex.getMessage());                
            }
        }
    }
    
    /**
     * 
     * @return File name of extended block store
     */
    
    public String getFileName()
    {
        return fileName;
    }

    /**
     * Puts new block hash in the file.
     * @param block
     * @return 
     */
    
    public boolean put(StoredBlock block)
    {        
        if(isCorrupted)
        {
            log.error("Header file corrupted");                            
            return false;
        }
        
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(fileName, "rw");
        } catch (FileNotFoundException ex) {
            log.error("Cannot open header file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        long fileSize;
        try {  
            fileSize = aFile.length();
                    
        } catch (IOException ex) {
            log.error("Cannot get header file size " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        if(fileSize < block.getHeight()*FULL_BLOCK_HASHES_SIZE)
        {
            log.error("Cannot store block, header file too small: block " + block.getHeight() + ", file size " + fileSize);                
            return false;            
        }
        
        try {
            aFile.seek(block.getHeight()*FULL_BLOCK_HASHES_SIZE);
        } catch (IOException ex) {
            log.error("Cannot set headers file position " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        try {
            aFile.write(block.getHeader().getHash().getBytes());
        } catch (IOException ex) {
            log.error("Cannot add block to header file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Cannot close header file " + ex.getClass().getName() + " " + ex.getMessage());                
            return false;
        }
        
        return true;
    }
        
    /**
     * 
     * @param height
     * @return hash of the block with specified height
     */
    
    public Sha256Hash getHash(int height)
    {
        Sha256Hash hash=Sha256Hash.ZERO_HASH;
        
        if(isCorrupted)
        {
            log.error("Header file corrupted");                            
            return hash;
        }

        
        RandomAccessFile aFile;
        try {
            aFile = new RandomAccessFile(fileName, "r");
        } catch (FileNotFoundException ex) {
            log.error("Cannot open header file " + ex.getClass().getName() + " " + ex.getMessage());                
            return hash;
        }
                
        long fileSize;
        try {  
            fileSize = aFile.length();
                    
        } catch (IOException ex) {
            log.error("Cannot get header file size " + ex.getClass().getName() + " " + ex.getMessage());                
            return hash;
        }
        
        if(fileSize<(height+1)*FULL_BLOCK_HASHES_SIZE)
        {
            log.error("Cannot read block hash, header file too small: block " + height + ", file size " + fileSize);                
            return hash;
        }
        
        try {
            aFile.seek(height*FULL_BLOCK_HASHES_SIZE);
        } catch (IOException ex) {
            log.error("Cannot set headers file position " + ex.getClass().getName() + " " + ex.getMessage());                
        }
        
        byte[] bytes=new byte[FULL_BLOCK_HASHES_SIZE];
        
        try {
            aFile.read(bytes);
        } catch (IOException ex) {
            log.error("Cannot read block hash from header file " + ex.getClass().getName() + " " + ex.getMessage());                
            return hash;
        }
        
        try {
            aFile.close();
        } catch (IOException ex) {
            log.error("Cannot close header file " + ex.getClass().getName() + " " + ex.getMessage());                
            return hash;
        }

        hash=new Sha256Hash(bytes);
        
        return hash;
    }
    
    
}
