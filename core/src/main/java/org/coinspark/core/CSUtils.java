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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class CSUtils {

    private static final Logger log = LoggerFactory.getLogger(CSUtils.class);
    public enum CSMimeType {

        /* Preferred Common Types */
        APPLICATION_PDF_PDF("application/pdf", ".pdf"),
        APPLICATION_ZIP_ZIP("application/zip", ".zip"),
        IMAGE_GIF_GIF("image/gif", ".gif"),
        TEXT_HTML_HTML("text/html", ".html"),
        IMAGE_JPEG_JPG("image/jpeg", ".jpg"),
        IMAGE_PNG_PNG("image/png", ".png"),
        TEXT_CSV_CSV("text/csv", ".csv"),
        TEXT_JAVASCRIPT_JS("text/javascript", ".js"),
        TEXT_PLAIN_TXT("text/plain", ".txt"),

        /* Other */
        APPLICATION_ARJ_ARJ("application/arj", ".arj"),
        APPLICATION_HLP_HLP("application/hlp", ".hlp"),
        APPLICATION_MSWORD_DOC("application/msword", ".doc"),
        APPLICATION_PLAIN_TEXT("application/plain", ".text"),
        APPLICATION_RTF_RTF("application/rtf", ".rtf"),
        APPLICATION_XGZIP_GZ("application/x-gzip", ".gz"),
        APPLICATION_XGZIP_GZIP("application/x-gzip", ".gzip"),
        APPLICATION_XRTF_RTF("application/x-rtf", ".rtf"),
        APPLICATION_XTAR_TAR("application/x-tar", ".tar"),
        IMAGE_BMP_BMP("image/bmp", ".bmp"),
        IMAGE_JPEG_JPE("image/jpeg", ".jpe"),
        IMAGE_JPEG_JPEG("image/jpeg", ".jpeg"),
        IMAGE_PICT_PIC("image/pict", ".pic"),
        IMAGE_PICT_PICT("image/pict", ".pict"),
        IMAGE_PJPEG_JFIF("image/pjpeg", ".jfif"),
        IMAGE_PJPEG_JPE("image/pjpeg", ".jpe"),
        IMAGE_PJPEG_JPEG("image/pjpeg", ".jpeg"),
        IMAGE_PJPEG_JPG("image/pjpeg", ".jpg"),
        IMAGE_PNG_XPNG("image/png", ".x-png"),
        IMAGE_TIFF_TIF("image/tiff", ".tif"),
        IMAGE_TIFF_TIFF("image/tiff", ".tiff"),
        IMAGE_XTIFF_TIF("image/x-tiff", ".tif"),
        IMAGE_XTIFF_TIFF("image/x-tiff", ".tiff"),
        IMAGE_XWINDOWSBMP_BMP("image/x-windows-bmp", ".bmp"),
        TEXT_HTML_HTM("text/html", ".htm"),
        TEXT_HTML_HTMLS("text/html", ".htmls"),
        TEXT_HTML_HTX("text/html", ".htx"),
        TEXT_HTML_SHTML("text/html", ".shtml"),
        TEXT_PLAIN_TEXT("text/plain", ".text"),
        TEXT_RICHTEXT_RT("text/richtext", ".rt"),
        TEXT_RICHTEXT_RTF("text/richtext", ".rtf"),
        TEXT_RICHTEXT_RTX("text/richtext", ".rtx");

        private String type;
        private String extension;

        CSMimeType(String type, String extension) {
            this.type = type;
            this.extension = extension;
        }
        
        /**
         * Get a String representation of this type. e.g. "application/zip"
         * @return
         */
        
        public String getType() {
            return type;
        }

        /**
         * Get the extension for this type. e.g. ".zip"
         * @return
         */
        
        public String getExtension() {
            return extension;
        }

        /**
         * Gets the first matching mime-type for the given type
         * @param type e.g. "application/zip"
         * @return The MimeType that matched the given type
         */
        
        public static CSMimeType fromType(String type) {
            if (type != null) {
                type = type.trim().toLowerCase();
                for (CSMimeType mt : CSMimeType.values()) {
                    if (mt.getType().equals(type)) {
                        return mt;
                    }
                }
            }

            return null;
        }

        /**
         * Gets the first matching mime-type for the given extension
         * @param extension e.g. ".zip"
         * @return The MimeType that matched the given extension
         */
        
        public static CSMimeType fromExtension(String extension) {
            if (extension != null) {
                extension = extension.trim().toLowerCase();
                for (CSMimeType mt : CSMimeType.values()) {
                    if (mt.getExtension().equals(extension)) {
                        return mt;
                    }
                }
            }

            return null;
        }
    }
    
    public static void copyArray(byte[] dest,int off,byte[] src)
    {
        System.arraycopy(src, 0, dest, off, src.length);
    } 

    public static  int codedSize(int Size,int Code)
    {
        return (Size & 0xFFFFFF) | (Code << 24); 
    }
    
    public static  void littleEndianByteArray(int n,byte[] arr,int off)
    {
        int k=n;
        for(int i=0;i<4;i++)
        {
            arr[off+i]=(byte)(k%256);
            k/=256;
        }
    }

    public static  void littleEndianByteArray(BigInteger n,byte[] arr,int off)
    {
        if(n == null)
        {
            return;
        }
        byte[] k=n.toByteArray();
        for(int i=0;i<k.length;i++)
        {
            arr[off+i]=k[k.length-i-1];
        }
    }
    
    public static  int littleEndianToInt(byte[] arr,int off)
    {
        int n=0;
        for(int i=0;i<4;i++)
        {
            n=n*256+(arr[off+3-i]&0xFF);
        }
        return n;
    }

    public static  BigInteger littleEndianToBigInteger(byte[] arr,int off)
    {
        byte[] k=new byte[8];        
        for(int i=0;i<8;i++)
        {
            k[i]=arr[off+8-i-1];
        }
        return new BigInteger(k);
    }

    public static String date2iso8601(Date d)
    {
        if(d == null)
        {
            return null;
        }
        
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        df.setTimeZone(tz);
        
        return df.format(d);
    }
    
    public static Date iso86012date(String s)
    {
        if(s == null)
        {
            return null;
        }
        
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        try {
            return df.parse(s);
        } catch (ParseException ex) {
        }
        
        return null;        
    }
    
    public static byte[] hex2Byte(String str)
    {
        if (str == null){
            return null;
        }
            

        byte[] bytes = new byte[str.length() / 2];
        for (int i = 0; i < bytes.length; i++)
        {
            bytes[i] = (byte) Integer
                    .parseInt(str.substring(2 * i, 2 * i + 2), 16);
        }
        return bytes;
    }
    
    public static String byte2Hex(byte[] b)
    {
        // String Buffer can be used instead
        String hs = "";

        for (int n = 0; n < b.length; n++)
        {
            String stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));

            if (stmp.length() == 1)
            {
                hs = hs + "0" + stmp;
            }
            else
            {
                hs = hs + stmp;
            }

            if (n < b.length - 1)
            {
                hs = hs + "";
            }
        }

        return hs;
    }
    
    public static String addHttpIfMissing(String URL)
    {
        if(URL.indexOf("://")>0)
        {
            return URL;
        }
        
        return "http://" + URL;
    }

    public static boolean readFromFileToBytes(RandomAccessFile aFile,byte [] raw)
    {
        int offset=0;       
        int bytesRead=0;
        try {
            while((offset<raw.length) && (bytesRead=aFile.read(raw,offset,raw.length-offset))>=0)
            {
                offset+=bytesRead;
            }
        } catch (IOException ex) {
            return false;
        }
        return true;
    }

    public static CSDownloadedURL getURL(String URLString,int Timeout,String FileNamePrefix)
    {
        CSDownloadedURL reader=new CSUtils().new CSDownloadedURL(URLString,Timeout,FileNamePrefix);
        reader.method="GET";
        reader.read();
        return reader;
    }
    
    public static CSDownloadedURL postURL(String URLString,int Timeout,String FileNamePrefix,String Request,Map <String,String> AdditionalHeaders)
    {
        CSDownloadedURL reader=new CSUtils().new CSDownloadedURL(URLString,Timeout,FileNamePrefix);
        reader.method="POST";
        reader.postRequest=Request;
        reader.additionalHeaders=AdditionalHeaders;
        reader.read();
        return reader;
    }
    
    
    
    public class CSDownloadedURL
    {        
        public String urlString;
        public CSMimeType mimeType;
        public String fileNamePrefix;
        public String fileName;
        public int timeout;
        public int timeoutConnect;
        public URL url;
        public String contents="";
        public String header;
        public String error;
        public int size=0;
        public String method;
        public String postRequest="";
        public int responseCode=0;
        public String ResponseMessage="";
        public Map <String,String> additionalHeaders=new HashMap<String, String>();
        
        
        public CSDownloadedURL(String URLString,int Timeout,String FileNamePrefix)
        {
            urlString=URLString;
            timeout=Timeout*1000;
            timeoutConnect=15000;                    
            fileNamePrefix=FileNamePrefix;
        }
        
        public CSDownloadedURL(String URLString,int Timeout)
        {
            urlString=URLString;
            timeout=Timeout*1000;
            timeoutConnect=15000;                    
            fileNamePrefix=null;
        }

        private String debugMessageHeader(long startTime)
        {
            return "!!Download!! " + startTime + ": " + (new Date().getTime() - startTime) + " - ";
        }
        
        public boolean read()
        {            
            HttpURLConnection connection=null;
            long startTime=new Date().getTime();
            error=null;
            
            try {
                if( (urlString == null) || urlString.isEmpty())
                {
                    throw new Exception("Empty URL string");
                }
        
                url = new URL(urlString);
                

                connection = (HttpURLConnection)url.openConnection();
                connection.setConnectTimeout(timeoutConnect);
                connection.setReadTimeout(timeout);
                
                connection.setRequestMethod(method);
                if(method.equals("POST"))
                {
                    connection.setDoOutput(true);
                    additionalHeaders.put("Content-Length",Integer.toString(postRequest.length()));
                    for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) 
                    {
                        connection.setRequestProperty(entry.getKey(),entry.getValue());
                    }        
                    
                    OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
                    writer.write(postRequest);
                    writer.flush();                
                    writer.close();
                }
                
                InputStream reader=connection.getInputStream();
                
                
                RandomAccessFile aFile = null;
                StringBuilder stringBuilder=null;
                
                responseCode=connection.getResponseCode();
                
                if(fileNamePrefix != null)                                      // mime type is returned only for files
                {
                    mimeType = CSUtils.CSMimeType.fromType(connection.getHeaderField("Content-Type"));            
                    if(mimeType == null)
                    {
                        throw new Exception("Cannot connect to server");
                    }
                    
                    fileName=fileNamePrefix+mimeType.getExtension();
                    File bdbFile = new File(fileName);

                    if(bdbFile.exists())
                    {
                        if(!bdbFile.delete())
                        {
                            throw new Exception("Cannot delete existing file");
                        }            
                    }

                    aFile = new RandomAccessFile(fileName, "rw");
                }
                else
                {
                    stringBuilder=new StringBuilder(4096);
                }
                
                
                byte [] buf=new byte[4096];
                int len;
                size=0;
                while ((len=reader.read(buf, 0, 4096))>=0)
                {
                    if(len>0)
                    {
                        if(aFile != null)
                        {
                            aFile.write(Arrays.copyOf(buf, len));                                    
                        }
                        if(stringBuilder != null)
                        {
                            stringBuilder.append(new String(Arrays.copyOf(buf, len)));                    
                        }
                        size+=len;
                    }
                }
                reader.close();
                if(stringBuilder != null)
                {
                    contents=stringBuilder.toString();                    
                }
            } 
            catch (Exception ex) 
            {
                if(connection != null)
                {
                    try {
                        responseCode=connection.getResponseCode();
                    } catch (IOException ex1) {
                    }
                }
                
                if(responseCode > 0)
                {
                    ResponseMessage="Network request: " + urlString + " - FAILURE, Response code: " + responseCode;
                }
                else
                {
                    ResponseMessage="Network request: " + urlString + " - FAILURE, Error: " + ex.getMessage();                    
                }
                log.error(ResponseMessage);
                error=ex.getClass().getName() + " " + ex.getMessage();
                return false;
            }
            
            if(responseCode != 200)
            {
                ResponseMessage="Network request: " + urlString + " - FAILURE, Response code: " + responseCode;
            }
            else
            {
                ResponseMessage="Network request: " + urlString + " - SUCCESS, Content size: " + size;
            }
            log.info(ResponseMessage);
            return true;
        }
        
        public boolean readHttpNonBlocking()
        {            
            long startTime=new Date().getTime();
            long timeNow;
            error=null;
            
            try
            {
                if( (urlString == null) || urlString.isEmpty())
                {
                    throw new Exception("Empty URL string");
                }
        
                url = new URL(urlString);
                RandomAccessFile aFile = null;
                StringBuilder stringBuilder=null;
                if(fileNamePrefix != null)                                      // mime type is returned only for files
                {
                    final URLConnection urlConnection = url.openConnection();

                    mimeType = CSUtils.CSMimeType.fromType(urlConnection.getHeaderField("Content-Type"));            
                    if(mimeType == null)
                    {
                        throw new Exception("Cannot connect to server");
                    }
                    
                    fileName=fileNamePrefix+mimeType.getExtension();
                    File bdbFile = new File(fileName);

                    if(bdbFile.exists())
                    {
                        if(!bdbFile.delete())
                        {
                            throw new Exception("Cannot delete existing file");
                        }            
                    }

                    aFile = new RandomAccessFile(fileName, "rw");
                }
                else
                {
                    stringBuilder=new StringBuilder(4096);
                }
                
                int port=url.getPort();
                if(port<=0)
                {
                    port=url.getDefaultPort();
                }
                

                SocketAddress remoteAddress = new InetSocketAddress(url.getHost(), port);            

                SocketChannel channel = SocketChannel.open();
                channel.configureBlocking(false);
                channel.connect(remoteAddress);
                
                while (!channel.finishConnect()) 
                {
                    timeNow=new Date().getTime();
                    if(timeNow > startTime + timeout)
                    {
                        throw new Exception("Connection timeout");
                    }
                }


                String additionalHeadersString="";
                if(method.equals("POST"))
                {
                    additionalHeaders.put("Content-Length",Integer.toString(postRequest.length()));
                    for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) 
                    {
                        additionalHeadersString+=entry.getKey() + ": " + entry.getValue() + "\r\n";
                    }        
                }

                String pathAndQuery=url.getPath();
                if((pathAndQuery == null) || pathAndQuery.isEmpty())
                {
                    pathAndQuery = "/";
                }
                if(url.getQuery() != null && !url.getQuery().isEmpty())
                {
                    pathAndQuery += "?" + url.getQuery();
                }
                
                String request = method + " " + pathAndQuery + 
                        " HTTP/1.0\r\n" + 
                        "Connection: close\r\n" + 
                        additionalHeadersString + 
                        "Host: " + url.getHost() + "\r\n" + "\r\n";

                if(method.equals("POST"))
                {
                    request += postRequest;
                }

                ByteBuffer requestBuffer = ByteBuffer.wrap(request.getBytes("ISO-8859-1"));
                
                
                while(requestBuffer.remaining() > 0)
                {
                    channel.write(requestBuffer);
                    timeNow=new Date().getTime();
                    if(timeNow > startTime + timeout)
                    {
                        throw new Exception("Request timeout");
                    }
                }

                
                header = "";
                size=0;
                boolean readingHTTPHeader=true;

                ByteBuffer buffer=ByteBuffer.allocate(4096);
                int bytes_read=channel.read(buffer);
                byte [] tail=new byte[0];
                        
                while(bytes_read >= 0)
                {
                    if(bytes_read > 0)
                    {
                        byte [] bytes=new byte[bytes_read+tail.length];
                        if(tail.length > 0)
                        {
                            System.arraycopy(tail, 0, bytes, 0, tail.length);
                        }                        
                        System.arraycopy(buffer.array(), 0, bytes, tail.length, bytes_read);
                        tail=new byte[0];
                        
                        if(readingHTTPHeader)
                        {
                            int endOfHeader=-1;
                            for(int i=0;i<bytes.length-4;i++)
                            {
                                if( (bytes[i] == 0x0d) && 
                                    (bytes[i+1] == 0x0a) &&
                                    (bytes[i+2] == 0x0d) &&
                                    (bytes[i+3] == 0x0a) )
                                {
                                    endOfHeader=i;
                                    break;
                                }                                                
                            }
                            if(endOfHeader >= 0)                                // If header exactly between chunks it will be missed
                            {
                                readingHTTPHeader=false;
                                header += new String(Arrays.copyOfRange(bytes, 0, endOfHeader));   
                                if(endOfHeader+4 < bytes.length)
                                {
                                    bytes=Arrays.copyOfRange(bytes, endOfHeader+4,bytes.length);
                                }
                                else
                                {
                                    bytes=null;
                                }
                            }
                            else
                            {
                                if(bytes.length>4)
                                {
                                    header += new String(Arrays.copyOf(bytes, bytes.length-4));                                    
                                    tail=Arrays.copyOfRange(bytes, bytes.length-4, bytes.length);
                                }
                                else
                                {
                                    tail=Arrays.copyOfRange(bytes, 0,bytes.length);
                                }
                            }
                        }

                        if(!readingHTTPHeader && (bytes != null))               // Doesn't support Transfer-Encode: chunked
                        {
                            if(aFile != null)
                            {
                                aFile.write(bytes);                                    
                            }
                            if(stringBuilder != null)
                            {
                                stringBuilder.append(new String(bytes));
                            }
                            size+=bytes.length;
                        }

                        buffer.clear();
                    }

                    timeNow=new Date().getTime();
                    if(timeNow > startTime + timeout)
                    {
                        throw new Exception("Timeout");
                    }

                    bytes_read=channel.read(buffer);
                }
                

                if(aFile != null)
                {
                    try {
                        aFile.close();
                    } catch (IOException ex) {
                        throw new Exception("Cannot close file " + ex.getClass().getName() + ex.getMessage());
                    }                            
                }
                if(stringBuilder != null)
                {
                    contents=stringBuilder.toString();                    
                }                    
            }
            
            catch (Exception ex)
            {
                error=ex.getClass().getName() + " " + ex.getMessage();
                return false;
            }
                
            return true;
        }
        
    }
}
