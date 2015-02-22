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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.commons.lang3.ArrayUtils;
import org.coinspark.protocol.CoinSparkMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.validator.routines.InetAddressValidator;


public class CSUtils {

    private static final Logger log = LoggerFactory.getLogger(CSUtils.class);
    
    public enum CSServerError {

        NOERROR(                                         0),

        UNKNOWN(                                         1),
        
        RESPONSE_NOT_OBJECT(                           100),
        RESPONSE_WRONG_ID(                             101),
        RESPONSE_WRONG_METHOD(                         102),
        RESPONSE_PARSE_ERROR(                          103),
        RESPONSE_RESULT_NOT_FOUND(                     104),
        RESPONSE_RESULT_NOT_OBJECT(                    105),
        RESPONSE_INVALID(                              106),
        RESPONSE_HASH_MISMATCH(                        107),
        
        CANNOT_SIGN(                                   150),
        
        METADATA_ENCODE_ERROR(                         160),
        
        SERVER_NOT_FOUND(                              190),

        
        
        SERVER_REDIRECT(                               300),
        SERVER_HTTP_ERROR(                             400),
        SERVER_FATAL_ERROR(                            500),
        SERVER_CANNOT_CONNECT(                         501),        
        
        
        PARSE_ERROR(                                -32700),
        INVALID_REQUEST(                            -32600),
        METHOD_NOT_FOUND(                           -32601),
        INVALID_PARAMS(                             -32602),
        INTERNAL_ERROR(                             -32603),    
        ASSET_NOT_FOUND(                            -10000),
        TXOUT_NOT_FOUND(                            -10001),    
        SENDER_NOT_ACCEPTED(                        -11000),
        SENDER_IS_SUSPENDED(                        -11001),
        SENDER_NETWORK_NOT_ACCEPTABLE(              -11002),
        SENDER_IP_NOT_ACCEPTED(                     -11003),
        SENDER_IP_IS_SUSPENDED(                     -11004),    
        NO_PUBLIC_MESSAGES(                         -11010),
        ONLY_PUBLIC_MESSAGES(                       -11011),    
        TOO_MANY_RECIPIENTS(                        -11020),
        RECIPIENT_NOT_ACCEPTED_ON_CREATE(           -11021),
        RECIPIENT_IS_SUSPENDED_ON_CREATE(           -11022),
        RECIPIENT_IP_NOT_ACCEPTED_ON_CREATE(        -11023),
        RECIPIENT_IP_IS_SUSPENDED_ON_CREATE(        -11024),    
        DURATION_NOT_ACCEPTABLE(                    -11030),    
        SALT_NOT_ACCEPTABLE(                        -11040),    
        TOO_MANY_MESSAGE_PARTS(                     -11050),
        TOTAL_MESSAGE_TOO_LARGE(                    -11051),
        MIME_TYPE_NOT_ACCEPTABLE(                   -11052),
        FILE_NAME_NOT_ACCEPTABLE(                   -11053),
        CONTENT_TOO_LARGE(                          -11054),   
        CONTENT_MISMATCH(			    -11055),   
        TXID_INVALID(                               -11080),    
        TX_MESSAGE_UNKNOWN(                         -12000),
        TX_MESSAGE_PENDING(                         -12001),
        TX_MESSAGE_EXPIRED(                         -12002),    
        RECIPIENT_NOT_ACCEPTED(                     -12010),
        RECIPIENT_IS_SUSPENDED(                     -12011),
        RECIPIENT_NETWORK_NOT_ACCEPTABLE(           -12012),
        RECIPIENT_IP_NOT_ACCEPTED(                  -12013),
        RECIPIENT_IP_IS_SUSPENDED(                  -12014),

        NONCE_NOT_FOUND(                            -13000),

        SIGNATURE_INCORRECT(                        -13010),
	PUBKEY_INCORRECT(			    -13011),
	PUBKEY_ADDRESS_MISMATCH(		    -13012);
        
        private int code;

        CSServerError(int Code) {
            this.code = Code;
        }
        
        public int getCode() {
            return code;
        }

        public static CSServerError fromCode(int Code) {
                for (CSServerError se : CSServerError.values()) {
                    if (se.getCode() == Code) {
                        return se;
                    }
                }

            return UNKNOWN;
        }
    }
    
    public static String getHumanReadableServerError(int code) {
	CSUtils.CSServerError error = CSUtils.CSServerError.fromCode(code);
	return getHumanReadableServerError(error);
    }
    
    public static String getHumanReadableServerError(CSUtils.CSServerError error) {
	if (error==null) return "";
	switch(error) {
	    case NOERROR:
		return "No error";
	    case UNKNOWN:
		return "Unknown error";
	    case RESPONSE_NOT_OBJECT:
		return "Not a JSON response object";
	    case RESPONSE_WRONG_ID:
		return "Wrong JSON response id";
	    case RESPONSE_WRONG_METHOD:
		return "Wrong JSON response method";
	    case RESPONSE_PARSE_ERROR:
		return "Could not parse JSON response";
	    case RESPONSE_RESULT_NOT_FOUND:
		return "Could not find JSON response result";
	    case RESPONSE_RESULT_NOT_OBJECT:
		return "JSON response not a result object";
	    case RESPONSE_INVALID:
		return "JSON response is invalid";
	    case RESPONSE_HASH_MISMATCH:
		return "Message hash does not match what was encoded in transaction metadata.";
	    case CANNOT_SIGN:
		return "Cannot sign transaction for sending message";
	    case METADATA_ENCODE_ERROR:
		return "Error encoding transaction metadata for message";
	    case SERVER_NOT_FOUND:
		return "Server not found";
	    case SERVER_REDIRECT:
		return "Server redirect error";
	    case SERVER_HTTP_ERROR:
		return "Server returned a HTTP error";
	    case SERVER_FATAL_ERROR:
		return "Server returned a fatal error";
	    case SERVER_CANNOT_CONNECT:
		return "Cannot connect to server";
	    case PARSE_ERROR:
		return "Parse error";
	    case INVALID_REQUEST:
		return "Invalid request";
	    case METHOD_NOT_FOUND:
		return "Method not fonud";
	    case INVALID_PARAMS:
		return "Invalid parameters";
	    case INTERNAL_ERROR:
		return "Internal error";
	    case ASSET_NOT_FOUND:
		return "Asset not found";
	    case TXOUT_NOT_FOUND:
		return "Transaction output not found";
	    case SENDER_NOT_ACCEPTED:
		return "Sender not accepted";
	    case SENDER_IS_SUSPENDED:
		return "Sender is suspended";
	    case SENDER_NETWORK_NOT_ACCEPTABLE:
		return "Sender network not accepted";
	    case SENDER_IP_NOT_ACCEPTED:
		return "Sender IP not accepted";
	    case SENDER_IP_IS_SUSPENDED:
		return "Sender IP is suspended";
	    case NO_PUBLIC_MESSAGES:
		return "Server does not deliver public messages";
	    case ONLY_PUBLIC_MESSAGES:
		return "Server only delivers public messages";
	    case TOO_MANY_RECIPIENTS:
		return "Too many recipients";
	    case RECIPIENT_NOT_ACCEPTED_ON_CREATE:
		return "Recipient not accepted";
	    case RECIPIENT_IS_SUSPENDED_ON_CREATE:
		return "Recipient is suspended";
	    case RECIPIENT_IP_NOT_ACCEPTED_ON_CREATE:
		return "Recipient IP is not accepted";
	    case RECIPIENT_IP_IS_SUSPENDED_ON_CREATE:
		return "Recipient IP is suspended";
	    case DURATION_NOT_ACCEPTABLE:
		return "Duration not accepted";
	    case SALT_NOT_ACCEPTABLE:
		return "Salt not accepted";
	    case TOO_MANY_MESSAGE_PARTS:
		return "Too many message parts";
	    case TOTAL_MESSAGE_TOO_LARGE:
		return "Message is too long";
	    case MIME_TYPE_NOT_ACCEPTABLE:
		return "Mime type not accepted";
	    case FILE_NAME_NOT_ACCEPTABLE:
		return "File name not accpeted";
	    case CONTENT_TOO_LARGE:
		return "Content is too large";
	    case CONTENT_MISMATCH:
		return "Content does not match mimetype";
	    case TXID_INVALID:
		return "Transaction ID is invalid";
	    case TX_MESSAGE_UNKNOWN:
		return "Cannot find message for Transaction ID";
	    case TX_MESSAGE_PENDING:
		return "Message has been found and is pending delivery";
	    case TX_MESSAGE_EXPIRED:
		return "Message has expired";
	    case RECIPIENT_NOT_ACCEPTED:
		return "Recipient not accepted";
	    case RECIPIENT_IS_SUSPENDED:
		return "Recipient has been suspended";
	    case RECIPIENT_NETWORK_NOT_ACCEPTABLE:
		return "Recipient network not accepted";
	    case RECIPIENT_IP_NOT_ACCEPTED:
		return "Recipient's IP address is not accepted";
	    case RECIPIENT_IP_IS_SUSPENDED:
		return "Recipient's IP address has been suspended";
	    case NONCE_NOT_FOUND:
		return "Internal error (nonce not found)";
	    case SIGNATURE_INCORRECT:
		return "Signature is not correct";
	    case PUBKEY_INCORRECT:
		return "Pubkey does not contain valid bitcoin public key in hexadecimal";
	    case PUBKEY_ADDRESS_MISMATCH:
		return "Pubkey does not match sender address";
	    default:
	}
	return "Unknown error";
    }

    
    
    
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
    
    public static  void littleEndianByteArray(long n,byte[] arr,int off)
    {
        long k=n;
        for(int i=0;i<8;i++)
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

    public static long littleEndianToLong(byte[] arr,int off)
    {
        long n=0;
        for(int i=0;i<8;i++)
        {
            n=n*256+(arr[off+7-i]&0xFF);
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
    
    /**
     * Return a new copy of the delivery servers, in random order,
     * with http:// protocol apended if a protocol does not exist.
     * @param servers Array of server URLs
     * @return Array of server URLs
     */
    public static String[] getRandomizedHTTPDeliveryServers(String[] servers) {
	int count = servers.length;
	if (count==0) return new String[0];
	
	String[] result = Arrays.copyOf(servers, count);
	for (int i = 0; i < result.length; i++) {
	    result[i] = addHttpIfMissing(result[i]);
	}
	
	// Shuffle underlying array in-place
	Collections.shuffle(Arrays.asList(result));
	return result;
    }
    
    @Deprecated
    public static void shuffleArray(String [] arr)
    {
        Random rnd = new Random();
        for (int i = arr.length - 1; i > 0; i--)
        {
            int index = rnd.nextInt(i + 1);
            String a = arr[index];
            arr[index] = arr[i];
            arr[i] = a;
        }
    }
    
    @Deprecated
    public static String [] getDeliveryServersArray(String [] source)
    {
        shuffleArray(source);
        String [] result;
        result=new String[2*source.length];
        
        int count=0;
        for(String urlString : source)
        {
            URL url;
            try {
                url = new URL(addHttpIfMissing(urlString));
                InetAddress address = InetAddress.getByName(url.getHost());
                if(address != null)
                {
                    String ip=address.getHostAddress();
                    result[count]="http://";
                    if("https".equals(url.getProtocol()))
                    {
                        result[count]="https://";
                    }
                    result[count]+=ip;
                    if(url.getPath().length()>0)
                    {
                        result[count]+=url.getPath();
                    }
                    count++;
                }
            } catch (MalformedURLException ex) {
            } catch (UnknownHostException ex) {
            }
            result[count]=urlString;
            count++;            
        }
        
        if(count==0)
        {
            return result; 
        }
        
        return Arrays.copyOf(result, count);
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

    public static CSDownloadedURL postMessagingURL(String URLString,int Timeout,String FileNamePrefix,String Request,Map <String,String> AdditionalHeaders) {
	return postURL_impl(URLString, Timeout, FileNamePrefix, Request, AdditionalHeaders, true);
    }

    public static CSDownloadedURL postURL(String URLString,int Timeout,String FileNamePrefix,String Request,Map <String,String> AdditionalHeaders) {
	return postURL_impl(URLString, Timeout, FileNamePrefix, Request, AdditionalHeaders, false);
    }
		
    public static CSDownloadedURL postURL_impl(String URLString,int Timeout,String FileNamePrefix,String Request,Map <String,String> AdditionalHeaders, boolean convertHostnameToIP)
    {
        CSDownloadedURL reader=new CSUtils().new CSDownloadedURL(URLString,Timeout,FileNamePrefix);
        reader.method="POST";
        reader.postRequest=Request;
        reader.additionalHeaders=AdditionalHeaders;
	reader.convertHostnameToIP = convertHostnameToIP; // for messaging to save bytes if true
        reader.read();
        return reader;
    }

    
    public static boolean setDeliveryServer(String URLString,CoinSparkMessage Message)
    {
        if( (URLString == null) || URLString.isEmpty())
        {
            return false;
        }
        if(!URLString.contains("://"))
        {
            URLString="http://" + URLString;
        }
        
        try {
            URL url = new URL(URLString);
            if("https".equals(url.getProtocol()))
            {
                Message.setUseHttps(true);
            }            
            else
            {
                Message.setUseHttps(false);
            }
            
            String host=url.getHost();
            if(url.getPort()>0)
            {
                host+=":"+url.getPort();
            }
            Message.setServerHost(host);
            
            String path=url.getPath();
            if((path.length()>"coinspark/".length()) && ("coinspark/".equals(path.substring(0, "coinspark/".length()))))
            {
                Message.setUsePrefix(true);
                path=path.substring("coinspark/".length());
            }
            else
            {
                Message.setUsePrefix(false);
            }
            
            path=path.trim();
            int from=0;
            int to=path.length();
            while((from<to) && path.substring(from,from+1).equals("/"))
            {
                from++;
            }
            while((from<to) && path.substring(to-1,to).equals("/"))
            {
                to--;
            }
            Message.setServerPath(path.substring(from,to));
            
        } catch (MalformedURLException ex) {
            return false;
        }
        
        return true;
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
        public boolean convertHostnameToIP = false; // if true, will convert hostname to IPv4
	public String originalHostname; // original hostname, if we converted to IPv4
	private boolean isAlreadyIPAddress; // true if original hostname was already an IPv4 address
        
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
            long startTime=new Date().getTime();
            error=null;
            InputStream reader=null;
            
            try {
                if( (urlString == null) || urlString.isEmpty())
                {
                    throw new Exception("Empty URL string");
                }

                if(!urlString.contains("://"))
                {
                    urlString="http://" + urlString;
                }

		/*
		 If required, convert hostname to an IPv4 address
		 */
		try {
		    URI uri = new URI(urlString);
		    String host = uri.getHost();
		    if (host!=null) {
			this.isAlreadyIPAddress = InetAddressValidator.getInstance().isValidInet4Address(host);
			if (this.isAlreadyIPAddress) {
			    this.convertHostnameToIP = false;
			}			
		    }
		} catch (URISyntaxException e) {
		}

		if (this.convertHostnameToIP) {
		    try {
			URI uri = new URI(urlString);
			String host = uri.getHost();
			this.originalHostname = host; // custom host verifier will check cert name against this
			InetAddress address = InetAddress.getByName(host);
			String hostIP = address.getHostAddress();
			URI newUri = new URI(uri.getScheme(), uri.getUserInfo(), hostIP, uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
			this.urlString = newUri.toString();
		    } catch (URISyntaxException use) {
			this.convertHostnameToIP = false;
		    } catch (UnknownHostException uhe) {
			this.convertHostnameToIP = false;
		    }
		}
		// End: converting hostname to IPv4

                url = new URL(urlString);
            } 
            catch (Exception ex) 
            {
                ResponseMessage="Network request: " + urlString + " - FAILURE, Error: " + ex.getMessage();                    
                log.error(ResponseMessage);
                error=ex.getClass().getName() + " " + ex.getMessage();
            } 
            

                
                
            if("https".equals(url.getProtocol()))
            {
                HttpsURLConnection connection=null;
                try
                {

                    connection = (HttpsURLConnection)url.openConnection();
		    
		    /*
		     If the hostname has been converted to an IP address, set a custom
		     host verifier, and check that the original hostname matches either
		     the Common Name (CN) or Subject Alternative Name (SAN) of the cert.
		     TODO: There is a small risk that the server IP may have changed
		     at time of message retrieval.  In future we will add an option
		     so that the message delivery server can indicate whether or not
		     connecting via an IP address is allowed or not.
		     */
		    if (this.convertHostnameToIP) {
			HostnameVerifier myVerifier = new HostnameVerifier() {
			    @Override
			    public boolean verify(String hostname, SSLSession session) {
//				log.debug(">>>> original host     = " + originalHostname);
//				log.debug(">>>> connected to host = " + hostname);
//				log.debug(">>>> SSLSession        = " + session);

				Set<String> namesFound = new HashSet<String>();
				try {
				    Certificate[] peerCertificates = session.getPeerCertificates();

				    if (peerCertificates.length > 0 && peerCertificates[0] instanceof X509Certificate) {
					X509Certificate peerCertificate = (X509Certificate) peerCertificates[0];
					String cn = peerCertificate.getSubjectX500Principal().getName();
					// Example value of cn: CN=msg1.coinspark.org,OU=PositiveSSL Multi-Domain,OU=Domain Control Validated
					// if CN, add to list of valid names, and then check against desired name before we used UP instead.
//					log.debug(">>>> common name = " + cn);
					try {
					    // Creative use of LDAP library... via http://stackoverflow.com/questions/2914521/how-to-extract-cn-from-x509certificate-in-java
					    LdapName ldapDN = new LdapName(cn);
					    for (Rdn rdn : ldapDN.getRdns()) {
//						log.debug("      " + rdn.getType() + " -> " + rdn.getValue());					
						if (rdn.getType().equals("CN")) {
						    namesFound.add(rdn.getValue().toString());
						}
					    }
					} catch (InvalidNameException ine) {
					}
					//log.debug(">>>> peerCertificate toString() = " + peerCertificate.toString());
					try {
					    // From Javadoc: Each entry is a List whose first entry is an Integer (the name type, 0-8) and whose second entry is a String or a byte array (the name, in string or ASN.1 DER encoded form, respectively).
					    for (List<?> item : peerCertificate.getSubjectAlternativeNames()) {
						Integer type = (Integer) item.get(0);
						if (type.intValue() == 2) { // 2 = dNSName, 7 = iPAddress (OCTET STRING)
						    String dNSName = (String) item.get(1);
						    namesFound.add(dNSName);
						}
					    }
					    //log.debug(">>>> sans = " + Arrays.toString(sans));
					} catch (CertificateParsingException e) {
					    // error, no SAN to add to list of found names
					}
				    }
				} catch (SSLPeerUnverifiedException ex) {
				    // Error with SSL connection, reject.
				    ex.printStackTrace();
				    return false;
				}
//				log.debug(">>>> Cert Names Found = " + ArrayUtils.toString(namesFound));
				return namesFound.contains(originalHostname);
			    }
			};
			connection.setHostnameVerifier(myVerifier);
		    }
		    else {
			// Don't convert hostname to IP address.  If the hostname
			// is an IP address, do allow the if SSL cert name to differ.
			// If the hostname is a string, proceed as normal with default behaviour.
			if (this.isAlreadyIPAddress) {
			    HostnameVerifier myVerifier = new HostnameVerifier() {
				@Override
				public boolean verify(String hostname, SSLSession session) {
//				    log.debug(">>>> original host     = " + originalHostname);
//				    log.debug(">>>> connected to host = " + hostname);
				    return InetAddressValidator.getInstance().isValidInet4Address(hostname);
				}
			    };
			    connection.setHostnameVerifier(myVerifier);
			}
		    }

		    
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

                    reader=connection.getInputStream();
                    responseCode=connection.getResponseCode();
                    mimeType = CSUtils.CSMimeType.fromType(connection.getHeaderField("Content-Type"));            
                    if(mimeType == null)
                    {
                        throw new Exception("Cannot connect to server");
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
                } 
            }
            else
            {
                HttpURLConnection connection=null;
                try {
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

                    reader=connection.getInputStream();
                    responseCode=connection.getResponseCode();
                    mimeType = CSUtils.CSMimeType.fromType(connection.getHeaderField("Content-Type"));            
                    if(mimeType == null)
                    {
                        throw new Exception("Cannot connect to server");
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
                } 
            }   
                        
            try
            {
                if(reader == null)
                {
                    throw new Exception("Cannot connect to server");                    
                }
                RandomAccessFile aFile = null;
                StringBuilder stringBuilder=null;
                
                
                if(fileNamePrefix != null)                                      // mime type is returned only for files
                {                    
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
