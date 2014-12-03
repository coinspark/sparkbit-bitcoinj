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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class CSPDFParser {

    private static final byte[] WHITESPACES = new byte[] {0x20,0x0a,0x0c,0x00,0x09,0x0d};
    private static final byte[] ENDOFLINES = new byte[] {0x0a,0x0d};
    private static final byte[] DELIMITERS = new byte[] {0x28,0x29,0x3c,0x3e,0x5b,0x5d,0x7b,0x7d,0x2f,0x25};
    private static final byte[] DELIMITERSANDWHITESPACES = new byte[] {0x20,0x0a,0x0c,0x00,0x09,0x0a,0x0c,0x0d,0x28,0x29,0x3c,0x3e,0x5b,0x5d,0x7b,0x7d,0x2f,0x25};
    private static final String STREAM = "stream";
    private static final String ENDSTREAM = "endstream";
    private static final String REFERENCE = "R";
    private static final String OBJ = "obj";
    private static final String ENDOBJ = "endobj";
    private static final String LENGTH = "Length";
    
    private boolean inArray(byte [] haystack, byte needle)
    {
        for(byte e : haystack)
        {
            if(e == needle)
            {
                return true;
            }
        }
        return false;
    }

    private boolean isHexadecimalOrWhiteSpace(byte b)
    {
        if(inArray(WHITESPACES, b))
        {
            return (b != 0x00);
        }
        
        if((b >= '0') && (b <='9'))
        {
            return true;
        }

        if((b >= 'a') && (b <='f'))
        {
            return true;
        }
        
        if((b >= 'A') && (b <='F'))
        {
            return true;
        }
        
        return false;
    }
    
    private boolean isNumeric(byte b)
    {
        if((b >= '0') && (b <='9'))
        {
            return true;
        }
        if((b == '+') || (b == '-') || (b == '.'))
        {
            return true;            
        }
        return false;
    }
            
    private int toInt(int Start,int End, int DefaultValue)
    {
        String rawString=toRawString(Start, End);
        
        if(rawString != null)
        {
            try {
                return Integer.parseInt(rawString);
            } catch (NumberFormatException e) {
                return DefaultValue;
            }
        }
        
        return DefaultValue;
    }
    
    private String toRawString(int Start,int End)
    {
        try {
            return new String(raw, Start, End-Start, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            return null;
        }
    }
    
    private int indexOf(byte [] Pattern,int Start,int End)
    {
        if(Start<0)
        {
            Start=0;
        }
        if(End<0)
        {
            End=raw.length;
        }
        for(int i=Start;i<End-Pattern.length;i++)
        {
            boolean found = true;
            for(int j = 0;j < Pattern.length;j++) 
            {
                if(raw[i+j] != Pattern[j])
                {
                    found = false;
                    break;                    
                }
            }
            if(found)
            {
                if(i>0)
                {
                    if(!inArray(DELIMITERSANDWHITESPACES, raw[i-1]))
                    {
                        found=false;
                    }                    
                }                
            }
            if(found)
            {
                if(i+Pattern.length<raw.length)
                {
                    if(!inArray(DELIMITERSANDWHITESPACES, raw[i+Pattern.length]))
                    {
                        found=false;
                    }                    
                }                
            }
            if(found)
            {
                return i;
            }
        }
        return -1;
    }
    
    public enum CSPDFObjectType{
        UNKNOWN,
        STRANGE,
        NAME,
        STRING,
        ARRAY,
        DELIMITER,
        DICTIONARY,
        STREAM,
        NUMBER,
        HEXSTRING,
        KEYWORD,
        OBJECT,
        REFERENCE,
    }
    
    public enum CSPDFObjectEmbedded{
        NONE,
        STRANGE,
        FILE,
        UNCLEAR,
        URL,
    }
    
    private byte [] raw;
    
    public CSPDFParser(byte [] Raw)
    {
        raw=Raw;
    }

    public class CSPDFObject
    {
        public CSPDFObjectType type;
        public int offset;
        public int offsetNext;
        public int streamOffset;
        public int streamEnd;
        
        private CSPDFParser parentParser=null;
        
        
        List<CSPDFObject> children;
        
        public CSPDFObject(CSPDFParser Parser)
        {
            parentParser=Parser;
            
            type=CSPDFObjectType.UNKNOWN;
            offset=-1;
            offsetNext=0;
            streamOffset=-1;
            streamEnd=-1;
            
            children=null;            
        }
        
        public int toInt(int DefaultValue)
        {
            if(type == CSPDFObjectType.NUMBER)
            {
                return parentParser.toInt(offset, offsetNext, DefaultValue);
            }
            
            return DefaultValue;
        }
        
        public String toRawString()
        {
            switch(type)
            {
                case HEXSTRING:
                case STRING:
                    return parentParser.toRawString(offset+1, offsetNext-1);
                case NAME:
                    return parentParser.toRawString(offset+1, offsetNext);
                case KEYWORD:
                case DELIMITER:
                case OBJECT:
                case REFERENCE:
                    return parentParser.toRawString(offset, offsetNext);
            }
            
            return null;
        }
        
        public CSPDFObject dereference()
        {
            if(type != CSPDFObjectType.REFERENCE)
            {
                return null;
            }
            
            String objString=toRawString();
            objString=objString.substring(0, objString.length()-1) + "obj";
            
            int pos=parentParser.indexOf(objString.getBytes(),offset-1000,-1);
            if(pos<0)
            {
                pos=parentParser.indexOf(objString.getBytes(),0,offset-1000);
            }
            if(pos>=0)
            {
                try {
                    return parentParser.getObject(pos+objString.length());
                } catch (Exception ex) {
                    return null;
                }
            }
            
            return null;
        }
        
        @Override
        public String toString()
        {
            return toString("");            
        }
  
        public CSPDFObjectEmbedded hasEmbeddedFileOrURL()
        {
            CSPDFObjectEmbedded result=CSPDFObjectEmbedded.NONE;            
            
            if((type == CSPDFObjectType.STREAM) || (type == CSPDFObjectType.DICTIONARY))
            {
                if(children != null)
                {
                    boolean isKey=true;
                    boolean isFS=false;
                    for(CSPDFObject subObject : children)
                    {
                        if(isKey)
                        {
                            if(subObject.type == CSPDFObjectType.NAME)
                            {
                                if("EF".equals(subObject.toRawString()))
                                {
                                    result=CSPDFObjectEmbedded.FILE;
                                }
                                if("FS".equals(subObject.toRawString()))
                                {
                                    isFS=true;
                                }
                            }                            
                            else
                            {
                                return CSPDFObjectEmbedded.STRANGE;
                            }
                        }
                        else
                        {
                            if(isFS)
                            {
                                if(subObject.type == CSPDFObjectType.NAME)
                                {
                                    if("URL".equals(subObject.toRawString()))
                                    {
                                        return CSPDFObjectEmbedded.URL;
                                    }                                    
                                }                                
                                else
                                {
                                    return CSPDFObjectEmbedded.UNCLEAR;
                                }
                            }
                            else
                            {
                                CSPDFObjectEmbedded subResult=subObject.hasEmbeddedFileOrURL();
                                switch(subResult)
                                {
                                    case STRANGE:
                                    case UNCLEAR:
                                    case URL:
                                        return subResult;
                                    case FILE:
                                        result=subResult;
                                        break;
                                }
                            }
                            
                            isFS=false;
                        }
                        isKey= !isKey;
                    }
                }                
            }
            
            return result;
        }
  
        public String toString(String Prefix)
        {
            String s="";
            s+=Prefix + type + " - " + String.format("(%06x-%06x)",offset,offsetNext) + ": ";
            if(type == CSPDFObjectType.STREAM)
            {
                s+=String.format("(%06x-%06x)",streamOffset,streamEnd);
            }
            else
            {
                if(toRawString() != null)
                {
                    s+=toRawString();
                }
                else
                {
                    if(toInt(-1234567890)!= -1234567890)
                    {
                        s+=toInt(-1234567890);
                    }
                }
            }
            
            if(children != null)
            {
                if(children.size() > 0)
                {
                    s+=" - SubObjects: " + children.size();
                }
                for(CSPDFObject subObject : children)
                {
                    s+="\n" + subObject.toString(Prefix + " ");
                }
            }
            
/*            
            if(type==CSPDFObjectType.REFERENCE)
            {
                CSPDFObject deref=dereference();
                if(deref != null)
                {
                    s+=" --> " + deref.type;
                    if(deref.toRawString() != null)
                    {
                        s+=": " + deref.toRawString();
                    }
                    else
                    {
                        if(deref.toInt(-1234567890)!= -1234567890)
                        {
                            s+=": " + deref.toInt(-1234567890);
                        }
                    }
                }
                else
                {
                    s+=" --> " + "NULL!!!";
                }
            }
            */
            return s;
        }
        
    }    

    private boolean isSpecificKeyword(int Offset,int End,String Keyword) throws UnsupportedEncodingException
    {
        if(Offset+Keyword.length() > raw.length)
        {
            return false;
        }
        
        if(End>=0)
        {
            if(End-Offset != Keyword.length())
            {
                return false;
            }
        }
        
        if(Keyword.equals(new String(raw, Offset, Keyword.length(), "UTF-8")))
        {
            return true;
        }
        
        return false;
    }
    
    private boolean isSpecificKeyword(int Offset,String Keyword) throws UnsupportedEncodingException
    {
        return isSpecificKeyword(Offset, -1, Keyword);
    }
    
    private int isEndOfLine(int Start,boolean AllowSingleCR)
    {
        int offset=Start;
        if((offset<raw.length) && (raw[offset] == 0x0a))
        {
            offset++;
            return offset-Start;
        }
        else
        {
            if((offset<raw.length) && (raw[offset] == 0x0d))
            {
                offset++;
                if((offset<raw.length) && (raw[offset] == 0x0a))
                {
                    offset++;
                    return offset-Start;
                }
                if(AllowSingleCR)
                {
                    return offset-Start;
                }
            }                            
        }
        
        return 0;
    }
    
    public CSPDFObject getObject(int Start) throws Exception
    {
        return getObject(Start,false);
    }
    
    public CSPDFObject getObject(int Start,boolean ignoreReferences) throws Exception
    {
        while((Start<raw.length) && inArray(WHITESPACES,raw[Start]))
        {
            Start++;
        }
        if(Start>=raw.length)
        {
            return null;
        }        
                
        CSPDFObject obj=new CSPDFObject(this);
        CSPDFObject subObject;
        
        int offset=Start;
        obj.offset=Start;
        
        byte thisByte=raw[offset];
        switch(thisByte)
        {
            case '%':                                                           // 0x25
                offset++;                
                while((offset<raw.length) && !inArray(ENDOFLINES,raw[offset]))
                {
                    offset++;
                }
                return getObject(offset); 
            case '/':                                                           // 0x2F
                offset++;
                obj.type=CSPDFObjectType.NAME;
                while((offset<raw.length) && !inArray(DELIMITERSANDWHITESPACES,raw[offset]))
                {
                    offset++;
                }
                obj.offsetNext=offset;
                break;
            case '(':                                                           // 0x28
                offset++;
                obj.type=CSPDFObjectType.STRING;
                int bracket_count=1;
                while((offset<raw.length) && (bracket_count > 0))
                {
                    switch(raw[offset])
                    {
                        case '\\':                                              // 0x5c
                            offset++;
                            break;
                        case '(':                                               // 0x28
                            bracket_count++;
                            break;
                        case ')':                                               // 0x29
                            bracket_count--;
                            break;
                    }
                    offset++;
                }
                if(bracket_count > 0)
                {
                    obj.type = CSPDFObjectType.STRANGE;  
                }
                else
                {
                    obj.offsetNext=offset;
                }
                break;
            case '[':
                offset++;
                obj.type=CSPDFObjectType.ARRAY;
                obj.children=new ArrayList<CSPDFObject>();
                
                subObject=getObject(offset);
                while((subObject != null) && ((subObject.type != CSPDFObjectType.DELIMITER) || (raw[subObject.offset] != ']')))
                {                    
                    obj.children.add(subObject);
                    if(subObject.type == CSPDFObjectType.STRANGE)
                    {
                        subObject=null;
                    }
                    else
                    {
                        offset=subObject.offsetNext;
                        subObject=getObject(offset);
                    }
                }         
                
                if(subObject == null)
                {
                    obj.type = CSPDFObjectType.STRANGE;  
                }                
                else
                {
                    offset=subObject.offsetNext;
                    obj.offsetNext=offset;
                }
                break;
            case ']':
                obj.type=CSPDFObjectType.DELIMITER;
                offset++;
                obj.offsetNext=offset;
                break;
            case '<':
                offset++;
                if((offset<raw.length) && (raw[offset] == '<'))
                {
                    offset++;
                    obj.type=CSPDFObjectType.DICTIONARY;
                    obj.children=new ArrayList<CSPDFObject>();

                    subObject=getObject(offset);
                    while((subObject != null) && ((subObject.type != CSPDFObjectType.DELIMITER) || (raw[subObject.offset] != '>')))
                    {                    
                        obj.children.add(subObject);
                        if(subObject.type == CSPDFObjectType.STRANGE)
                        {
                            subObject=null;
                        }
                        else
                        {
                            offset=subObject.offsetNext;
                            subObject=getObject(offset);
                        }
                    }                
                    if(subObject == null)
                    {
                        obj.type = CSPDFObjectType.STRANGE;  
                    }                
                    else
                    {
                        offset=subObject.offsetNext;
                        if((offset>=raw.length) || (raw[offset] != '>'))
                        {
                            obj.type = CSPDFObjectType.STRANGE;                              
                        }                        
                    }
                    if(obj.type == CSPDFObjectType.DICTIONARY)
                    {
                        offset++;
                        obj.offsetNext=offset;                        
                    }
                    while((offset<raw.length) && inArray(WHITESPACES,raw[offset]))
                    {
                        offset++;
                    }
                    try {
                        if(isSpecificKeyword(offset, STREAM))
                        {
                            obj.type = CSPDFObjectType.STREAM;
                            offset+=STREAM.length();
                        }
                    } catch (UnsupportedEncodingException ex) {
                        obj.type = CSPDFObjectType.STRANGE;
                    }
                    int streamLength=0;
                    if(obj.type == CSPDFObjectType.STREAM)
                    {
                        streamLength=-3;
                        int count=0;
                        for(CSPDFObject child : obj.children)
                        {
                            if((count % 2) == 0)
                            {
                                if(child.type == CSPDFObjectType.NAME)
                                {
                                    if(LENGTH.equals(child.toRawString()))
                                    {
                                        if(streamLength == -3)
                                        {
                                            streamLength=-2;
                                        }
                                        else
                                        {
                                            obj.type = CSPDFObjectType.STRANGE;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                if(streamLength == -2)
                                {
                                    streamLength=-1;
                                    
                                    if(child.type == CSPDFObjectType.NUMBER)
                                    {
                                        streamLength=child.toInt(-1);
                                    }                                    
                                    else
                                    {
                                        if(child.type == CSPDFObjectType.REFERENCE)
                                        {
                                            subObject=child.dereference();
                                            if((subObject != null) && (subObject.type == CSPDFObjectType.NUMBER))
                                            {
                                                streamLength=subObject.toInt(-1);
                                            }                                            
                                        }                                                                           
                                    }
                                }
                            }
                            count++;
                        }
                        
                        if(streamLength<0)
                        {
                            obj.type = CSPDFObjectType.STRANGE;
                        }
                    }
                    if(obj.type == CSPDFObjectType.STREAM)
                    {
                        if(offset+streamLength+2+ENDSTREAM.length() > raw.length)
                        {
                            obj.type = CSPDFObjectType.STRANGE;
                        }
                    }
                    if(obj.type == CSPDFObjectType.STREAM)
                    {
                        int eolSize=isEndOfLine(offset, false);
                        if(eolSize > 0)
                        {
                            offset+=eolSize;
                            obj.streamOffset=offset;
                            obj.streamEnd=offset+streamLength;
                        }
                        else
                        {
                            obj.type=CSPDFObjectType.STRANGE;
                        }                        
                    }
                    if(obj.type == CSPDFObjectType.STREAM)
                    {
                        offset+=streamLength;
/*                        
                        int eolSize=isEndOfLine(offset, true);
                        if(eolSize > 0)
                        {
                            offset+=eolSize;
                        }
                        else
                        {
                            obj.type=CSPDFObjectType.STRANGE;
                        }                        
        */
                                                                                // Sometimes this EOL is included in tLength calculation
                                                                                // If not - skip it
                        while((offset<raw.length) && inArray(WHITESPACES,raw[offset]))
                        {
                            offset++;
                        }
                    }    
                    if(obj.type == CSPDFObjectType.STREAM)
                    {
                        try {
                            if(isSpecificKeyword(offset, ENDSTREAM))
                            {
                                offset+=ENDSTREAM.length();
                                
                                subObject=getObject(offset);
                                if((subObject.type != CSPDFObjectType.KEYWORD) || !ENDOBJ.equals(subObject.toRawString()))
                                {
                                    obj.type=CSPDFObjectType.STRANGE;
                                }
                            }
                            else
                            {
                                obj.type = CSPDFObjectType.STRANGE;                                
                            }
                        } catch (UnsupportedEncodingException ex) {
                            obj.type = CSPDFObjectType.STRANGE;
                        }
                    }
                    if(obj.type == CSPDFObjectType.STREAM)
                    {
                        obj.offsetNext=offset;
                    }                    
                }
                else
                {
                    offset++;
                    obj.type=CSPDFObjectType.HEXSTRING;
                    while((offset<raw.length) && (isHexadecimalOrWhiteSpace(raw[offset])))
                    {
                        offset++;
                    }
                    if((offset<raw.length) && (raw[offset] == '>'))
                    {
                        offset++;
                        obj.offsetNext=offset;
                    }
                    else
                    {
                        obj.type = CSPDFObjectType.STRANGE;                        
                    }                    
                }
                break;
            case '>':
                obj.type=CSPDFObjectType.DELIMITER;
                offset++;
                obj.offsetNext=offset;
                break;
            default:
                if(isNumeric(thisByte))
                {
                    obj.type=CSPDFObjectType.NUMBER;
                    while((offset<raw.length) && isNumeric(raw[offset]))
                    {
                        offset++;
                    }
                    obj.offsetNext=offset;
                    
                    if(!ignoreReferences)
                    {
                        subObject=getObject(offset,true);
                        if((subObject != null) && (subObject.type == CSPDFObjectType.NUMBER))
                        {
                            offset=subObject.offsetNext;
                            subObject=getObject(offset,true);
                            if((subObject != null) && (subObject.type == CSPDFObjectType.KEYWORD))
                            {
                                if(OBJ.equals(subObject.toRawString()))
                                {
                                    obj.type=CSPDFObjectType.OBJECT;
                                    obj.offsetNext=subObject.offsetNext;
                                }
                                if(REFERENCE.equals(subObject.toRawString()))
                                {
                                    obj.type=CSPDFObjectType.REFERENCE;
                                    obj.offsetNext=subObject.offsetNext;
                                }
                            }                        
                        }
                    }
                }
                else
                {
                    if(inArray(DELIMITERS,raw[offset]))
                    {
                        obj.type=CSPDFObjectType.DELIMITER;
                        offset++;
                        obj.offsetNext=offset;
                    }
                    else
                    {
                        
                        obj.type=CSPDFObjectType.KEYWORD;
                        while((offset<raw.length) && !inArray(DELIMITERSANDWHITESPACES,raw[offset]))
                        {
                            if((raw[offset]<=0x20) || (raw[offset]>0x7E))
                            {
                                obj.type=CSPDFObjectType.STRANGE;
                            }
                            offset++;
                        }
                        obj.offsetNext=offset;                    
                        
                    }
                }
                break;
        }
        

        if(obj.offsetNext <= obj.offset)
        {
            obj.type=CSPDFObjectType.STRANGE;
        }
        
        return obj;
    }
}
