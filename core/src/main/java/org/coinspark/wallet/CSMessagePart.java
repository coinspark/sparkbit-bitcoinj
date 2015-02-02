/*
 * Copyright 2015 Coin Sciences Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.coinspark.wallet;

import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * CSMessagePart represents a messaging attachment which is persisted to database.
 */
@DatabaseTable(tableName = "message_parts")
public class CSMessagePart implements Comparable<CSMessagePart> {

    public static final String GENERATED_ID_FIELD_NAME = "auto_id";
    public static final String MESSAGE_ID_FIELD_NAME = "message_id";
    public static final String PART_ID_FIELD_NAME = "part_id";
    public static final String MIME_TYPE_FIELD_NAME = "mimetype";
    public static final String FILE_NAME_FIELD_NAME = "filename";
    public static final String CONTENT_SIZE_FIELD_NAME = "size";
    public static final String CONTENT_FIELD_NAME = "content";

    // Auto-generated id sequence needed by Dao
    @DatabaseField(generatedId = true, columnName=GENERATED_ID_FIELD_NAME)
    public long id;

    @DatabaseField(foreign=true, columnName=MESSAGE_ID_FIELD_NAME, canBeNull = false, uniqueCombo=true)
    public CSMessage message;

    @DatabaseField(columnName=PART_ID_FIELD_NAME, canBeNull = false, uniqueCombo=true)
    public int partID;
    
    @DatabaseField(columnName = MIME_TYPE_FIELD_NAME, canBeNull = true)
    public String mimeType;

    @DatabaseField(columnName = FILE_NAME_FIELD_NAME, canBeNull = true)
    public String fileName;

    // Is this required or can it be derived from Blob?
    @DatabaseField(columnName = CONTENT_SIZE_FIELD_NAME, canBeNull = false)
    public int contentSize;

    // columnDefinition overrides canBeNull=false so must explicitly specify NOT NULL
//    @DatabaseField(columnName = CONTENT_FIELD_NAME, columnDefinition = "LONGBLOB not null", dataType=DataType.BYTE_ARRAY)
//    public byte[] content;
    
    /**
     * Compare based on txid first and then part ID.
     * @param other
     * @return 
     */
    @Override
    public int compareTo(CSMessagePart other){
	int r1 = this.message.getTxID().compareTo(other.message.getTxID());
	if (r1 != 0) {
	    return r1;
	}
	// txid is the same, let's just compare by part number.
	int p = other.partID;
	if (partID < p) {
	    return -1;
	} else if (partID > p) {
	    return 1;
	}
	return 0;
    }	
    

    CSMessagePart(int partID, String MimeType, String FileName, int ContentSize) {
	this.partID = partID;
	mimeType = MimeType;
	fileName = FileName;
	contentSize = ContentSize;
    }

    CSMessagePart() {
    }
}
