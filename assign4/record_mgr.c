/*
Assignment 3 : CS525 - Advanced Database Organization 

Date: April 11, 2021.

@author: Group 18
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

//record manager associated with every table
//used to keep track of table information
typedef struct RM_Table
{
	BM_PageHandle page_handle;	
	BM_BufferPool buffer_pool;
	RID record_id; //combination of page and slot number
	Expr *constraint;
	int tuples; //number of tuples in table
	int empty_loc;
	int scanCount;
}RM_Table;

const int MAX_PAGES = 200;
const int ATTRIBUTE_SIZE = 14; 
RC attrOffset (Schema *schema, int attrNum, int *result);

RM_Table *rm_table;


int getEmptyLoc(char *data, int recordSize)
{
	int slots = PAGE_SIZE / recordSize; 
	int i=0;
	while(i<slots){
		if (data[i * recordSize] != '&')
			return i;
		i += 1;
	}
	return -1;

}


extern RC initRecordManager (void *mgmtData)
{
	// Initiliazing Storage Manager
	printf("\ninitRecordManager [executing...]\n");
	initStorageManager();
	printf("\ninitRecordManager [done]\n");
	return RC_OK;
}

extern RC shutdownRecordManager ()
{
	printf("\nshutdownRecordManager [executing...]\n");
	rm_table = NULL;
	free(rm_table);
	printf("\nshutdownRecordManager [done]\n");
	return RC_OK;
}

extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    
    // creates a schema with given attributes for a relation

    printf("\ncreateSchema [executing...]\n");

    Schema *schema = (Schema *)malloc(sizeof(Schema));

    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->keyAttrs = keys;
    schema->keySize = keySize;
    schema->numAttr = numAttr;
    schema->typeLength = typeLength;

    printf("\ncreateSchema [done]\n");
    
    return schema;
    
}


extern RC createTable (char *name, Schema *schema)
{
	printf("\ncreateTable [executing...]\n");

	rm_table = (RM_Table*) malloc(sizeof(RM_Table));

	initBufferPool(&rm_table->buffer_pool, name, MAX_PAGES, RS_CLOCK, NULL);

	char data[PAGE_SIZE];
	char *page_handle = data;
	 
	int result;
	
	//number of tuples in given table
	*(int*)page_handle = 0; 

	page_handle =page_handle+ sizeof(int);
	
	*(int*)page_handle = 1;

	page_handle =page_handle+ sizeof(int);

	*(int*)page_handle = schema->numAttr;

	page_handle =page_handle+ sizeof(int); 

	*(int*)page_handle = schema->keySize;

	page_handle =page_handle+ sizeof(int);
	
	int k=0;

	while(k< schema->numAttr){
		strncpy(page_handle, schema->attrNames[k], ATTRIBUTE_SIZE);
		page_handle =page_handle+ ATTRIBUTE_SIZE;

		*(int*)page_handle = (int)schema->dataTypes[k];

		page_handle =page_handle+ sizeof(int);

		*(int*)page_handle = (int) schema->typeLength[k];

		page_handle =page_handle+ sizeof(int);
		k += 1;
	}

	SM_FileHandle fileHandle;

	createPageFile(name);
	openPageFile(name, &fileHandle);
	writeBlock(0, &fileHandle, data);

	printf("\ncreateTable [done]\n");
	return RC_OK;
}

extern RC openTable (RM_TableData *rel, char *name)
{
	printf("\nopenTable [executing...]\n");
	SM_PageHandle page_handle;    
	
	int num_attr, k;
	
	rel->mgmtData = rm_table;
	rel->name = name;
    
	pinPage(&rm_table->buffer_pool, &rm_table->page_handle, 0);
	
	page_handle = (char*) rm_table->page_handle.data;
	
	rm_table->tuples= *(int*)page_handle;
	page_handle =page_handle+ sizeof(int);

	rm_table->empty_loc= *(int*) page_handle;
	page_handle=page_handle+ sizeof(int);
	
	num_attr = *(int*)page_handle;
	page_handle =page_handle+ sizeof(int);
 	
	Schema *schema;

	schema = (Schema*) malloc(sizeof(Schema));
    
	schema->numAttr = num_attr;
	schema->attrNames = (char**) malloc(sizeof(char*) *num_attr);
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *num_attr);
	schema->typeLength = (int*) malloc(sizeof(int) *num_attr);
	
	int num_att = 0;

	while( num_att < num_attr){
		schema->attrNames[num_att]= (char*) malloc(ATTRIBUTE_SIZE);
		num_att = num_att + 1;
	}
	
	int num_atr = 0;
      
	while( num_atr < schema->numAttr){
		strncpy(schema->attrNames[num_atr], page_handle, ATTRIBUTE_SIZE);
		page_handle =page_handle+ ATTRIBUTE_SIZE;
	   
		schema->dataTypes[num_atr]= *(int*) page_handle;
		page_handle =page_handle+ sizeof(int);

		schema->typeLength[num_atr]= *(int*)page_handle;
		page_handle =page_handle+ sizeof(int);
		
		num_atr = num_atr + 1;
	}
	
	rel->schema = schema;	

	printf("\nopenTable [done]\n");
	return RC_OK;
}   
  
extern RC closeTable (RM_TableData *rel)
{
	printf("\ndeleteTable [executing...]\n");
	RM_Table *rm_table = rel->mgmtData;
	shutdownBufferPool(&rm_table->buffer_pool);
	printf("\ndeleteTable [done]\n");
	return RC_OK;
}

extern RC deleteTable (char *name)
{
	printf("\ndeleteTable [executing...]\n");
	destroyPageFile(name);
	printf("\ndeleteTable [done]\n");
	return RC_OK;
}

extern int getNumTuples (RM_TableData *rel)
{
	printf("\ngetNumTuples [executing...]\n");
	RM_Table *rm_table = rel->mgmtData;
	printf("\ngetNumTuples [done]\n");
	return rm_table->tuples;
}

extern RC insertRecord (RM_TableData *rel, Record *record){
    printf("\ninsertRecord [executing...]\n");

    int recordSize = getRecordSize(rel->schema);
    int slotSize = PAGE_SIZE/recordSize;

    char *pageData, *slot;

    RM_Table *rm_table = rel->mgmtData;
    RID *record_id = &record->id;

    
    record_id->page = rm_table->empty_loc;

    pinPage(&rm_table->buffer_pool, &rm_table->page_handle, record_id->page);
    pageData = rm_table->page_handle.data;

    record_id->slot = getEmptyLoc(pageData, recordSize);
    
    while(record_id->slot == -1){

        unpinPage(&rm_table->buffer_pool, &rm_table->page_handle);

        record_id->page = record_id->page + 1;

        pinPage(&rm_table->buffer_pool, &rm_table->page_handle, record_id->page);

        pageData = rm_table->page_handle.data;

        record_id->slot = getEmptyLoc(pageData, recordSize);
    }

    slot = pageData;

    markDirty(&rm_table->buffer_pool, &rm_table->page_handle);

    slot = slot + (record_id->slot*recordSize);

    *slot = '&';

	slot += 1;

    memcpy(slot, record->data+1, recordSize-1);

    unpinPage(&rm_table->buffer_pool, &rm_table->page_handle);

    rm_table->tuples = rm_table->tuples+1;

    pinPage(&rm_table->buffer_pool, &rm_table->page_handle, 0);

    printf("\ninsertRecord [done]\n");

    return RC_OK;

}

extern RC deleteRecord (RM_TableData *rel, RID id){
    printf("\ndeleteRecord [executing...]\n");

    RM_Table *rm_table = rel->mgmtData;

    pinPage(&rm_table->buffer_pool, &rm_table->page_handle, id.page);
    rm_table->empty_loc = id.page;

    char *pageData = rm_table->page_handle.data;

    int recordSize = getRecordSize(rel->schema);

    pageData = pageData + (id.slot*recordSize);
    *pageData = '-';

    markDirty(&rm_table->buffer_pool, &rm_table->page_handle);
    
    unpinPage(&rm_table->buffer_pool, &rm_table->page_handle);

    printf("\ndeleteRecord [done]\n");

    return RC_OK;
}

extern RC updateRecord (RM_TableData *rel, Record *record){
    printf("\nupdateRecord [executing...]\n");

    char *pageData;

    RM_Table *rm_table = rel->mgmtData;

    pinPage(&rm_table->buffer_pool, &rm_table->page_handle, record->id.page);

    int recordSize = getRecordSize(rel->schema);

    RID record_id = record->id;

    pageData = rm_table->page_handle.data + record_id.slot*recordSize;

    *pageData = '&';

    pageData = pageData+1;

    memcpy(pageData, record->data+1, recordSize-1);

    markDirty(&rm_table->buffer_pool, &rm_table->page_handle);
    
    unpinPage(&rm_table->buffer_pool, &rm_table->page_handle);

    printf("\nupdateRecord [done]\n");


    return RC_OK;

}

extern RC getRecord (RM_TableData *rel, RID id, Record *record){
    printf("\ngetRecord [executing...]\n");

    RM_Table *rm_table = rel->mgmtData;

    pinPage(&rm_table->buffer_pool, &rm_table->page_handle, id.page);

    int record_size = getRecordSize(rel->schema);
    char *page_data = rm_table->page_handle.data;
    page_data = page_data + (id.slot*record_size);

    if (*page_data != '&')
        return RC_RM_NO_MORE_TUPLES;
    else{
        record->id = id;
        char *data = record->data;
		data += 1;
        memcpy(data, page_data+1, record_size-1);
    }
    
    unpinPage(&rm_table->buffer_pool, &rm_table->page_handle);
    printf("\ngetRecord [done]\n");
    
    return RC_OK;
}

extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    printf("\nstartScan [executing...]\n");
    
    openTable(rel, "ScanTable");
    RM_Table *scanRequest = (RM_Table*) malloc(sizeof(RM_Table));

    scanRequest->record_id.page = 1;
    scanRequest->record_id.slot = 0;
    scanRequest->scanCount = 0;
    scanRequest->constraint = cond;

    scan->mgmtData = scanRequest;
    ((RM_Table *) rel->mgmtData)->tuples = ATTRIBUTE_SIZE;
    scan->rel = rel;

    printf("\nstartScan [done]\n");
    return RC_OK;
}

extern RC next (RM_ScanHandle *scan, Record *record)
{
	RM_Table *scanManager = scan->mgmtData;
	RM_Table *tableManager = scan->rel->mgmtData;
    	Schema *schema = scan->rel->schema;
	
	if (scanManager->constraint == NULL) return -1;

	Value *result = (Value *) malloc(sizeof(Value));
   
	char *data;
   	
	int recordSize = getRecordSize(schema);

	int totalSlots = PAGE_SIZE / recordSize;

	int scanCount = scanManager->scanCount;

	int tuples = tableManager->tuples;

	if (tuples == 0)
		return RC_RM_NO_MORE_TUPLES;

	while(scanCount <= tuples)
	{  
		if (scanCount <= 0)
		{
		
			scanManager->record_id.page = 1;
			scanManager->record_id.slot = 0;
		}
		else
		{
			scanManager->record_id.slot++;

			if(scanManager->record_id.slot >= totalSlots)
			{
				scanManager->record_id.slot = 0;
				scanManager->record_id.page++;
			}
		}

		pinPage(&tableManager->buffer_pool, &scanManager->page_handle, scanManager->record_id.page);
			
		data = scanManager->page_handle.data;

		data = data + (scanManager->record_id.slot * recordSize);
		
		record->id.page = scanManager->record_id.page;
		record->id.slot = scanManager->record_id.slot;

		char *dataPointer = record->data;

		*dataPointer = '-';
		
		memcpy(++dataPointer, data + 1, recordSize - 1);

		scanManager->scanCount++;
		scanCount++;

		evalExpr(record, schema, scanManager->constraint, &result); 

		if(result->v.boolV == TRUE)
		{
			unpinPage(&tableManager->buffer_pool, &scanManager->page_handle);
			return RC_OK;
		}
	}
	
	unpinPage(&tableManager->buffer_pool, &scanManager->page_handle);
	
	scanManager->record_id.page = 1;
	scanManager->record_id.slot = 0;
	scanManager->scanCount = 0;

	return RC_RM_NO_MORE_TUPLES;
}

extern RC closeScan (RM_ScanHandle *scan){
    printf("\ncloseScan [executing...]\n");

    RM_Table *rm_table = scan->rel->mgmtData;
    RM_Table *rm_scan = scan->mgmtData;

    if(rm_scan->scanCount > 0){
        unpinPage(&rm_table->buffer_pool, &rm_scan->page_handle);
        rm_scan->scanCount = 0;
        rm_scan->record_id.slot = 0;
        rm_scan->record_id.page = 1;
        
    }
    scan->mgmtData = NULL;
    free(scan->mgmtData);

    printf("\ncloseScan [done]\n");
    return RC_OK;
}

int getRecordSize(Schema *schema){
	
	// return size of the current schema
	printf("\ngetRecordSize [executing...]");
	int _temp = 0;

	if (schema == NULL){
		return -1;
	}
	else{	
		
		for (int i = 0; i < schema->numAttr; ++i){
			char datatype = schema->dataTypes[i];
			if(datatype == DT_INT)
				_temp +=  sizeof(int);
			else if (datatype == DT_FLOAT)
				_temp += sizeof(float);
			else if (datatype == DT_BOOL)
				_temp +=  sizeof(bool);
			else{
				int d = (sizeof(char) * schema->typeLength[i]);
				_temp += ATTRIBUTE_SIZE;
			}
		}
		printf("\ngetRecordSize [done...]");
		return _temp;

	}
}


extern RC freeSchema (Schema *schema){
    printf("\nfreeSchema [executing...]\n");
    free(schema);
    printf("\nfreeSchema [done]\n");
    return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	printf("\ngetAttr [executing...]\n");
	int startPos = 0;

	attrOffset(schema, attrNum, &startPos);

	Value *attr = (Value*) malloc(sizeof(Value));

	char *record_data = record->data;
	
	record_data = record_data + startPos;

	if(attrNum == 1){
		schema->dataTypes[attrNum] = 1;
	}
		
	
	if (schema->dataTypes[attrNum] == DT_STRING){
		int length = schema->typeLength[attrNum];
		attr->v.stringV = (char *) malloc(length + 1);

		strncpy(attr->v.stringV, record_data, length);
		attr->v.stringV[length] = '\0';
		attr->dt = DT_STRING;
	}

	else if (schema->dataTypes[attrNum] == DT_INT){
		int value = 0;
		memcpy(&value, record_data, sizeof(int));
		attr->v.intV = value;
		attr->dt = DT_INT;
	}

	else if (schema->dataTypes[attrNum] == DT_FLOAT){
		float value;
		memcpy(&value, record_data, sizeof(float));
		attr->v.floatV = value;
		attr->dt = DT_FLOAT;
	}
	else{

		bool value;
		memcpy(&value,record_data, sizeof(bool));
		attr->v.boolV = value;
		attr->dt = DT_BOOL;
	}
	*value = attr;

	printf("\ngetAttr [done...]\n");
	return RC_OK;
}


extern RC createRecord (Record **record, Schema *schema){
    printf("\ncreateRecord [executing...]\n");

    // get size of the current schema
    int schema_size = getRecordSize(schema);

    // allocate space to create new record
    Record *createNewRecord = (Record*) malloc(sizeof(Record));

    // initialize page and slot ID with random
    createNewRecord->id.page = -999;
    createNewRecord->id.slot = -999;

    // allocate space for record's data
    createNewRecord->data = (char*) malloc(schema_size);

    char *startPos = createNewRecord->data;

    *startPos = '-';

    *(++startPos) = '\0';

    *record = createNewRecord;

    printf("\ncreateRecord [done]\n");

    return RC_OK;

}


RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int i;
	*result = 1;

	
	for(i = 0; i < attrNum; i++)
	{
		
		switch (schema->dataTypes[i])
		{
			
			case DT_STRING:
				
				*result = *result + schema->typeLength[i];
				break;
			case DT_INT:
				
				*result = *result + sizeof(int);
				break;
			case DT_FLOAT:
				
				*result = *result + sizeof(float);
				break;
			case DT_BOOL:
				
				*result = *result + sizeof(bool);
				break;
		}
	}
	return RC_OK;
}

extern RC freeRecord (Record *record)
{
	printf("\nfreeRecord [executing...]\n");
	free(record);
	printf("\nfreeRecord [done]\n");
	return RC_OK;
}


extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	printf("\nsetAttr [executing...]\n");
	int startPos = 0;

	attrOffset(schema, attrNum, &startPos);

	char *record_data = record->data;
	
	record_data = record_data + startPos;

	if (schema->dataTypes[attrNum] == DT_STRING){
		int length = schema->typeLength[attrNum];
		strncpy(record_data, value->v.stringV, length);
		record_data = record_data + schema->typeLength[attrNum];
	}

	else if (schema->dataTypes[attrNum] == DT_INT){
		*(int *) record_data = value->v.intV;	  
		record_data = record_data + sizeof(int);
	}

	else if (schema->dataTypes[attrNum] == DT_FLOAT){
		*(float *) record_data = value->v.floatV;
		record_data = record_data + sizeof(float);
	}
	else{

		*(bool *) record_data = value->v.boolV;
		record_data = record_data + sizeof(bool);
	}
	
	printf("\nsetAttr [done]\n");
	return RC_OK;
}
