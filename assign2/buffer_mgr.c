/*
Assignment 2 : CS525 - Advanced Database Organization 

Date: March 12, 2021.

@author: Group 18
*/

#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>
#include<unistd.h>
#include <sys/time.h>

// define a data structure for bookkeeping
typedef struct Page
{
	int dirty; 
	int fixCount;
	SM_PageHandle data;
	PageNumber pageNum;
    long int timeStamp;
    int used;
} PageFrame;


long GetTimeStamp() {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    long int t = 1000000+tv.tv_sec+tv.tv_usec;
    return t;
}

void displayBufferPool(BM_BufferPool *const bm);
void FIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum);
void LRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum);
void CLOCK(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum);

int read_pageCount; //maintains count record of number of pages read from disk
int write_pageCount; //maintains count record of number of pages written to disk

int page_ptr = 0;

int fifo_count = 0;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy,void *stratData){

    // Initializes buffer pool with provided parameters
    printf("\ninitBufferPool [executing...]\n");
    // check if page file already exists
    FILE *file;
    file =fopen(pageFileName, "r");
    if(file == NULL){
        printf("\ngiven page file does not exists. Terminating...\n");
        return RC_FILE_NOT_FOUND;
    }

    else{
        if(numPages != 0){

            PageFrame *page = malloc(sizeof(PageFrame)*numPages);

            // initialize all the page frames
            for(int i = 0; i < numPages; i++){
                page[i].dirty = -1;
                page[i].fixCount = 0;
                page[i].data = NULL;
                page[i].pageNum = NO_PAGE;
                page[i].timeStamp = -1;
                page[i].used = 0;
            }

            //  number of page frames   
            bm->numPages = numPages;
            // page replacement strategy
            bm->strategy = strategy;
            // name of page file associated with buffer pool
            bm->pageFile = (char *) pageFileName;
            // pointer to bookkeeping data
            bm->mgmtData = page;

            fifo_count = 0;
            write_pageCount = 0;
            read_pageCount = 0;

            displayBufferPool(bm);
            // fclose(file);
            printf("\ninitBufferPool [done]\n");
            return RC_OK;
        }
        else{
            printf("\nNot enough number of pages. Terminating...\n");
            return -1;
        }
    }
    printf("\ninitBufferPool [done]\n");
}

void displayBufferPool(BM_BufferPool *const bm){
    // display current state of buffer
    printf("\ndisplayBufferPool [executing...]\n");

    PageFrame *pageFlag = (PageFrame *) bm->mgmtData;
    printf("\nTotal pages in buffer pool: %d  Associdated File: %s\n", bm->numPages, bm->pageFile);
    for (int i = 0; i<bm->numPages; i++){
        printf("\nPage %d\n", i+1);
        printf("\nFixCount: %d  Is Dirty: %d  PageNum: %d Time: %ld\n", 
                        pageFlag[i].fixCount,
                        pageFlag[i].dirty, 
                        pageFlag[i].pageNum, 
                        pageFlag[i].timeStamp);
    }
    printf("\ndisplayBufferPool [done]\n");
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){

    // unpins the page provided in parameter
    printf("unpinPage [executing...]\n");

    if(bm == NULL || bm->numPages == 0 || bm->mgmtData == NULL){
        printf("Number of pages in buffer pool: %d \n Terminating", bm->numPages);
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    // load data of all the pages of buffer pool
    PageFrame *pageFlag = (PageFrame *)bm->mgmtData;

    bool pageFound = false;

    // check if requested page exists in framelist
    for(int i=0; i<bm->numPages; i++){
        if (pageFlag[i].pageNum == page->pageNum){
            pageFound = true;
        }
    }

    if(pageFound){
        for(int i =0; i < bm->numPages; i++){
            if (pageFlag[i].pageNum == page->pageNum){
                pageFlag[i].fixCount -= 1;
                break;
            }
        }
        printf("\nunpinPage [done]\n");
        return RC_OK;
    }
    else{
        printf("\nRequested page not in framelist.\n");
        return -1;
    }
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    // marks the page dirty provided in parameter if it was modified by client
    printf("markDirty [executing...]\n");
    printf("\nRequested Page %d", page->pageNum);

    if(bm == NULL || bm->numPages == 0){
        printf("Number of pages in buffer pool: %d \n Terminating", bm->numPages);
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    // load data of all the pages of buffer pool
    PageFrame *pageFlag = (PageFrame *)bm->mgmtData;
    bool pageFound = false;

    // check if requested page exists in framelist
    for(int i=0; i<bm->numPages; i++){
        if (pageFlag[i].pageNum == page->pageNum){
            pageFound = true;
        }
    }


    if(pageFound){
        for(int i =0; i < bm->numPages; i++){
            if(pageFlag[i].pageNum == page->pageNum){
                pageFlag[i].dirty = 1;
            }
        }
        displayBufferPool(bm);
        printf("\nmarkDirty [done]\n");
        return RC_OK;
    }
    else{
        return -1;
    }
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){

    // pins a page by pageNum from given file to buffer pool
    printf("\npinPage [executing...]\n");

    printf("\nRequested File: %s  Requested Page: %d\n", bm->pageFile, pageNum);

    if(pageNum < 0 || bm->mgmtData == NULL || bm == NULL){
        printf("Negative page number");
        return -1;
    }

    // load data for all the pages in buffer pool
    PageFrame *pageFlag = (PageFrame *)bm->mgmtData;

    for(int i=0; i<bm->numPages; i++){
        // check if any buffer pool page is unassigned
        if(pageFlag[i].pageNum == NO_PAGE){
            SM_FileHandle fh;
            openPageFile(bm->pageFile, &fh);
            pageFlag[i].data = (SM_PageHandle)malloc(PAGE_SIZE);
            ensureCapacity(pageNum, &fh);
            readBlock(pageNum, &fh, pageFlag[i].data);
            read_pageCount += 1;

            // printf("\n\nafter readblock: %s  %d\n", pageFlag[i].data, i);
            pageFlag[i].fixCount+=1;
            pageFlag[i].pageNum = pageNum;
            pageFlag[i].timeStamp = GetTimeStamp();
            pageFlag[i].used = 0;
            page_ptr += 1;
            page->pageNum = pageNum;
            page->data = pageFlag[i].data;
            // printf("%d, %s", page->pageNum, page->data);
            displayBufferPool(bm);
            printf("\npinPage [done]\n");
            return RC_OK;
        }
        // check if requested page exists in the buffer pool
        else if(pageFlag[i].pageNum == pageNum){
            pageFlag[i].fixCount += 1;
            pageFlag[i].timeStamp = GetTimeStamp();
            pageFlag[i].used = 1;
            page->pageNum = pageNum;
            page->data = pageFlag[i].data;
            printf("\npinPage [done]\n");
            return RC_OK;
        }
    }
    
    int count = 0;

    // check if all the pages are pinned
    for(int i=0; i<bm->numPages; i++){
        if (pageFlag[i].fixCount > 0){
            count += 1;
        }
    }

    if (count == bm->numPages){
        printf("\nAll pages are pinned. Cannot pin new pages\n");
        return -1;
    }

    // replace page in the buffer pool using requested replacement strategy
    if (bm->strategy == RS_FIFO)
        FIFO(bm, page, pageNum);
    else if(bm->strategy == RS_LRU)
        LRU(bm, page, pageNum);
    else
        CLOCK(bm, page, pageNum);

    displayBufferPool(bm);
    return RC_OK;
}

void FIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
    printf("\nFIFO [executing...]\n");
   
    // load data of all the pages of buffer pool
    PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
    

    fifo_count = fifo_count % bm->numPages;
    if(pageFrame[fifo_count].fixCount != 0)
        fifo_count += 1;
    fifo_count = fifo_count % bm->numPages;
    
    if(pageFrame[fifo_count].dirty == 1){
        SM_FileHandle file;
        openPageFile(bm->pageFile, &file);
        writeBlock(pageFrame[fifo_count].pageNum, &file, pageFrame[fifo_count].data);
        write_pageCount+=1;
    }
    printf("FIFO  Index: %d", fifo_count);

    PageFrame *pageFlag = (PageFrame *)bm->mgmtData;

	SM_FileHandle fh;
	openPageFile(bm->pageFile, &fh);

	pageFlag[fifo_count].data = (SM_PageHandle) malloc(PAGE_SIZE);
	readBlock(pageNum, &fh, pageFlag[fifo_count].data);
    read_pageCount += 1;
    pageFlag[fifo_count].fixCount+=1;
    pageFlag[fifo_count].pageNum = pageNum;
    pageFlag[fifo_count].dirty = -1;
    pageFlag[fifo_count].timeStamp = GetTimeStamp();
    pageFlag[fifo_count].used = 0;
    page_ptr += 1;
    page->pageNum = pageNum;
    page->data = pageFlag[fifo_count].data;


    fifo_count += 1;

}

void LRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){

    printf("\nLRU [executing...]\n");

    // load data for all the pages in buffer pool
    PageFrame *pageFlag = (PageFrame *)bm->mgmtData;

    int idx = 0;
    double  least_idx = INFINITY;
    
    for(int i=0; i<bm->numPages; i++){
        if (pageFlag[i].timeStamp < least_idx){
            least_idx = pageFlag[i].timeStamp;
            idx = i;
        }
    }

    printf("\nLeast Recently Used Idx: %d\n", idx);

    SM_FileHandle fh;
	openPageFile(bm->pageFile, &fh);

	pageFlag[idx].data = (SM_PageHandle) malloc(PAGE_SIZE);
	readBlock(pageNum, &fh, pageFlag[idx].data);
    read_pageCount += 1;
    pageFlag[idx].fixCount+=1;
    pageFlag[idx].pageNum = pageNum;
    pageFlag[idx].dirty = -1;
    pageFlag[idx].timeStamp = GetTimeStamp();
    pageFlag[idx].used = 0;
    page_ptr += 1;
    page->pageNum = pageNum;
    page->data = pageFlag[idx].data;

    printf("\nLRU [done]\n");

}

void CLOCK(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){

    printf("\nCLOCK [executing...]\n");
    if (page_ptr > bm->numPages-1)
        page_ptr = 0;

    // load all the page data from buffer pool
    PageFrame *pageFlag = (PageFrame *) bm->mgmtData;
    SM_FileHandle fh;

    for(int i=0; i<2; i++){
        if(pageFlag[page_ptr].used == 0){
            if(pageFlag[page_ptr].dirty == 1){
                openPageFile(bm->pageFile, &fh);
                writeBlock(pageFlag[page_ptr].pageNum, &fh, pageFlag[page_ptr].data);
                write_pageCount += 1;
            }

            pageFlag[page_ptr].data = (SM_PageHandle) malloc(PAGE_SIZE);
            readBlock(pageNum, &fh, pageFlag[page_ptr].data);
            read_pageCount += 1;
            pageFlag[page_ptr].fixCount+=1;
            pageFlag[page_ptr].pageNum = pageNum;
            pageFlag[page_ptr].dirty = -1;
            pageFlag[page_ptr].timeStamp = GetTimeStamp();
            pageFlag[page_ptr].used = 0;
            page_ptr ++;
            page->pageNum = pageNum;
            page->data = pageFlag[page_ptr].data;
            break;
        }
        else{
            pageFlag[page_ptr].used = 0;
            page_ptr++;
        }
    }
    printf("\nCLOCK [done]\n");
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    printf("ForcePage [Executing..]");
    PageFrame *pageFlag = (PageFrame *)bm->mgmtData;

    bool pageFound = false;

    // check if requested page exists in framelist
    for(int i=0; i<bm->numPages; i++){
        if (pageFlag[i].pageNum == page->pageNum){
            pageFound = true;
        }
    }

    if(pageFound){
        for(int i = 0; i <bm->numPages; i++){
            if(pageFlag[i].dirty == 1 && pageFlag[i].pageNum == page->pageNum)
            {
                SM_FileHandle fh;
                openPageFile(bm->pageFile, &fh);
                writeBlock(pageFlag[i].pageNum, &fh, pageFlag[i].data);
                write_pageCount += 1; 
                pageFlag[i].dirty = 0; 
            }
        }
        printf("ForcePage [Done]");
        return RC_OK;
    }
    else{
        return -1;
    }
    
}   

RC forceFlushPool(BM_BufferPool *const bm){

    // writes content of page to pagefile if found dirty
    printf("\nforceFlushPool [executing...]\n");

    if (bm == NULL || bm->numPages == 0 || bm->mgmtData == NULL){
        printf("\nNothing to force flush! Terminating...\n");
        return -1;
    }    

    // load data of all the pages of buffer pool
    PageFrame *pageFlag = (PageFrame *) bm->mgmtData;

    
   
    for (int i = 0; i<bm->numPages; i++){
        
        if(pageFlag[i].dirty == 1){
            // if page is dirty writeback
            SM_FileHandle file;
            openPageFile(bm->pageFile, &file);
            writeBlock(pageFlag[i].pageNum, &file, pageFlag[i].data);
            write_pageCount+=1;

            // remove the dirty flag
            pageFlag[i].dirty = -1;
        } 

    }
    printf("\nforceFlush [done...]\n");
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){

    // destroys a buffer pool
    printf("\nshutdownBufferPool [executing...]\n");

    // printf("here %ld", ftell(bm->pageFile));

    if (bm == NULL || bm->numPages == 0 || bm->mgmtData == NULL){
        printf("\nNothing to destroy. Terminating...\n");
        return -1;
    }

    // load data for all the pages
    PageFrame *pageFlag = bm->mgmtData;

    // if buffer pool contains any dirty pages
    if(forceFlushPool(bm) == RC_OK){
        free(pageFlag);
        bm->mgmtData = NULL;
        
        printf("\nshutDownBufferPool [done]\n");
        return RC_OK;
    }
    printf("\nOops! Some error occured.\n");
    return -1;


}

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    //frameContent of size numPages
    PageNumber *fContents = (PageNumber *) malloc(sizeof(PageNumber)*bm->numPages);
    PageFrame *pageFlag = (PageFrame *) bm->mgmtData;

    int i = 0;
    // check if frame contents pointer initialized or not
    // check if buffer pool is empty or not
    if(fContents == NULL)
    {
        printf("\nFrame of Contents not initialized\n");
        printf("\nnumPages is NULL\n");
    }
    else
    {
        while (i < bm->numPages) 
        {
            if (pageFlag[i].pageNum == -1)
            {
                // assign NO_PAGE when page frame is empty
                fContents[i] = NO_PAGE;
            }
            else
            {
                // assign page frame contents to frameContents
                fContents[i] = pageFlag[i].pageNum;
            }

            i++;
        }
    }
    printf("\ngetFrameContents [done]\n");
    return fContents;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    //isDirty array of size numPages
    bool *isDirty = (bool *) malloc(sizeof(bool)*bm->numPages);
    PageFrame *pageFlag = (PageFrame *) bm->mgmtData;

    int i = 0;

    // check if dirty check pointer initialized or not
    // check if buffer pool is empty or not
    if(isDirty == NULL)
    {
        printf("\nDirty Flags Array not initialized\n");
        printf("\nnumPages is NULL\n");
    }
    else
    {
        while (i < bm->numPages)
        {
           //clean when page frame is empty (-1)
           //dirty otherwise
           isDirty[i] = (pageFlag[i].dirty == -1) ? false : true;
           i++;
        }
    }
    printf("\ngetDirtyFlags [done]\n");
    return isDirty;
}

int *getFixCounts (BM_BufferPool *const bm) 
{
	int *fix_counts = (int *)malloc(sizeof(int) * bm->numPages);

    // load all the data for page
	PageFrame *pageFlag= (PageFrame *)bm->mgmtData;

    //check if initialized with size of numPages
	if(fix_counts == NULL)
    {
        printf("\nFIx Counts Array not initialized\n");
        printf("\nnumPages is NULL\n");
    }
    int i=0;

	while(i < bm->numPages) //loop through buffer pool
	{
		if(pageFlag[i].fixCount == -1) 
		{
            fix_counts[i] = 0;
		}
		else
		{
            //element at i is the fix count stored at i in the page frame
			fix_counts[i] = pageFlag[i].fixCount;
		}
        i++;
	}
    printf("\ngetFixCounts [done]\n");
	return fix_counts;
}

int getNumReadIO (BM_BufferPool *const bm)
{
    return read_pageCount;
}

int getNumWriteIO (BM_BufferPool *const bm)
{
    return write_pageCount;
}