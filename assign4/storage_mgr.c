/*
Assignment 1 : CS525 - Advanced Database Organization 

Date: February 09, 2021.

@author: Group 18
*/

#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dberror.h"
#include <unistd.h>

FILE *file;

void initStorageManager(){
    
    // initializing the storage manager

    file = NULL;
    printf("\nStorage manager initialized.\n");

}

void closefile(FILE *file){
    if(fclose(file) == 0)
        printf("\nFile closed successfully.\n");
}

RC createPageFile (char *fileName) {

    // Creates a new page file fileName
    // Initial file size is one page

    printf("\ncreatePageFile [executing...]\n");
	
    // open a file to write the contents
    file = fopen(fileName, "w+");

    // allocate memory using calloc
    SM_PageHandle page_file = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    
    // the memset method will copy the character '\0' 
    // pointed to by the argument page_file
    // page_file : pointer to the block of memory to fill
    // '\0' : value to be written
    // PAGE_SIZE : number of bytes 
    
    memset(page_file, '\0', PAGE_SIZE);

    // this will write the data pointed to, by page_file to the given file
    // sizeof(char) : size in bytes of each element
    // PAGE_SIZE : number of elements equal to sizeof(char) bytes.
    // file : pointer to a file.

    fwrite(page_file, sizeof(char), PAGE_SIZE, file);

    // deallocate the allocated memory on line 36
    // page_file : pointer to a memory block  
    free(page_file);
    closefile(file);
    printf("\ncreatePageFile [done]\n");

    return RC_OK;

}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){

    // Opens an existing page file
    //  *filehandle -> existing file handle
    printf("\nopenPageFile [executing...]\n");

    // open given file in read mode
    file = fopen(fileName, "r");	
    int pagecnt,filesize;
    
    if (file == 0)
	{
        RC_message = "File not found";
        return RC_FILE_NOT_FOUND; 
    }
        
    else {
    
        // if file exists
        fseek(file, 0, SEEK_END); //end of the file
        filesize = ftell(file); //size of the file
        pagecnt = filesize / PAGE_SIZE; //number of pages in the file
        
        // Initialize fHandle attributes
        fHandle->fileName = fileName;
        fHandle->curPagePos = 0; //pointer updated to start of the file
        fHandle->totalNumPages = pagecnt;
        fHandle->mgmtInfo = file;
        closefile(file);
        printf("\nopenPageFile [done]\n");

        return RC_OK;
    }
}

RC closePageFile (SM_FileHandle *SM_FileHandle){
    printf("\nclosePageFile [executing...]\n");

    // closes the file if found in open mode
    // ftell checks the status of file
    // if ftell return negative value -> file already closed
    if (ftell(file) < 0){
            printf("file is in closed mode");
            return RC_OK;
    }
    else{
        closefile(file);
        printf("\nclosePageFile [done]\n");
        return RC_OK;    
    }
}

RC destroyPageFile(char *fileName){

    // delete/destory a page file
    printf("\ndestroyPageFile [executing...]\n");
    file = fopen(fileName, "r");
    if (file != NULL){
        // unlink will delete a name from filesystem
        // file is deleted and space allocated is made available
        printf("\nFile being destroyed: %s\n", fileName);
        closefile(file);
        unlink(fileName);
    }
    
    else{
        return RC_FILE_NOT_FOUND;
    }

    printf("\ndestroyPageFile [done]\n");

    return RC_OK;
}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    printf("\nreadBlock [executing...]\n");
    printf("here");
    
    file = fopen(fHandle->fileName ,"r+"); 
	if ( file == NULL)
    {
			RC_message = "File Not Found";
			return RC_FILE_NOT_FOUND;
    }
    
    else{
        
        if(pageNum < 0)//if page number less than 0 or page number greater than total pages then error
        {
            RC_message = "Page does not exist";
            closefile(file);
            return RC_READ_NON_EXISTING_PAGE;
		}
        
        else{
        
            fseek(file, (pageNum) * PAGE_SIZE, SEEK_SET);
            fread(memPage, sizeof(char), PAGE_SIZE, file); //read file block
            fHandle->curPagePos = pageNum;// update current page position
            closefile(file);
            printf("\nreadBlock [done]\n");
            return RC_OK;
        }
    }
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    printf("\nreadFirstBlock [executing...]\n");
    // open given file
    file = fopen(fHandle->fileName, "r");
    if (file != NULL){
        // read contents of first block
        for (int i=0; i<PAGE_SIZE; i++){
            char content = fgetc(file);
            printf("%c", content);
            if(feof(file)){
                break;
            }
            else{
                memPage[i] = content;
            }
        }
    }
    else{
        return RC_FILE_NOT_FOUND;
    }
    closefile(file);
    printf("\nreadFirstBlock [done]\n");
    return RC_OK;
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    printf("\nreadCurrentBlock [executing...]\n");

    // open file to read current block
    file = fopen(fHandle->fileName, "r");

    if(file != NULL){
        // get the current block position
        int curr_ptr = getBlockPos(fHandle)/PAGE_SIZE;
        printf("\nCurrent Pointer is at: %d\n", curr_ptr*PAGE_SIZE);

        // set the file position indicator pointed to by file
        fseek(file, curr_ptr*PAGE_SIZE, SEEK_CUR);

        // read PAGE_SIZE items of data
        // store data in the location given by memPage
        // ptr stores the file position indiciator equvivalent to 
        // number of bytes read
        fread(memPage, sizeof(char), PAGE_SIZE, file);
        
        printf("After reading a block Pointer is at: %ld\n", ftell(file));

        // save new pointer location
        fHandle->curPagePos = ftell(file);
    }
    else{
        printf("File not found");
        return RC_FILE_NOT_FOUND;
    }
    closefile(file);
    printf("\nreadCurrentBlock [done]\n");
    return RC_OK;
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
     printf("\nreadPreviousBlock [executing...]\n");

    // open file to read current block
    file = fopen(fHandle->fileName, "r");

    if(file != NULL){
        // get the current block position
        int curr_ptr = getBlockPos(fHandle)/PAGE_SIZE-2;
        printf("\nCurrent Pointer is at: %d\n", curr_ptr*PAGE_SIZE);

        // set the file position indicator pointed to by file
        int a = fseek(file, curr_ptr*PAGE_SIZE, SEEK_CUR);
        // read PAGE_SIZE items of data
        // store data in the location given by memPage
        // ptr stores the file position indiciator equvivalent to 
        // number of bytes read
        fread(memPage, sizeof(char), PAGE_SIZE, file);

        printf("After reading a block Pointer is at: %ld\n", ftell(file));

        // save new pointer location
        fHandle->curPagePos = ftell(file);
    }
    else{
        printf("File not found");
        return RC_FILE_NOT_FOUND;
    }
    closefile(file);
    printf("\nreadPreviousBlock [done]\n");
    return RC_OK;
 }

int getBlockPos (SM_FileHandle *fHandle){

    // gets the current page position in the file
    RC Block_POS = (*fHandle).curPagePos;

    return Block_POS;
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    // Write a page to disk using either the current position or an absolute position
    printf("\nwriteBlock [executing...]\n");
    if (pageNum < 0){
        //checks if page number is within range of total number of pages 
        //throws error as this condition checks out of range situation
        return RC_WRITE_FAILED;
    }
    file = fopen(fHandle->fileName, "r+");
    if (file == NULL){
        //checks if file is found or not
        return RC_FILE_NOT_FOUND;
    }
    else if (pageNum >= 0){
        //checks condition if file is found and pageNum is within range 0 and total number of pages
        //fseek will check here the file pointer position
        //seek operation works successfully if the fseek value equates to 0
        printf("\nBefore writting pointer at %d", pageNum*PAGE_SIZE);

        if(fseek(file, pageNum*PAGE_SIZE, SEEK_SET)==0){
            int ptr = fwrite(memPage, sizeof(char), PAGE_SIZE, file);

           //moving the pointer to current page position after write operation is complete 
           fHandle->curPagePos = ftell(file);
           fHandle->totalNumPages += 1;
        }
    }
    
    printf("\nAfter writting pointer at %ld", ftell(file));
    printf("\nwriteBlock [done]\n");
    fclose(file);
    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    // open given file
    file = fopen(fHandle->fileName, "r");

    if(file != NULL){
        // get current block position to write
        int curr_pos = getBlockPos(fHandle);
        if (writeBlock(curr_pos, fHandle, memPage) == RC_OK){
            // increment number of pages by 1
            // filehandle->totalNumPages += 1;
            closefile(file);
            return RC_OK;
        }
        else{
            return RC_WRITE_FAILED;
        }
    }
    else{
        return RC_FILE_NOT_FOUND;
    }
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    // If the file has less than numberOfPages pages then increase the size to numberOfPages
    // first checks if total pages of file is already greater or equal to number of pages
    printf("\nensureCapacity [executing...]\n");
    if ((*fHandle).totalNumPages >= numberOfPages)
        return RC_OK;
    else{
        // increases size of file to the number of pages by appending blocks
        // keeps increasing till the file page size equals number of pages
        while ((*fHandle).totalNumPages < numberOfPages){
            appendEmptyBlock(fHandle);
        }
        printf("\nensureCapacity [done]\n");
        return RC_OK;
    }

}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    printf("\nreadNextBlock [executing...]\n");

    // open file to read current block
    file = fopen(fHandle->fileName, "r");

    if(file != NULL){
        // get the current block position
        int curr_ptr = getBlockPos(fHandle)/PAGE_SIZE+1;
        printf("\nCurrent Pointer is at: %d\n", curr_ptr*PAGE_SIZE);

        // set the file position indicator pointed to by file
        fseek(file, curr_ptr*PAGE_SIZE, SEEK_CUR);

        // read PAGE_SIZE items of data
        // store data in the location given by memPage
        // ptr stores the file position indiciator equvivalent to 
        // number of bytes read
        fread(memPage, sizeof(char), PAGE_SIZE, file);
        
        printf("After reading a block Pointer is at: %ld\n", ftell(file));

        // save new pointer location
        fHandle->curPagePos = ftell(file);
    }
    else{
        printf("File not found");
        return RC_FILE_NOT_FOUND;
    }
    closefile(file);
    printf("\nreadCurrentBlock [done]\n");
    return RC_OK;
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fopen(fHandle->fileName ,"r") == NULL)// if file not present
    {
	    RC_message = "File Not Found";
		return RC_FILE_NOT_FOUND;
    }
        
    else
    {
        return  readBlock(fHandle->totalNumPages-1,fHandle,memPage);// last block is the total number of pages minus 1
    }
}

RC appendEmptyBlock (SM_FileHandle *fHandle)
{
    file =  fopen(fHandle->fileName ,"r");
    if(file == NULL )// missing file
    {
        
        RC_message = "File Not Found";
		return RC_FILE_NOT_FOUND;
    }
    
    else{
    
        SM_PageHandle EmptyBlock = (SM_PageHandle)calloc(PAGE_SIZE,sizeof(char));//initialize empty block
    
        fseek(file, 0, SEEK_END); //seek to the current page
    
        fwrite(EmptyBlock, sizeof(char), PAGE_SIZE, file);//write the new block to file
        fHandle->totalNumPages = fHandle->totalNumPages + 1;// total number of pages increased by 1
        fHandle->curPagePos = fHandle->totalNumPages; //the current position is updated to the end
        
        //fprintf("Total number of pages in file: %d", fHandle->totalNumPages);
        free(EmptyBlock);
        
    }
    closefile(file);
    return RC_OK;
}    
