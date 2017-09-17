/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall hello.c `pkg-config fuse --cflags --libs` -o hello
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#define bool int
#define true 1
#define false 0


#define max_blocks_per_file  500
#define block_size 4096

void * mem_block; //memory block starting address for input size file structure
long SIZE; //STORES the SIZE of MALLOC area
char persistent_file[1024] = {0};
int data_block_offset = 0; //data block offset pointing to free location
//Dictionary Structure...filename and addr of content storage

typedef struct metadata_structure{
  int meta_count1;
  int data_block_offset1;
  char file_path[1024]; //stores path of file eg. /hello
  int type; // -1:Dictionary, 1:File -2:Deleted
  int block[max_blocks_per_file]; //stores offset of block, default -1
  int length; //stores length of file...stores -1 if directory
}metadata_structure;
metadata_structure *meta; //array of metadata entries
int meta_count = 0; //stores count of number of metadata entries
int meta_elements; //maximum number of metadata entries
int size_metadata;

int fd1, fd2;

int file_number(const char * path){
  int i;
  /*Gives file number of OPENED file*/
  for(i=0; i<meta_count; i++){
    if (strcmp(path, meta[i].file_path) == 0){
      /*Check if entry is present but file is currently deleted*/
      if(meta[i].type == 0)
	continue;
      else
	break;
    }
  }
  return i;
}

/*returns the parent directory of current path*/
char * getDir(char * file_path){
  char *lastSlash = NULL;
  char *parent = NULL;
  //int len = strlen(file_path);
    lastSlash = strrchr(file_path, '/');
    parent = strndup(file_path, strlen(file_path) - strlen(lastSlash) + 1);
    return parent;
}

/*Modify all the subchild entries in case of DIRECTORY rename or move*/
void modifyDir(char * to_be_replaced, char * replace_by){
  int i;
  char * lastloc = NULL;
  int len = strlen(to_be_replaced);
  char ch[1024];
  for(i=0; i<meta_count; i++){
    lastloc = strstr(meta[i].file_path, to_be_replaced);
    if(lastloc == NULL)
      continue;
    char * new_name = meta[i].file_path + len;
    strcpy(ch, replace_by);
    strcat(ch, new_name);
    strcpy(meta[i].file_path, ch);
    
  }
}

/*Returns TRUE if DIR is empty, else return FALSE*/
bool isEmptyDir(char * path){
  char * file_dir;
  int len = strlen(path);
   /*If no '/' in the END then add it*/
  if(path[len-1] != '/')
    strcat(path, "/");
  int i;
  for(i=1; i<meta_count; i++){
    // printf("FILE(%d): %s\n", i, meta[i].file_path);
    file_dir = getDir(meta[i].file_path);
    //printf("FILEDIR(%d): %s\n", i, file_dir);
    if((strcmp(file_dir,path) == 0) && (meta[i].type != 0))
      return false;
  }
  return true;
}

static int hello_rename(const char *old_path, const char *new_path){
  int file_no_old = file_number(old_path);
  int file_no_new = file_number(new_path);
  //printf("\nINSIDE RENAME\n");
  //printf("OLD PATH: %s\n", old_path);
  //printf("NEW PATH: %s\n", new_path);
  //printf("VALUE: %u\n", val);
  int res = 0;
  char * dirname = getDir((char *)new_path);
  int len1 = strlen(dirname);
  //printf("DIRNAME %s\n", dirname);
  if((dirname[len1-1] == '/') && len1 != 1)
    dirname[len1-1] = '\0';
  //printf("PARENT of NEW PATH: %s\n", dirname);

  //printf("Check 0\n");
  /*Case 0: if old path doesn't already exist then return ERROR*/
  if(file_no_old == meta_count)
    return -ENOENT;

  //printf("Check 1\n");
  /*Case 1: Check if parent of new path already exists, if not then return ERROR*/
  if(file_number(dirname) == meta_count)
    return -ENOENT; //////////////return some diff error

  //printf("Check 2\n");
  /*Case 2: Check if old path is file*/
  if(meta[file_no_old].type == 1){
    //printf("IT IS FILE\n");
    /*Case 2.1: Check if entry already present with new path name*/
    if(file_no_new < meta_count)
      meta[file_no_new].type = 0; //mark the file as deleted
      
    strcpy(meta[file_no_old].file_path, new_path);
    return 0;
  }

  //printf("Check 3\n");
  /*Case 3: Check if old path is directory*/
  if(meta[file_no_old].type == -1){
    //printf("IT IS DIRECTORY\n");

    char * to_be_replaced = meta[file_no_old].file_path;
    strcpy(meta[file_no_old].file_path, new_path);
    char * replace_by = meta[file_no_old].file_path;
    /*Replace this in the while structure*/
    modifyDir(to_be_replaced, replace_by);

    
    /*Case 2.1: Check if entry already present with new path name*/
    /*if(file_no_new < meta_count)
      meta[file_no_new].type = 0; //mark the file as deleted
    */
    return 0;
  }
  
  else
    res = -ENOENT;

  //printf("Check 4\n");
  //printf("OUTSIDE RENAME\n\n");
  return res;
}


  
static int hello_getattr(const char *path, struct stat *stbuf)
{
  //printf("\nINSIDE GETATTR\n");
  //printf("PATH : %s\n", path);
  //printf("Metadata START: %ld\nMetadata END: %ld\n", 0, size_metadata);
  int res = 0;
  memset(stbuf, 0, sizeof(struct stat));
  int file_no = file_number(path);

  /*Case 1: Check if DIR*/
  if (meta[file_no].type == -1) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    //printf("Return 0\n");
    return 0;
  }

  /*Case 2: Check if File*/
  else{
    //int file_no = file_number(path);
    if(file_no <meta_count && meta[file_no].type == 1){
      stbuf->st_mode = S_IFREG | 0777;
      stbuf->st_nlink = 1;
      stbuf->st_size = meta[file_no].length;
    }
    else if(file_no == meta_count){
      //printf("Value of i: %d\n", file_no);
      res = -ENOENT;
    }
  }
  //printf("RES: %d\n", res);
  //printf("OUTSIDE GETATTR\n\n");
  return res;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
  //printf("\nINSIDE READDIR\n");
  
  //printf("PATH : %s\n", path);
  //printf("BUF1 : %s\n", (char *) buf);
  (void) offset;
  (void) fi;
  int file_no;
  file_no = file_number(path);

  /*Check if directory or not*/
  if (meta[file_no].type != -1)
    return -ENOENT;
  
  filler(buf, ".", NULL, 0);
  filler(buf, "..", NULL, 0);
  char * file_dir;

  int len = strlen(path);
  /*If no '/' in the END then add it*/
  if(path[len-1] != '/')
    strcat(path, "/");
  
  int i;
  for(i=1; i<meta_count; i++){
    //printf("FILE(%d): %s\n", i, meta[i].file_path);
    file_dir = getDir(meta[i].file_path);
    //printf("FILEDIR(%d): %s\n", i, file_dir);
    if((strcmp(file_dir,path) == 0) && (meta[i].type != 0))
      filler(buf, meta[i].file_path + strlen(path), NULL, 0);
  }
  //printf("BUF2 : %s\n", (char *)buf);
  //printf("OUTSIDE READDIR\n");
  return 0;
}

static int hello_mkdir(const char *path, mode_t mode){
  //printf("\nINSIDE MKDIR\n");
  //int len = strlen(path);
  /*
    if(path[len-1] != '/')
    strcat(path, "/");
  */
  //printf("PATH %s\n", path);
  
  
  strcpy(meta[meta_count].file_path, path);
  meta[meta_count].type = -1; //this is directory
  //meta[meta_count].length = 0; //set the length 
  meta_count ++;
  
  //printf("LEAVING MKDIR\n");
  
  return 0;
}

static int hello_create(const char *path, mode_t mode, struct fuse_file_info *fi){
  //printf("Inside CREATE\n");
  strcpy(meta[meta_count].file_path, path);
  meta[meta_count].type = 1; //this is file
  meta[meta_count].length = 0; //set the length of file to 0 saying that file is empty and is just created

  meta_count ++;
  return 0;
}

static int hello_open(const char *path, struct fuse_file_info *fi)
{
  //printf("\nINSIDE OPEN\n");
  //printf("PATH : %s\n", path);
  //printf("FLAGS %d\n", fi->flags);
  int i, flag = 0;
  
  //Case 1: if path of file found then set the flag to 1
  for(i=0; i<meta_count; i++){
    if (strcmp(path, meta[i].file_path) == 0)
      flag = 1;
  }
  
  //Case 2: if path not found then retturn error
  if(flag==0)
    return -ENOENT;

  //check for readonly permission
  if ((fi->flags & 3) != O_RDONLY){
    //printf("OPEN FILE IN OTHER MODE\n");
    return 0;
  }
    //return -EACCES;
  //printf("OUTSIDE OPEN\n\n");
  return 0;
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
  //printf("\nINSIDE READ\n");
  //printf("PATH : %s\n", path);
  //printf("BUF1 : %s\n", buf);
  //printf("SIZE : %lu\n", size);
  //printf("OFFSET : %ld\n", offset);
  size_t len;
  (void) fi;

  int i, file_no;

  //printf("Calculating File no\n");
  file_no = file_number(path);
  //printf("File no is %d\n", file_no);
  
  //printf("\nFILE to be opened: %s\n", meta[file_no].file_path);
    
  len = meta[file_no].length; //length of opened file
  //printf("Length of file: %lu\n", len);

  /*Checks for valid offset*/
  if (offset < len) {

    /*Case 0: offset+size > len means reading till end then set exact bytes to be read*/
    if (offset + size > len)
      size = len - offset;

    /*Case 1: if request to read complete file from START(offset=0) to END(size=len)*/
    if(offset == 0 && size== len){
      int no_blk = size/4096; //number of blocks to read
      int last_offset = size % 4096; //bytes to read from last block
      int shift_offset = 0, i;
      /*Read the complete blocks*/
      for(i=0; i<no_blk; i++){
	memcpy(buf+shift_offset, meta[file_no].block[i] + mem_block, block_size);
	shift_offset = shift_offset + block_size;
      }
      /*Read the last remaining block bytes*/
      memcpy(buf+shift_offset, meta[file_no].block[i] + mem_block, last_offset);
      //printf("Size2: %lu\n", size);
      //printf("OUTSIDE READ\n\n");
      return size;
    }

    /*Case 2: If request to read from some offset i.e NOT FROM START*/
    if(offset > 0){
      int size1 = (size + offset) % block_size;
      int no_blk_to_shift = offset/block_size; //no of blk to shift
      int no_blk = (offset + size)/block_size; //no of blk to read completely
      int start_loc = offset % block_size; //start location of reading
      int no = block_size - start_loc; //bytes to be read from first block
      int shift_offset = 0; //stores shift offset of buffer

      /*Case 2.1 : Read request to read data from only one block*/
      if(no_blk_to_shift == no_blk)
	memcpy(buf+shift_offset, meta[file_no].block[no_blk_to_shift] + start_loc + mem_block, size);
	
      /*Case 2.2 : Read request to read from more than one block*/
      else{
	memcpy(buf+shift_offset, meta[file_no].block[no_blk_to_shift] + start_loc + mem_block, no);
	shift_offset = no;
	//size1 = size1 - no;
	for(i=no_blk_to_shift+1; i<no_blk; i++){
	  memcpy(buf+shift_offset, meta[file_no].block[i] + mem_block, block_size);
	  shift_offset = shift_offset + block_size;
	}
	memcpy(buf+shift_offset, meta[file_no].block[i] + mem_block, size1);
      }
    }
    //printf("Size2 :%lu\n", size);
    //printf("OUTSIDE READ\n\n");
    return size;
  }
  //else return 0 if offset was beyond the file
  return 0;
}


static int hello_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
  //printf("\nINSIDE WRITE\n");
  //printf("PATH : %s\n", path);
  //printf("BUF : %s", buf);
  //printf("SIZE : %lu\n", size);
  //printf("OFFSET : %ld\n", offset);

  int file_no = file_number(path);

  int len = meta[file_no].length;
  int current_no_blk = (meta[file_no].length-1)/block_size + 1;  //exact current number of blocks including half filled
  int no_blk_to_write = (offset+size-1)/block_size + 1; //exact no of blocks to write
  int start_loc = offset % block_size; //start location of writing
  int start_blk = offset/block_size; //starting block to write data
  int no = block_size - start_loc;  //no of bytes to write in first block
  //int size1 = (size+offset) % block_size; //no of bytes to write in last blk
  int i, shift_offset = 0;
  //int buf_size = buf;
  
  /*If empty file then number of blocks is 0*/
  if(len == 0)
    current_no_blk = 0;
  
  //printf("CURRENT BLOCKS: %d\n", current_no_blk);
  //printf("No BLK to WRITE: %d\n", no_blk_to_write);
  //printf("START BLOCK: %d\n", start_blk);
  //printf("START LOC: %d\n", start_loc);
  //printf("File block Offset: %d\n", meta[file_no].block[0]);
  //printf("Data block Offset: %d\n", data_block_offset);

  
  /*If the file requires new blocks then reserve the blocks first*/
  while(current_no_blk < no_blk_to_write){
    //printf("INSIDE WHILE\n");
    meta[file_no].block[current_no_blk] = data_block_offset;
    current_no_blk = current_no_blk + 1;
    data_block_offset = data_block_offset + block_size;
  }
  memcpy(meta[file_no].block[start_blk]+start_loc+mem_block, buf, no); //data to be written to first block
  //printf("FILE2 name: %s", meta[0].file_path + 1);
  shift_offset = shift_offset + no;
  /*Traverse till last full block and write data over there*/
  for(i=start_blk+1; i<no_blk_to_write; i++){
    //printf("Check 1\n");
    memcpy(meta[file_no].block[i] + mem_block, buf+shift_offset, block_size);
    //printf("FILE3 name: %s", meta[0].file_path + 1);
    shift_offset = shift_offset + block_size;
  }
  meta[file_no].length = offset + size; //set the length to current length      

  //printf("OUTSIDE WRITE\n\n");
  return size;
}

static int hello_unlink(const char * path){
  int file_no = file_number(path);
  //printf("INSIDE UNLINK\n");
  //printf("PATH: %s\n", path);

  //printf("Check 1\n");
  /*Case 1: If the file to be removed is not present then return ERROR*/
  if(file_no == meta_count)
    return -ENOENT;

  //printf("Check 2\n");
  /*Case 2: If the specified path matches a directory then return error*/
  if(meta[file_no].type == -1)
    return -ENOENT; ////////return different error

  //printf("Check 3\n");
  /*Case 3: If the path matches file then remove the file*/
  if(meta[file_no].type == 1){
    meta[file_no].type = 0; //mark the file as removed
    return 0;
  }

  //printf("OUTSIDE HELLO\n");
  return -ENOENT;
}

static int hello_rmdir(const char * path){
  int file_no = file_number(path);
  //printf("INSIDE RMDIR\n");
  //printf("PATH: %s\n", path);

  //printf("Check 1\n");
  /*Case 1: If the DIR to be removed is not present then return ERROR*/
  if(file_no == meta_count)
    return -ENOENT;

  //printf("Check 2\n");
  /*Case 2: If the specified path matches a file then return error*/
  if(meta[file_no].type == 1)
    return -ENOENT; ////////return different error

  //printf("Check 3\n");
  /*Case 3: If the path matches DIR then remove the DIR if it is empty*/
  if(meta[file_no].type == -1){
    bool isEmpty = isEmptyDir(path); //Check if Dir is empty or not

    /*Case 3.1: If DIR is empty then remove it*/
    if(isEmpty){
      meta[file_no].type = 0; //mark the file as removed
      return 0;
    }
    /*Case 3.2: If DIR not empty then return not empty*/
    else
      return -ENOTEMPTY; //return error as not empty
  }

  //printf("Leaving RMDIR\n");
  return -ENOENT;
}

static int hello_truncate(const char *path, off_t truncate_length){
  int file_no = file_number(path);
  //printf("INSIDE TRUNCATE\n");
  //printf("PATH: %s\n", path);
  
  printf("Check 1\n");
  /*Case 1: If the file to be removed is not present then return ERROR*/
  if(file_no == meta_count)
    return -ENOENT;
  
  //printf("Check 2\n");
  /*Case 2: If the specified path matches a DIR then return error*/
  if(meta[file_no].type == -1)
    return -ENOENT; ////////return different error

  //printf("Check 3\n");
  /*Case 3: If the path matches FILE then set the file length*/
  if(meta[file_no].type == 1){
    
    /*Case 3.1: If file > truncate len, then truncate the file to specified len*/
    if(meta[file_no].length >= truncate_length){
      meta[file_no].length = truncate_length; //set the file length to truncate length
      return 0;
    }
    /*Case 3.2: If truncate len greather than file_len*/
    else{
      //int len = meta[file_no].length;
      int current_no_blk = (meta[file_no].length-1)/block_size + 1;  //exact current number of blocks including half filled
      int no_blk_to_write = truncate_length/block_size + 1; //exact no of blocks to write

      /*If the file requires new blocks then reserve the blocks*/
      while(current_no_blk < no_blk_to_write){
	//printf("INSIDE WHILE\n");
	meta[file_no].block[current_no_blk] = data_block_offset;
	current_no_blk = current_no_blk + 1;
	data_block_offset = data_block_offset + block_size;
      }

      meta[file_no].length = truncate_length; //set the file length to truncate length
      return 0;
    }
  }
  //printf("Leaving TRUNCATE\n");
  return -ENOENT;
}


static void hello_destroy(){
  //printf("INSIDE DESTROY\n");
  if(persistent_file[0] == 0)
    return;
  int fd = open(persistent_file, O_WRONLY | O_CREAT,  S_IRWXU | S_IRUSR | S_IRWXG | S_IRWXO);
  meta[0].meta_count1 = meta_count;
  meta[0].data_block_offset1 = data_block_offset;
  //printf("[DEBUG] METACOUNT: %d\n", meta_count);
  //printf("[DEBUG] DATAOFFSET: %d\n", data_block_offset);
  write(fd, meta, SIZE);

  //printf("LEAVING DESTROY\n");
}

static struct fuse_operations hello_oper = {
  .getattr	= hello_getattr,
  .readdir	= hello_readdir,
  .open		= hello_open,
  .read		= hello_read,
  .create       = hello_create,
  .write        = hello_write,
  .mkdir        = hello_mkdir,
  .rename       = hello_rename,
  .unlink       = hello_unlink,
  .rmdir        = hello_rmdir,
  .truncate     = hello_truncate,
  .destroy      = hello_destroy,
};

int main(int argc, char *argv[])
{
  int ip_size = atoi(argv[2]); //Third argument in MAKE COMMAND
  if (argc ==4)
    strcpy(persistent_file, argv[3]);
  
  SIZE = ip_size*5*1024*1024;
  long actual_size = SIZE;
  mem_block = (void *)malloc(actual_size);
  
  int fd = open(persistent_file, O_RDONLY);
  //printf("FILE DESCRIPTOR: %d\n", fd);
  
  /*PERSISTENT FILESYSTEM FILE does not already exist*/
  if(fd < 0){
    //printf("CHECK -1\n");
    //memset(mem_block, 0, SIZE);
    long total_blocks = actual_size / block_size; //total number of blocks in FS
    //printf("CHECK 3\n");
    int ten_percent = total_blocks/10;  //10% for metedata
    size_metadata = ten_percent * block_size; //size of metadata
    meta_elements = size_metadata / sizeof(metadata_structure);
    data_block_offset = size_metadata; //current offset of storing file data
    //printf("CHECK 4\n");
    meta = (metadata_structure *)mem_block; //meta points to start of the complete structure
    /*Initialize the block offset of all blocks to -1 by default*/
    int blk, count1;
    //printf("CHECK 5\n");
    for(blk=0; blk<meta_elements; blk++){
      for(count1=0; count1<max_blocks_per_file; count1++){
	meta[blk].block[count1] = -1;
      }
    }

    /*Add entry for root DIR initially*/
    strcpy(meta[meta_count].file_path, "/");
    meta[meta_count].type = -1; //this is directory
    meta_count++;

    //printf("CHECK 6\n");
  }

  /*PERSISTENT FILESYSTEM file ALREADY exists*/
  else{
    //printf("INSIDE ELSE\n");
    int ret = read(fd, mem_block, SIZE);
    //printf("RETURN VAL of READ: %d\n", ret);
    meta = (metadata_structure *)mem_block;
    meta_count = meta[0].meta_count1;
    //printf("[DEBUG] METACOUNT: %d\n", meta_count);
    data_block_offset = meta[0].data_block_offset1;
    //printf("[DEBUG] DATAOFFSET: %d\n", data_block_offset);
    //printf("INITIALIZATION SUCCESSFUL\n");
  }
  
  //printf("Checkpoint 12\n");
  return fuse_main(2, argv, &hello_oper, NULL);
  //printf("Checkpoint 2\n");
}
