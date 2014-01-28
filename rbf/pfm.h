#ifndef _pfm_h_
#define _pfm_h_

#include <cstdio>
#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <queue>
#include <cstdlib>
#include <sys/stat.h>

using namespace std;

typedef int RC;
#define UNKNOWN_FAILURE -1
#define SUCC 0
#define FILE_EXIST 1
#define FILE_NOT_EXIST 2
#define FILE_OPEN_FAILURE 3
#define FILE_REMOVE_FAILURE 4
#define FILE_NOT_OPEN_BY_HANDLE 5
#define FILE_STREAM_FAILURE 6

#define PAGE_NOT_EXIST 100

// define buffer management error code
#define BUFFER_FILE_NOT_HIT 10000
#define BUFFER_PAGENUM_NOT_HIT 10001
// define Page list error code
#define FILE_SPACE_NO_SPACE 20
#define FILE_SPACE_EMPTY 21

typedef unsigned PageNum;

#define PAGE_SIZE 4096
#define PAGE_ALMOST_FULL_RATIO	5	// defines the ratio that a page is almost full
#define PAGE_LEVEL 4 				// defines # of lists for different degrees of free space
#define MAX_PAGE_IN_MEM 1024		// defines the maximum # of pages opened in a file 262144

// define pages for table data
#define TABLE_PAGES_NUM 6


class FileHandle;

/*
 * File information to main the reference counter
 */
struct FileInfo {
	FILE *fp;
	int cnt;
	FileInfo(FILE *f, int c) : fp(f), cnt(c){}
	FileInfo(const FileInfo &fi) : fp(fi.fp) , cnt(fi.cnt) {}
	FileInfo & operator=(const FileInfo &fi) {
		if (this == &fi)
			return *this;
		this->fp = fi.fp;
		this->cnt = fi.cnt;
		return *this;
	}
};
/*
 * Heap file management
 */
struct PageSpaceInfo {
	unsigned short freeSpaceSize;
	unsigned pageNum;
	PageSpaceInfo & operator=(const PageSpaceInfo &p) {
		if (&p == this)
			return *this;
		this->freeSpaceSize = p.freeSpaceSize;
		this->pageNum = p.pageNum;
		return *this;
	}
	PageSpaceInfo(const PageSpaceInfo &p) : freeSpaceSize(p.freeSpaceSize),
			pageNum(p.pageNum) {}
	PageSpaceInfo(unsigned f, unsigned p) : freeSpaceSize(f), pageNum(p) {}
private:
	PageSpaceInfo();
};

class spaceComparator {
public:
	bool operator() (const PageSpaceInfo &lhs, const PageSpaceInfo &rhs) {
		return lhs.freeSpaceSize < rhs.freeSpaceSize;
	}
};

class FileSpaceManager {
private:
	FileSpaceManager();
	priority_queue<PageSpaceInfo, vector<PageSpaceInfo>, spaceComparator> pageQueue;
public:
	FileSpaceManager(FileHandle &fileHandle);
	// Get the page number with the greatest free space size
	RC getPageSpaceInfo(const unsigned &recordSize, PageNum &pageNum);
	// Pop the page space info
	RC popPageSpaceInfo();
	// Push the page space info
	RC pushPageSpaceInfo(const unsigned &freeSpaceSize, const unsigned &pageNum);
	// Clear all the page space info
	void clearPageSpaceInfo();
};
typedef unordered_map<string, FileSpaceManager> MultipleFilesSpaceManager;
/*
 * Paged File Manager
 */
class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file
    void PrintFileStreamError(const string & str) {
    #ifdef __DEBUG__
    	perror(str.c_str());
    #endif
    }
    void PrintError(const string & str) {
    #ifdef __DEBUG__
    	cerr << str << endl;
    #endif
    }
    MultipleFilesSpaceManager filesSpaceManager; // manage the file space
protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
    bool fileExist(const char *fileName);
    unordered_map<string, FileInfo> refCounter;			// count each file's reference
    void inc_refCounter(const string &fileName, FileHandle &fileHandle);		// reference increment
    void dec_refCounter(const string &fileName);		// reference decrement
    int get_refCounter(const string &fileName, FILE *&fp);		// get reference of a file
    void close_refCounter(const string &fileName);	// close the reference counter given file name
    void closeFileZeroRef();						// try to close those files with zero reference
};

/*
 * File handle
 */

class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file

    FILE * pFile;
    string fileName;													// file name of the handler
    unsigned getSpaceOfPage(PageNum pageNum, void *page);			// Get the free space associated with page num
private:
    FileHandle & operator=(const FileHandle &);			// prevent accidentally copy class
    FileHandle(const FileHandle &);						// prevent copy constructor
 };

#endif
