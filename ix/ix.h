
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <unordered_map>
#include <string>
#include <unordered_set>


#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

// define return code error
#define IX_SLOT_DIR_OVERFLOW 60
#define IX_NOT_ENOUGH_SPACE 61
#define IX_SLOT_DIR_LESS_ZERO 62
#define IX_READ_DUP_PAGE 63
// define return code warning
#define IX_SEARCH_LOWER_BOUND 70
#define IX_SEARCH_UPPER_BOUND 71
#define IX_SEARCH_HIT 72
#define IX_SEARCH_HIT_MED 73
#define IX_FAILTO_ALLOCATE_PAGE 74

// define duplicate flag
typedef bool Dup;
/*
 * The structure of a leaf frame
 * key			variable
 * RID			8bytes
 * Dup	bool	1byte
 * The structure of a nonleaf frame
 * key			variable
 * PageNum		4bytes	prev
 * PageNum		4bytes	next
 */
// define the index slot directory
typedef unsigned short Offset;
typedef unsigned short RecordLength;
struct IndexDir{
	Offset slotOffset;
	RecordLength recordLength;
};

// define the slot to be delete for dup record page
const Offset DUP_SLOT_IN_USE = 0;
const Offset DUP_SLOT_DEL = 1;
const PageNum DUP_PAGENUM_END = -1;


// define the free space length
typedef unsigned long Address;
typedef char IsLeaf;
const char CONST_NOT_LEAF = 0;
const char CONST_IS_LEAF = 1;
const char CONST_IS_DUP_PAGE = 2;
const int DUP_RECORD_SIZE = sizeof(RID)*2;

// define the begin/end of page
const PageNum EOF_PAGE_NUM = -1;
// define the page number of a page
const PageNum ROOT_PAGE = 0;


class IX_ScanIterator;

class IndexManager {
 public:
  static IndexManager* instance();

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Insert new index entry
  RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Delete index entry

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  RC scan(FileHandle &fileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;
 public:
  // api to handle an index page
  void setPageEmpty(void *page);
  bool isPageEmpty(void *page);

  // free space operation
  void* getFreeSpaceStartPoint(const void *page);
  Offset getFreeSpaceOffset(const void *page);
  void setFreeSpaceStartPoint(void *page, const void *point);
  int getFreeSpaceSize(const void *page);

  // leaf flag operation
  IsLeaf isPageLeaf(const void *page);
  void setPageLeaf(void *page, const IsLeaf &isLeaf);

  // set/get prev/next page number
  PageNum getPrevPageNum(const void *page);
  PageNum getNextPageNum(const void *page);
  void setPrevPageNum(void *page, const PageNum &pageNum);
  void setNextPageNum(void *page, const PageNum &pageNum);

  // slot number
  SlotNum getSlotNum(const void *page);
  void setSlotNum(void *page, const SlotNum &slotNum);

  // slot dir
  RC getIndexDir(const void *page,
		  IndexDir &indexDir,
		  const SlotNum &slotNum);
  RC setIndexDir(void *page,
		  const IndexDir &indexDir,
		  const SlotNum &slotNum);

  // get a key's size
  int getKeySize(const Attribute &attr, const void *key);

  /*
   * Insert/delete/search an entry
   */
  // Insert new index entry
  RC insertEntry(const PageNum &pageNum,
		  FileHandle &fileHandle,
		  const Attribute &attribute,
		  const void *key, const RID &rid,
		  void *copiedUpKey, bool &copiedUp);
  // Delete index entry
  RC deleteEntry(const PageNum &pageNum,
		  FileHandle &fileHandle,
		  const Attribute &attribute,
		  const void *key, const RID &rid);

  // binary search an entry
  RC binarySearchEntry(const void *page,
		  const Attribute &attr,
		  const void *key,
		  SlotNum &slotNum);
  // insert an entry into page at pos n
  RC insertEntryAtPos(void *page, const SlotNum &slotNum,
		  const Attribute &attr,
		  const void *key, const unsigned &keyLen,
		  const RID &rid, const Dup &dup,
		  const PageNum &prevPageNum,
		  const PageNum &nextPageNum);
  // delete an entry of page at pos n
  RC deleteEntryAtPos(void *page, const SlotNum &slotNum);

  /*
   * Split/merge pages
   */
  RC splitPageLeaf(void *oldPage,
		  void *pageLeft, void *pageRight,
		  const Attribute &attr,
		  const void *key, const RID &rid,
		  void *keyCopiedUp);
  RC splitPageNonLeaf(void *oldPage,
		  void *pageLeft, void *pageRight,
		  const Attribute &attr,
		  const void *key, const PageNum &nextPageNum,
		  void *keMovedUp);
  RC mergePage(void *srcPage, void *destPage,
		  const Attribute &attr);


  // compare two key
  // negative: <; positive: >; equal: =;
  int compareKey(const Attribute &attr,
		  const void *lhs, const void *rhs);


  // for debug only
  void printPage(const void *page, const Attribute &attr);
  void printLeafPage(const void *page, const Attribute &attr);
  void printNonLeafPage(const void *page, const Attribute &attr);
  void printDupPage(const void *page);
  void printKey(const Attribute &attr,
		  const void *key);
  char Entry[PAGE_SIZE];
  char block[PAGE_SIZE];
};

class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan
};

//TODO
// print out the error message for a given return code
void IX_PrintError (RC rc);

/*
 * 		in charge of half used (for overflow)
 * 		and empty page
 * 		next dup RID; RID in database
 */
class SpaceManager {
public:
	static SpaceManager* instance();
	static SpaceManager *_space_manager;
protected:
	SpaceManager();
	~SpaceManager();
private:
	unordered_map<string, unordered_set<PageNum> >dupRecordPageList;
	unordered_map<string, unordered_set<PageNum> >emptyPageList;
public:
	RC initIndexFile(const string &indexFileName);
	void closeIndexFileInfo(const string &indexFileName);
	// given first duplicated head RID and data RID
	// insert the dup record with assigned RID in index
	// NOTE: first use: need config dupHeadRid.PageNum = DUP_PAGENUM_END
	RC insertDupRecord(FileHandle &fileHandle,
			const RID &dupHeadRID,
			const RID &dataRID,
			RID &dupAssignedRID);
	RC deleteDupRecord(FileHandle &fileHandle,
			RID &dupHeadRID,
			const RID &dataRID);
	// get dup record page
	RC getDupPage(FileHandle &fileHandle,
			PageNum &pageNum);
	// put an available dup page to the list
	// Note: must check the space availability before inserting
	void putDupPage(FileHandle &fileHandle,
			const PageNum &pageNum);
	// check if a dup page exists
	bool dupPageExist(FileHandle &fileHandle,
			const PageNum &pageNum);
	// get an empty page if there is one
	// if no empty page exists, new one and return that
	RC getEmptyPage(FileHandle &fileHandle,
			PageNum &pageNum);
	RC putEmptyPage(FileHandle &fileHandle,
			const PageNum &pageNum);
private:
	char page[PAGE_SIZE];
	char prevPage[PAGE_SIZE];
};


#endif
