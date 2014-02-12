
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

// define return code error
#define IX_SLOT_DIR_OVERFLOW 60
#define IX_NOT_ENOUGH_SPACE 61
#define IX_SLOT_DIR_LESS_ZERO 62
// define return code warning
#define IX_LOWER_BOUND 70
#define IX_UPPER_BOUND 71
#define IX_HIT 72
#define IX_ABOVE 73
#define IX_BELOW 74

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

// define the free space length
typedef unsigned long Address;
typedef bool IsLeaf;

// define the begin/end of page
const PageNum EOF_PAGE_NUM = -1;


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

 public:
  static IndexManager *_index_manager;
  // api to handle an index page
  void setPageEmpty(void *page);

  // free space operation
  void* getFreeSpaceStartPoint(const void *page);
  Offset getFreeSpaceOffset(const void *page);
  void setFreeSpaceStartPoint(void *page, const void *point);
  int getFreeSpaceSize(const void *page);

  // set/get prev/next page number
  PageNum getPrevPageNum(const void *page);
  PageNum getNextPageNum(const void *page);
  void setPrevPageNum(void *page, const PageNum &pageNum);
  void setNextPageNum(void *page, const PageNum &pageNum);

  // leaf flag operation
  bool isPageLeaf(const void *page);
  void setPageLeaf(void *page, const bool &isLeaf);

  // slot number
  SlotNum getSlotNum(const void *page);
  void setSlotNum(void *page, const SlotNum &slotNum);

  // slot dir
  RC getSlotDir(const void *page,
		  IndexDir &indexDir,
		  const SlotNum &slotNum);
  RC setSlotDir(void *page,
		  const IndexDir &indexDir,
		  const SlotNum &slotNum);

  // get a key's size
  int getKeySize(const Attribute &attr, const void *key);

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

  // compare two key
  // negative: <; positive: >; equal: =;
  int compareKey(const Attribute &attr,
		  const void *lhs, const void *rhs);


  // for debug only
  void printPage(const void *page, const Attribute &attr);
  void printLeafPage(const void *page, const Attribute &attr);
  void printNonLeafPage(const void *page, const Attribute &attr);
  void printKey(const Attribute &attr,
		  const void *key);
  char Entry[PAGE_SIZE];
};

class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
