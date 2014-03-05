
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

#define RM_CANNOT_FIND_ATTRIBUTE 80
#define RM_CANNOT_FIND 81

typedef unsigned AttrNumber;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
  RBFM_ScanIterator rbfm_si;
};

class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key); 	// Get next matching entry
  RC close();             			// Terminate index scan
  IX_ScanIterator ix_scanIterator;
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuples(const string &tableName);

  RC deleteTuple(const string &tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);
  // never reorganize the first two pages
  RC reorganizePage(const string &tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);
  // never reorganize the first two pages
  RC reorganizeTable(const string &tableName);



protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  char tuple[PAGE_SIZE];
  Attribute headVersionAttribute;
  // tools
  // add the version to data
  void addVersion2Data(void *verData, const void *data,
		  const VersionNumber &ver, const unsigned &recordSize);
  RC openTable(const string &tableName, FileHandle *&fileHandle);
  RC closeTable(const string &tableName);
  void makeIndexName(const string &tableName, const string &attributeName,
		  string &indexName);
  RC openIndex(const string &tableName,
		  const string &attributeName, FileHandle *&fileHandle);
  RC closeIndex(const string &tableName, const string &attributeName);
  bool isIndexOpen(const string &tableName, const string &attributeName);
  RC insertIndex(const string &tableName, const RID &rid);
  RC deleteIndex(const string &tableName, const RID &rid);
  RC deleteIndices(const string &tableName);
  // get the record size: start from the second attr excluding the Ver
  unsigned getRecordSize(const void *formattedData,
		  const vector<Attribute> &recordDescriptor);
  // get specific attribute
  RC getSpecificAttribute(const string &tableName, const string &attributeName, Attribute &attr);
  unordered_map<string, FileHandle *> cachedTableFileHandles;
  unordered_map<string, FileHandle*> cachedIndexFileHandles;
  unordered_map<string, Attribute> cachedAttributes;
};

#endif
