#ifndef _BTFILE_H
#define _BTFILE_H

#include "btindex.h"
#include "btleaf.h"
#include "index.h"
#include "btfilescan.h"
#include "bt.h"

const int treeOrder = 4;

class BTreeFile: public IndexFile {
	
public:
	
	friend class BTreeFileScan;

	BTreeFile(Status& status, const char *filename);
	~BTreeFile();
	
	Status DestroyFile();
	
	Status Insert(const int key, const RecordID rid); 
	Status Delete(const int key, const RecordID rid);
    
	IndexFileScan *OpenScan(const int *lowKey, const int *highKey);
	
	Status Print();
	Status DumpStatistics();

private:
	
	// You may add members and methods here.

	PageID      rootPid;
	
	Status PrintTree(PageID pid);
	Status PrintNode(PageID pid);
	Status do_insert(PageID pid, const LeafEntry entry, IndexEntry * &new_index);
	Status do_delete(PageID Ppid, PageID pid, const LeafEntry entry, IndexEntry *&oldchildentry);
};


#endif // _BTFILE_H
