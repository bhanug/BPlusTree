#ifndef _BTREE_FILESCAN_H
#define _BTREE_FILESCAN_H

#include "btfile.h"

class BTreeFile;

class BTreeFileScan : public IndexFileScan {
	
public:
	
	friend class BTreeFile;

	Status GetNext (RecordID &rid,  int &key);
	Status DeleteCurrent ();

	~BTreeFileScan();
	
private:
	BTLeafPage *curLeaf;
	PageID cur_pid;

	const int *highKey;
	const int *lowKey;
	bool firstTime;
	int curKey;
	RecordID cur_rid;
	RecordID t_rid;
	Status status;

};

#endif
