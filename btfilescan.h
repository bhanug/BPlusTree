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

};

#endif
