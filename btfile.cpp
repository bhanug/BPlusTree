#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"


//-------------------------------------------------------------------
// BTreeFile::BTreeFile
//
// Input   : filename - filename of an index.  
// Output  : returnStatus - status of execution of constructor. 
//           OK if successful, FAIL otherwise.
// Purpose : If the B+ tree exists, open it.  Otherwise create a
//           new B+ tree index.
//-------------------------------------------------------------------

BTreeFile::BTreeFile (Status& returnStatus, const char *filename) 
{

}


//-------------------------------------------------------------------
// BTreeFile::~BTreeFile
//
// Input   : None 
// Output  : None
// Purpose : Clean Up
//-------------------------------------------------------------------

BTreeFile::~BTreeFile()
{

}


//-------------------------------------------------------------------
// BTreeFile::DestroyFile
//
// Input   : None
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Delete the entire index file by freeing all pages allocated
//           for this BTreeFile.
//-------------------------------------------------------------------

Status 
BTreeFile::DestroyFile()
{
	return OK;
}


//-------------------------------------------------------------------
// BTreeFile::Insert
//
// Input   : key - pointer to the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.  
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------


Status 
BTreeFile::Insert (const int key, const RecordID rid)
{
	return OK;
}


//-------------------------------------------------------------------
// BTreeFile::Delete
//
// Input   : key - pointer to the value of the key to be deleted.
//           rid - RecordID of the record to be deleted.
// Output  : None
// Return  : OK if successful, FAIL otherwise. 
// Purpose : Delete an index entry with this rid and key.  
// Note    : If the root becomes empty, delete it.
//-------------------------------------------------------------------

Status 
BTreeFile::Delete (const int key, const RecordID rid)
{
	return OK;
}


//-------------------------------------------------------------------
// BTreeFile::OpenScan
//
// Input   : lowKey, highKey - pointer to keys, indicate the range
//                             to scan.
// Output  : None
// Return  : A pointer to IndexFileScan class.
// Purpose : Initialize a scan.  
// Note    : Usage of lowKey and highKey :
//
//           lowKey   highKey   range
//			 value	  value	
//           --------------------------------------------------
//           NULL     NULL      whole index
//           NULL     !NULL     minimum to highKey
//           !NULL    NULL      lowKey to maximum
//           !NULL    =lowKey   exact match (may not be unique)
//           !NULL    >lowKey   lowKey to highKey
//-------------------------------------------------------------------

IndexFileScan *
BTreeFile::OpenScan(const int *lowKey, const int *highKey)
{
	return NULL;
}


//-------------------------------------------------------------------
// BTreeFile::PrintTree
//
// Input   : pageID - root of the tree to print.
// Output  : None
// Return  : None
// Purpose : Print out the content of the tree rooted at pid.
//-------------------------------------------------------------------

Status 
BTreeFile::PrintTree (PageID pageID)
{ 
	SortedPage *page;
	BTIndexPage *index;
	Status s;
	PageID curPageID;
	RecordID curRid;
	int  key;


	PIN (pageID, page);
	NodeType type = (NodeType) page->GetType ();
	
	if (type == INDEX_NODE) 
	{
		index = (BTIndexPage *)page;
		curPageID = index->GetLeftLink();
		PrintTree(curPageID);
		s = index->GetFirst (key, curPageID, curRid);
		while (s != DONE)
		{	
			PrintTree(curPageID);
			s = index->GetNext(key, curPageID, curRid);
		}
		UNPIN(pageID, CLEAN);
		PrintNode(pageID);
	} 
	return OK;
}

//-------------------------------------------------------------------
// BTreeFile::PrintNode
//
// Input   : pageID - the node to print.
// Output  : None
// Return  : None
// Purpose : Print out the content of the node pid.
//-------------------------------------------------------------------

Status 
BTreeFile::PrintNode (PageID pageID)
{
	
	char filename[50]="c:\\temp\\BTREENODES.TXT";
	SortedPage *page;
	BTIndexPage *index;
	BTLeafPage *leaf;
	int i;
	Status s;
	PageID curPageID;
	RecordID currRid;
	int  key;
	RecordID dataRid;

	std::ofstream os(filename, std::ios::app);
	
	PIN (pageID, page);
	NodeType type = (NodeType) page->GetType ();
	i = 0;
	switch (type) 
	{
		case INDEX_NODE:
			index = (BTIndexPage *)page;
			curPageID = index->GetLeftLink();
			os << "\n---------------- Content of index node " << pageID << "-----------------------------" << std::endl;
			os << "\n Left most PageID:  "  << curPageID << std::endl;
			s = index->GetFirst (key, curPageID, currRid); 
			while ( s != DONE)
			{	
				i++;
				os <<  "Key: " << key << "	PageID: " << curPageID  << std::endl;
				s = index->GetNext(key, curPageID, currRid);
			}
			os << "\n This page contains  " << i <<"  entries." << std::endl;
			break;
		
		case LEAF_NODE:
			leaf = (BTLeafPage *)page;
			s = leaf->GetFirst (key, dataRid, currRid);
			while ( s != DONE)
			{	
				os << "\nContent of leaf node"  << pageID << std::endl;
				os << "DataRecord ID: " << dataRid << " Key: " << key << std::endl;
				s = leaf->GetNext(key, dataRid, currRid);
				i++;
			}
			os << "\n This page contains  " << i <<"  entries." << std::endl;
			break;
	}
	UNPIN(pageID, CLEAN);
	os.close();

	return OK;	
}

//-------------------------------------------------------------------
// BTreeFile::Print
//
// Input   : None
// Output  : None
// Return  : None
// Purpose : Print out this B+ Tree
//-------------------------------------------------------------------

Status 
BTreeFile::Print()
{	
	char filename[50]="c:\\temp\\BTREENODES.TXT";	
	std::ofstream os(filename, std::ios::app);	

	os << "\n\n-------------- Now Begin Printing a new whole B+ Tree -----------"<< std::endl;

	if (PrintTree(rootPid)== OK)
		return OK;
	return FAIL;
}

//-------------------------------------------------------------------
// BTreeFile::DumpStatistics
//
// Input   : None
// Output  : None
// Return  : None
// Purpose : Print out the following statistics.
//           1. Total number of leaf nodes, and index nodes.
//           2. Total number of leaf entries.
//           3. Total number of index entries.
//           4. Mean, Min, and max fill factor of leaf nodes and 
//              index nodes.
//           5. Height of the tree.
//-------------------------------------------------------------------
Status 
BTreeFile::DumpStatistics()
{	
	return OK;
}
