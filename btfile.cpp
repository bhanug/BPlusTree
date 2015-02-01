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
	Page *rootPage;

	// filename contains the name of the BTreeFile to be opened
	if (MINIBASE_DB->GetFileEntry(filename, rootPid) == OK)
	{
		MINIBASE_BM->PinPage(rootPid, rootPage);
	}
	// create a new B+ tree index, add a new file entry into database
	else
	{
		MINIBASE_BM->NewPage(rootPid, rootPage);
		MINIBASE_DB->AddFileEntry(filename, rootPid);
		((SortedPage *)rootPage)->Init(rootPid);

		// initialize the type of the page
		((SortedPage *)rootPage)->SetType(LEAF_NODE);
	}
	returnStatus = OK;
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
	LeafEntry entry;
	IndexEntry *new_index = NULL;
	Status s;

	entry.key = key;
	entry.rid = rid;
	s = do_insert(rootPid, entry, new_index);
	return OK;
}

Status BTreeFile::do_insert(PageID pid, const LeafEntry entry, IndexEntry * &new_index)
{
	SortedPage *page;
	RecordID tRid;

	MINIBASE_BM->PinPage(pid, (Page *&)page);

	if (page->GetType() == INDEX_NODE)
	{
		BTIndexPage *N = (BTIndexPage *)page;	// non-leaf node
		PageID Pi;
		IndexEntry Ki, Ki1;
		int i = 0;

		// Choose a subtree

		N->GetFirst(Ki1.key, Ki1.pid, tRid);

		while (Ki1.key < entry.key)
		{
			i++;
			Ki = Ki1;

			if (N->GetNext(Ki1.key, Ki1.pid, tRid) == DONE)
			{
				//No more entires
				break;
			}
		}

		///////////////////
		if (i == 0)
		{
			Pi = N->GetLeftLink();
		}
		else
		{
			Pi = Ki.pid;
		}

		MINIBASE_BM->UnpinPage(pid, CLEAN);

		// Recursively, insert entry
		InsertAux(Pi, entry, newchildentry);

		// Usual case; didn't split child
		if (newchildentry == NULL)
		{
			return OK;
		}
		// We split child, must insert *newchildentry in N
		else
		{
			MINIBASE_BM->PinPage(pid, (Page *&)page);
			N = (BTIndexPage *)page;

			// Usual case ; there exists enough space
			if (N->GetNumOfRecords() < 2 * treeOrder)
			{
				// Insert new child into N
				N->Insert(newchildentry->key, newchildentry->pid, tRid);
				MINIBASE_BM->UnpinPage(pid, DIRTY);
				// Set newchildentry to NULL
				delete newchildentry;
				newchildentry = NULL;
				return OK;
			}
			// Split node ; no enough space
			else
			{
				PageID pid2;
				SortedPage *page2;
				BTIndexPage *N2;
				IndexEntry tEntry, *temp = new IndexEntry[2 * treeOrder + 1];
				int i = 0, j = 0;
				bool insertFlag = true;

				// Allocate a new nonleaf-node page
				MINIBASE_BM->NewPage(pid2, (Page *&)page2);
				MINIBASE_BM->PinPage(pid2, (Page *&)page2);
				N2 = (BTIndexPage *)page2;
				N2->Init(pid2);
				N2->SetType(INDEX_NODE);

				// Split N
				N->GetFirst(tEntry.key, tEntry.pid, tRid);

				while (!N->IsEmpty())
				{
					N->GetFirst(tEntry.key, tEntry.pid, tRid);
					if (insertFlag && tEntry.key > newchildentry->key)
					{
						temp[i] = *newchildentry;
						i = i + 1;
						insertFlag = false;
					}
					temp[i] = tEntry;
					i = i + 1;
					N->Delete(tEntry.key, tRid);
				}

				if (insertFlag)
				{
					temp[i] = *newchildentry;
					i = i + 1;
				}

				for (; j < treeOrder; j++)
				{
					N->Insert(temp[j].key, temp[j].pid, tRid);
				}

				N2->SetLeftLink(temp[j].pid);

				for (; j < i; j++)
				{
					N2->Insert(temp[j].key, temp[j].pid, tRid);
				}

				// *newchildentry set to guide searches btwn N and N2
				delete newchildentry;
				newchildentry = new IndexEntry;
				newchildentry->key = temp[treeOrder].key;
				newchildentry->pid = pid2;

				MINIBASE_BM->UnpinPage(pid, DIRTY);
				MINIBASE_BM->UnpinPage(pid2, DIRTY);

				return OK;
			}
		}
	}
	else
	{
		BTLeafPage *L = (BTLeafPage *)page;	// leaf node

		// Usual case
		if (L->GetNumOfRecords() < 2 * treeOrder)
		{
			L->Insert(entry.key, entry.rid, tRid);
			MINIBASE_BM->UnpinPage(pid, DIRTY);
			return OK;
		}
		// In the case that the leaf is full
		else
		{
			PageID pid2, Spid;
			SortedPage *page2, *Spage;
			BTLeafPage *L2, *S;
			LeafEntry tEntry, *temp = new LeafEntry[2 * treeOrder + 1];
			int i = 0, j = 0;
			bool insertFlag = true;

			// Allocate a new leaf-node page
			MINIBASE_BM->NewPage(pid2, (Page *&)page2);
			MINIBASE_BM->PinPage(pid2, (Page *&)page2);
			L2 = (BTLeafPage *)page2;
			L2->Init(pid2);
			L2->SetType(LEAF_NODE);

			// Split L
			while (!L->IsEmpty())
			{
				L->GetFirst(tEntry.key, tEntry.rid, tRid);
				if (insertFlag && tEntry.key > entry.key)
				{
					temp[i] = entry;
					i = i + 1;
					insertFlag = false;
				}
				temp[i] = tEntry;
				i = i + 1;
				L->Delete(tEntry.key, tEntry.rid, tRid);
			}

			if (insertFlag)
			{
				temp[i] = entry;
				i = i + 1;
			}

			for (j = 0; j < treeOrder; j++)
			{
				L->Insert(temp[j].key, temp[j].rid, tRid);
			}

			for (; j < i; j++)
			{
				L2->Insert(temp[j].key, temp[j].rid, tRid);
			}

			// Set *newchildentry
			delete newchildentry;
			newchildentry = new IndexEntry;
			newchildentry->key = temp[treeOrder].key;
			newchildentry->pid = pid2;

			// Set sibling pointers
			Spid = L->GetNextPage();

			if (Spid != -1)
			{
				MINIBASE_BM->PinPage(Spid, (Page *&)Spage);
				S = (BTLeafPage *)Spage;
				S->SetPrevPage(pid2);
				MINIBASE_BM->UnpinPage(Spid, DIRTY);
			}

			L->SetNextPage(pid2);
			L2->SetPrevPage(pid);
			L2->SetNextPage(Spid);

			MINIBASE_BM->UnpinPage(pid, DIRTY);
			MINIBASE_BM->UnpinPage(pid2, DIRTY);
		}
	}

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
