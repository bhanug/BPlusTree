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
		std::cout << "create a new B+ Tree" << std::endl;
		MINIBASE_BM->NewPage(rootPid, rootPage);
		if (MINIBASE_DB->AddFileEntry(filename, rootPid) != OK) {
			std::cout << "error in AddFileEntry()" << std::endl;
		}
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
	IndexEntry *new_index_entry = NULL;
	Status s;

	entry.key = key;
	entry.rid = rid;
	s = do_insert(rootPid, entry, new_index_entry);

	// root node was just split
	if (new_index_entry != NULL)
	{
		PageID Rpid;
		RecordID tRid;
		SortedPage *Rpage;
		BTIndexPage *R;

		// Create a new root-node page
		MINIBASE_BM->NewPage(Rpid, (Page *&)Rpage);
		MINIBASE_BM->PinPage(Rpid, (Page *&)Rpage);
		R = (BTIndexPage *)Rpage;
		R->Init(Rpid);
		R->SetType(INDEX_NODE);
		R->Insert(new_index_entry->key, new_index_entry->pid, tRid);
		R->SetLeftLink(rootPid);

		// Change the root node
		rootPid = Rpid;

		MINIBASE_BM->UnpinPage(Rpid, DIRTY);
		delete new_index_entry;
	}
	return s;
}

Status BTreeFile::do_insert(PageID pid, const LeafEntry entry, IndexEntry * &new_index_entry)
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
		do_insert(Pi, entry, new_index_entry);

		// after the Recursion return, we will go to here!
		// Usual case; didn't split child
		if (new_index_entry == NULL)
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
				N->Insert(new_index_entry->key, new_index_entry->pid, tRid);
				MINIBASE_BM->UnpinPage(pid, DIRTY);
				// Set newchildentry to NULL
				delete new_index_entry;
				new_index_entry = NULL;
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
					if (insertFlag && tEntry.key > new_index_entry->key)
					{
						temp[i] = *new_index_entry;
						i = i + 1;
						insertFlag = false;
					}
					temp[i] = tEntry;
					i = i + 1;
					N->Delete(tEntry.key, tRid);
				}

				if (insertFlag)
				{
					temp[i] = *new_index_entry;
					i = i + 1;
				}

				for (; j < treeOrder; j++)
				{
					N->Insert(temp[j].key, temp[j].pid, tRid);
				}

				N2->SetLeftLink(temp[j].pid);

				// I modify here
				int splited_new_entry = j;
				j++;
				for (; j < i; j++)
				{
					N2->Insert(temp[j].key, temp[j].pid, tRid);
				}

				// *newchildentry set to guide searches btwn N and N2
				delete new_index_entry;
				new_index_entry = new IndexEntry;
				new_index_entry->key = temp[treeOrder].key;
				new_index_entry->pid = pid2;

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
			delete new_index_entry;
			new_index_entry = new IndexEntry;
			new_index_entry->key = temp[treeOrder].key;
			new_index_entry->pid = pid2;

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
	// To do
	LeafEntry entry;
	IndexEntry *oldchildentry = NULL;

	entry.key = key;
	entry.rid = rid;

	return do_delete(-1, rootPid, entry, oldchildentry);
}

Status 
BTreeFile::do_delete(PageID Ppid, PageID pid, const LeafEntry entry, IndexEntry *&oldchildentry)
{
	SortedPage *page;
	RecordID tRid;

	MINIBASE_BM->PinPage(pid, (Page *&)page);

	// Index node
	if (page->GetType() == INDEX_NODE)
	{
		BTIndexPage *N = (BTIndexPage *)page;	// non-leaf node
		PageID Pi;
		IndexEntry Ki, Ki1;
		int i = 0;

		// Choose a subtree

		N->GetFirst(Ki1.key, Ki1.pid, tRid);

		std::cout << "ki1.key = " << Ki1.key << std::endl;

		while (Ki1.key <= entry.key)
		{
			i++;
			Ki = Ki1;

			if (N->GetNext(Ki1.key, Ki1.pid, tRid) == DONE)
			{
				//No more entires
				break;
			}
		}

		std::cout << "Ki = " << Ki.key << "  ki1 = " << Ki1.key << std::endl;
		std::cout << "i = " << i << std::endl;

		///////////////////
		if (i == 0)
		{
			Pi = N->GetLeftLink();
			std::cout << "get left link = " << Pi << std::endl;
		}
		else
		{
			Pi = Ki.pid;
		}

		MINIBASE_BM->UnpinPage(pid, CLEAN);

		// Recursively, delete entry
		do_delete(pid, Pi, entry, oldchildentry);

		// Usual case : Do not delete child node
		if (oldchildentry == NULL)
		{
			return OK;
		}
		// Discard child node
		else
		{
			MINIBASE_BM->PinPage(pid, (Page *&)page);
			N = (BTIndexPage *)page;
			std::cout << "we will delete this key in parent node : " << oldchildentry->key << std::endl;
			N->Delete(oldchildentry->key, tRid);
			std::cout << "after delete, the number of records = " << N->GetNumOfRecords() << std::endl;

			// Change root
			if (pid == rootPid && N->GetNumOfRecords() <= 0)
			{
				rootPid = N->GetLeftLink();
				// reset to the LEAF_NODE
				BTIndexPage *new_root_page;
				MINIBASE_BM->PinPage(rootPid, (Page *&)new_root_page);
				new_root_page->SetType(LEAF_NODE);
				MINIBASE_BM->UnpinPage(rootPid);

				delete oldchildentry;
				oldchildentry = NULL;

				MINIBASE_BM->UnpinPage(pid, DIRTY);
				MINIBASE_BM->FreePage(pid);
				return OK;
			}
			// Check for underflow
			else if (N->GetNumOfRecords() >= treeOrder || pid == rootPid)
			{
				delete oldchildentry;
				oldchildentry = NULL;
				MINIBASE_BM->UnpinPage(pid, DIRTY);
				return OK;
			}
			else
			{
				//Get a sibling S of N
				SortedPage *Spage, *Ppage;
				BTIndexPage *S, *P;
				IndexEntry left, right, tEntry;

				MINIBASE_BM->PinPage(Ppid, (Page *&)Ppage);
				P = (BTIndexPage *)Ppage;
				P->GetFirst(left.key, left.pid, tRid);
				P->GetNext(right.key, right.pid, tRid);

				// Bring right sibling
				if (P->GetLeftLink() == pid || left.pid == pid)
				{
					if (P->GetLeftLink() == pid)
					{
						right = left;
					}

					MINIBASE_BM->PinPage(right.pid, (Page *&)Spage);
					S = (BTIndexPage *)Spage;

					// S has extra entries
					if (S->GetNumOfRecords() > treeOrder)
					{
						PageID tPid;

						// Redistribution
						tPid = S->GetLeftLink();

						S->GetFirst(tEntry.key, tEntry.pid, tRid);
						S->Delete(tEntry.key, tRid);
						S->SetLeftLink(tEntry.pid);

						P->Delete(right.key, tRid);
						P->Insert(tEntry.key, right.pid, tRid);

						N->Insert(right.key, tPid, tRid);

						// Set oldchildentry to null
						oldchildentry = NULL;

						MINIBASE_BM->UnpinPage(pid, DIRTY);
						MINIBASE_BM->UnpinPage(Ppid, DIRTY);
						MINIBASE_BM->UnpinPage(right.pid, DIRTY);
						return OK;
					}
					//Merge N and S (M = S)
					else
					{
						// Set oldchildentry
						delete oldchildentry;
						oldchildentry = new IndexEntry;
						*oldchildentry = right;

						// Pull splitting key from parent
						N->Insert(right.key, S->GetLeftLink(), tRid);

						// Move all entries from M
						while (!S->IsEmpty())
						{
							S->GetFirst(tEntry.key, tEntry.pid, tRid);
							N->Insert(tEntry.key, tEntry.pid, tRid);
							S->Delete(tEntry.key, tRid);
						}

						MINIBASE_BM->UnpinPage(pid, DIRTY);
						MINIBASE_BM->UnpinPage(Ppid, DIRTY);
						MINIBASE_BM->UnpinPage(right.pid, DIRTY);

						// Discard empty node M
						MINIBASE_BM->FreePage(right.pid);
						return OK;
					}
				}
				// Bring left sibling
				else
				{
					while (right.pid != pid)
					{
						left = right;
						P->GetNext(right.key, right.pid, tRid);
					}

					MINIBASE_BM->PinPage(left.pid, (Page *&)Spage);
					S = (BTIndexPage *)Spage;

					// S has extra entries
					if (S->GetNumOfRecords() > treeOrder)
					{
						// Redistribution
						S->GetFirst(tEntry.key, tEntry.pid, tRid);
						while (S->GetNext(tEntry.key, tEntry.pid, tRid) != DONE)
							;
						S->Delete(tEntry.key, tRid);

						P->Delete(right.key, tRid);
						P->Insert(tEntry.key, right.pid, tRid);

						N->Insert(right.key, N->GetLeftLink(), tRid);
						N->SetLeftLink(tEntry.pid);

						// Set oldchildentry to null
						oldchildentry = NULL;

						MINIBASE_BM->UnpinPage(pid, DIRTY);
						MINIBASE_BM->UnpinPage(Ppid, DIRTY);
						MINIBASE_BM->UnpinPage(left.pid, DIRTY);
						return OK;
					}
					// Merge S and N  (M = N)
					else
					{
						// Set oldchildentry
						delete oldchildentry;
						oldchildentry = new IndexEntry;
						*oldchildentry = right;

						// Pull splitting key from parent
						S->Insert(right.key, N->GetLeftLink(), tRid);

						// Move all entries from M
						while (!N->IsEmpty())
						{
							N->GetFirst(tEntry.key, tEntry.pid, tRid);
							S->Insert(tEntry.key, tEntry.pid, tRid);
							N->Delete(tEntry.key, tRid);
						}
						// Discard empty node M
						MINIBASE_BM->UnpinPage(pid, DIRTY);
						MINIBASE_BM->UnpinPage(Ppid, DIRTY);
						MINIBASE_BM->UnpinPage(left.pid, DIRTY);

						MINIBASE_BM->FreePage(pid);
						return OK;
					}
				}
			}
		}
	}

	// Leaf node
	else
	{
		BTLeafPage *L = (BTLeafPage *)page;	// leaf node

		L->Delete(entry.key, entry.rid, tRid);

		if (L->GetNumOfRecords() >= treeOrder || pid == rootPid)
		{
			std::cout << "delete leaf / root page element" << std::endl;
			delete oldchildentry;
			oldchildentry = NULL;
			MINIBASE_BM->UnpinPage(pid, DIRTY);
			return OK;
		}
		else
		{
			//Get a sibling S of N
			SortedPage *Spage, *Ppage;
			BTLeafPage *S;
			BTIndexPage *P;
			IndexEntry left, right;
			LeafEntry tEntry;

			MINIBASE_BM->PinPage(Ppid, (Page *&)Ppage);
			P = (BTIndexPage *)Ppage;
			P->GetFirst(left.key, left.pid, tRid);
			P->GetNext(right.key, right.pid, tRid);

			std::cout << "pid = " << pid << ", left.pid = " << left.pid << "  right.pid = " << right.pid << std::endl;

			// Bring right sibling
			if (P->GetLeftLink() == pid || left.pid == pid)
			{
				if (P->GetLeftLink() == pid)
				{
					right = left;
				}
				MINIBASE_BM->PinPage(right.pid, (Page *&)Spage);
				S = (BTLeafPage *)Spage;

				// S has extra entries
				if (S->GetNumOfRecords() > treeOrder)
				{
					// Redistribution
					S->GetFirst(tEntry.key, tEntry.rid, tRid);
					S->Delete(tEntry.key, tEntry.rid, tRid);

					L->Insert(tEntry.key, tEntry.rid, tRid);

					S->GetFirst(tEntry.key, tEntry.rid, tRid);

					P->Delete(right.key, tRid);
					P->Insert(tEntry.key, right.pid, tRid);

					// Set oldchildentry to null
					oldchildentry = NULL;

					MINIBASE_BM->UnpinPage(pid, DIRTY);
					MINIBASE_BM->UnpinPage(Ppid, DIRTY);
					MINIBASE_BM->UnpinPage(right.pid, DIRTY);
					return OK;
				}
				//Merge N and S (M = S)
				else
				{
					PageID tPid;

					// Set oldchildentry
					delete oldchildentry;
					oldchildentry = new IndexEntry;
					*oldchildentry = right;

					// Move all entries from M
					while (!S->IsEmpty())
					{
						S->GetFirst(tEntry.key, tEntry.rid, tRid);
						L->Insert(tEntry.key, tEntry.rid, tRid);
						S->Delete(tEntry.key, tEntry.rid, tRid);
					}

					// Adjust sibling pointers
					tPid = S->GetNextPage();

					if (tPid != -1)
					{
						SortedPage *tPage;
						BTLeafPage *tS;
						MINIBASE_BM->PinPage(tPid, (Page *&)tPage);
						tS = (BTLeafPage *)tPage;
						tS->SetPrevPage(pid);
						MINIBASE_BM->UnpinPage(tPid, DIRTY);
					}

					L->SetNextPage(tPid);

					MINIBASE_BM->UnpinPage(pid, DIRTY);
					MINIBASE_BM->UnpinPage(Ppid, DIRTY);
					MINIBASE_BM->UnpinPage(right.pid, DIRTY);

					// Discard empty node M
					MINIBASE_BM->FreePage(right.pid);
					return OK;
				}
			}
			// Bring left sibling
			else
			{
				while (right.pid != pid)
				{
					left = right;
					P->GetNext(right.key, right.pid, tRid);
				}

				MINIBASE_BM->PinPage(left.pid, (Page *&)Spage);
				S = (BTLeafPage *)Spage;

				// S has extra entries
				if (S->GetNumOfRecords() > treeOrder)
				{
					// Redistribution
					S->GetFirst(tEntry.key, tEntry.rid, tRid);
					while (S->GetNext(tEntry.key, tEntry.rid, tRid) != DONE)
						;
					S->Delete(tEntry.key, tEntry.rid, tRid);

					L->Insert(tEntry.key, tEntry.rid, tRid);

					L->GetFirst(tEntry.key, tEntry.rid, tRid);

					P->Delete(right.key, tRid);
					P->Insert(tEntry.key, right.pid, tRid);

					// Set oldchildentry to null
					oldchildentry = NULL;

					MINIBASE_BM->UnpinPage(pid, DIRTY);
					MINIBASE_BM->UnpinPage(Ppid, DIRTY);
					MINIBASE_BM->UnpinPage(left.pid, DIRTY);
					return OK;
				}
				// Merge S and N  (M = N)
				else
				{
					PageID tPid;

					// Set oldchildentry
					delete oldchildentry;
					oldchildentry = new IndexEntry;
					*oldchildentry = right;

					// Move all entries from M
					while (!L->IsEmpty())
					{
						L->GetFirst(tEntry.key, tEntry.rid, tRid);
						S->Insert(tEntry.key, tEntry.rid, tRid);
						L->Delete(tEntry.key, tEntry.rid, tRid);
					}

					// Adjust sibling pointers
					tPid = L->GetPrevPage();

					if (tPid != -1)
					{
						SortedPage *tPage;
						BTLeafPage *tS;
						MINIBASE_BM->PinPage(tPid, (Page *&)tPage);
						tS = (BTLeafPage *)tPage;
						tS->SetNextPage(left.pid);
						MINIBASE_BM->UnpinPage(tPid, DIRTY);
					}

					S->SetPrevPage(tPid);

					MINIBASE_BM->UnpinPage(pid, DIRTY);
					MINIBASE_BM->UnpinPage(Ppid, DIRTY);
					MINIBASE_BM->UnpinPage(left.pid, DIRTY);

					// Discard empty node M
					MINIBASE_BM->FreePage(pid);
					return OK;
				}
			}

		}
	}
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
	BTreeFileScan* bTFileScan = new BTreeFileScan;
	PageID pid;
	Status s;

	bTFileScan->highKey = highKey;
	bTFileScan->lowKey = lowKey;
	bTFileScan->tree = this;
	bTFileScan->firstTime = true;

	IndexEntry index_entry, t_entry;
	RecordID t_rid;

	int temp = 0;
	SortedPage *page;

	if (lowKey != NULL)
	{
		pid = rootPid;

		MINIBASE_BM->PinPage(rootPid, (Page *&)page);
		while (page->GetType() == INDEX_NODE)
		{
			((BTIndexPage *)page)->GetFirst(index_entry.key, index_entry.pid, t_rid);
			MINIBASE_BM->UnpinPage(pid, CLEAN);
			////////////////
			temp = 0;
			while (index_entry.key < *lowKey)
			{
				temp++;
				t_entry = index_entry;

				if (((BTIndexPage *)page)->GetNext(index_entry.key, index_entry.pid, t_rid) == DONE)
				{
					break;
				}
			}

			///////////////////
			if (!temp)
			{
				pid = ((BTIndexPage *)page)->GetLeftLink();
			}
			else
			{
				index_entry = t_entry;
				pid = index_entry.pid;
			}

			MINIBASE_BM->PinPage(pid, (Page *&)page);
		}
		//bTFileScan->curLeaf = (BTLeafPage *&)page;
		MINIBASE_BM->UnpinPage(pid, CLEAN);
		s = MINIBASE_BM->PinPage(pid, (Page *&)bTFileScan->curLeaf);
		bTFileScan->cur_pid = pid;
		if (s != OK) return NULL;
	}
	else
	{
		PageID nextPid = rootPid;
		MINIBASE_BM->PinPage(nextPid, (Page *&)page);

		while (page->GetType() == INDEX_NODE)
		{
			MINIBASE_BM->UnpinPage(nextPid, CLEAN);
			nextPid = ((BTIndexPage*&)page)->GetLeftLink();
			MINIBASE_BM->PinPage(nextPid, (Page *&)page);
		}
		MINIBASE_BM->UnpinPage(nextPid, CLEAN);


		s = MINIBASE_BM->PinPage(nextPid, (Page *&)bTFileScan->curLeaf);
		bTFileScan->cur_pid = nextPid;
		if (s != OK) return NULL;

	}

	return bTFileScan;
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
		std::cout << "Page ID = " << pageID << " is a INDEX_NODE" << std::endl;
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
	else {
		std::cout << "Page ID = " << pageID << " is a LEAF_NODE" << std::endl;
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
