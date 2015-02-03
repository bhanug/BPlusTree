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
	LeafEntry leafEntry;
	IndexEntry *new_index_entry = NULL;
	Status s;

	leafEntry.key = key;
	leafEntry.rid = rid;
	s = do_insert(rootPid, leafEntry, new_index_entry);

	// root node was just split
	if (new_index_entry != NULL)
	{
		PageID pid;
		RecordID rid;
		SortedPage *page;
		BTIndexPage *newIndexPage;

		// Create a new root-node page
		MINIBASE_BM->NewPage(pid, (Page *&)page);
		MINIBASE_BM->PinPage(pid, (Page *&)page);
		newIndexPage = (BTIndexPage *)page;
		newIndexPage->Init(pid);
		newIndexPage->SetType(INDEX_NODE);
		newIndexPage->Insert(new_index_entry->key, new_index_entry->pid, rid);
		newIndexPage->SetLeftLink(rootPid);

		// Change the root node
		rootPid = pid;

		MINIBASE_BM->UnpinPage(pid, DIRTY);
		delete new_index_entry;
	}
	return s;
}

Status BTreeFile::do_insert(PageID pid, const LeafEntry leafEntry, IndexEntry * &new_index_entry)
{
	SortedPage *page;
	RecordID tRid;

	MINIBASE_BM->PinPage(pid, (Page *&)page);

	if (page->GetType() == INDEX_NODE)
	{
		BTIndexPage *indexPage = (BTIndexPage *)page;	// non-leaf node
		PageID childPid;
		IndexEntry index, indexSaved;

		// Choose a subtree
		indexPage->GetFirst(index.key, index.pid, tRid);

		if (index.key > leafEntry.key) {
			childPid = indexPage->GetLeftLink();
		}
		else {
			indexSaved = index;
			while (index.key < leafEntry.key) { // we don't support duplicate numbers!!!!1
				indexSaved = index;
				if (indexPage->GetNext(index.key, index.pid, tRid) == DONE)
					break;
			}
			childPid = indexSaved.pid;
		}

		MINIBASE_BM->UnpinPage(pid, CLEAN);

		// Recursion, insert entrys
		do_insert(childPid, leafEntry, new_index_entry);

		// after the Recursion return, we will go to here!
		if (new_index_entry == NULL)
		{
			return OK;
		}
		// We split child, must insert *new_index_entry into N
		else
		{
			MINIBASE_BM->PinPage(pid, (Page *&)page);
			indexPage = (BTIndexPage *)page;

			// Usual case ; there exists enough space
			if (indexPage->AvailableSpace() >= A_INDEX_NODE_SIZE)
			{
				// Insert new child into N

				if (indexPage->Insert(new_index_entry->key, new_index_entry->pid, tRid) != OK) {
					std::cout << "Error : insert to index node" << std::endl;
					std::cout << "number of records / slotnumber = " << page->GetNumOfRecords() << std::endl;
					std::cout << "AvailableSpace() = " << indexPage->AvailableSpace();
					std::cout << ", less than leafEntry + Slot = " << sizeof(IndexEntry) + 2 * sizeof(short) << std::endl;
					return FAIL;
				}
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
				BTIndexPage *newIndexPage;
				int n = indexPage->GetNumOfRecords();
				IndexEntry tEntry, *temp = new IndexEntry[n + 1];
				std::cout << "new allocate n temp IndexEntrys, n = " << n << std::endl;
				int i = 0, j = 0;
				bool insertFlag = true;

				// Allocate a new nonleaf-node page
				MINIBASE_BM->NewPage(pid2, (Page *&)page2);
				MINIBASE_BM->PinPage(pid2, (Page *&)page2);
				newIndexPage = (BTIndexPage *)page2;
				newIndexPage->Init(pid2);
				newIndexPage->SetType(INDEX_NODE);

				while (!indexPage->IsEmpty())
				{
					indexPage->GetFirst(tEntry.key, tEntry.pid, tRid);
					if (insertFlag && tEntry.key > new_index_entry->key)
					{
						temp[i] = *new_index_entry;
						i = i + 1;
						insertFlag = false;
					}
					temp[i] = tEntry;
					i = i + 1;
					indexPage->Delete(tEntry.key, tRid);
				}

				if (insertFlag)
				{
					temp[i] = *new_index_entry;
					i = i + 1;
				}

				for (; j < n/2; j++)
				{
					indexPage->Insert(temp[j].key, temp[j].pid, tRid);
				}

				newIndexPage->SetLeftLink(temp[j].pid);

				// because of the property of B+ Tree, No dumplicate number in Index Node
				// so, jump j
				int splited_new_entry = j;
				j++;
				for (; j < i; j++)
				{
					newIndexPage->Insert(temp[j].key, temp[j].pid, tRid);
				}

				// *newchildentry set to guide searches btwn N and N2
				delete new_index_entry;
				new_index_entry = new IndexEntry;
				new_index_entry->key = temp[splited_new_entry].key;
				new_index_entry->pid = pid2;

				MINIBASE_BM->UnpinPage(pid, DIRTY);
				MINIBASE_BM->UnpinPage(pid2, DIRTY);

				return OK;
			}
		}
	}
	else
	{
		BTLeafPage *leafPage = (BTLeafPage *)page;	// leaf node

		// Usual case
		if (leafPage->AvailableSpace() >= A_LEAF_NODE_SIZE)
		{
			if (leafPage->Insert(leafEntry.key, leafEntry.rid, tRid) != OK) {
				std::cout << "Error in insert to leaf" << std::endl;
				std::cout << "number of records / slotnumber = " << page->GetNumOfRecords() << std::endl;
				std::cout << "AvailableSpace() = " << leafPage->AvailableSpace();
				std::cout << ", less than leafEntry + Slot = " << sizeof(LeafEntry)+2*sizeof(short) << std::endl;
				return FAIL;
			}
			MINIBASE_BM->UnpinPage(pid, DIRTY);
			return OK;
		}
		// In the case that the leaf is full
		else
		{
			PageID pidNew, siblingPid;
			SortedPage *page2, *sPage;
			BTLeafPage *newLeafPage, *sibling;
			int n = leafPage->GetNumOfRecords();
			LeafEntry tEntry, *temp = new LeafEntry[n + 1];
			//std::cout << "new allocate n temp LeafEntrys, n = " << n << std::endl;
			int i = 0, j = 0;
			bool insertFlag = true;

			// Allocate a new leaf-node page
			MINIBASE_BM->NewPage(pidNew, (Page *&)page2);
			MINIBASE_BM->PinPage(pidNew, (Page *&)page2);
			newLeafPage = (BTLeafPage *)page2;
			newLeafPage->Init(pidNew);
			newLeafPage->SetType(LEAF_NODE);

			// Split the old leafPage
			while (!leafPage->IsEmpty())
			{
				leafPage->GetFirst(tEntry.key, tEntry.rid, tRid);
				if (insertFlag && tEntry.key > leafEntry.key)
				{
					temp[i] = leafEntry;
					i = i + 1;
					insertFlag = false;
				}
				temp[i] = tEntry;
				i = i + 1;
				leafPage->Delete(tEntry.key, tEntry.rid, tRid);
			}

			if (insertFlag)
			{
				temp[i] = leafEntry;
				i = i + 1;
			}

			for (j = 0; j < n/2; j++)
			{
				leafPage->Insert(temp[j].key, temp[j].rid, tRid);
			}

			for (; j < i; j++)
			{
				newLeafPage->Insert(temp[j].key, temp[j].rid, tRid);
			}

			// Set *newchildentry
			delete new_index_entry;
			new_index_entry = new IndexEntry;
			new_index_entry->key = temp[n/2].key;
			new_index_entry->pid = pidNew;

			// Set sibling pointers
			siblingPid = leafPage->GetNextPage();

			if (siblingPid != -1)
			{
				MINIBASE_BM->PinPage(siblingPid, (Page *&)sPage);
				sibling = (BTLeafPage *)sPage;
				sibling->SetPrevPage(pidNew);
				MINIBASE_BM->UnpinPage(siblingPid, DIRTY);
			}

			leafPage->SetNextPage(pidNew);
			newLeafPage->SetPrevPage(pid);
			newLeafPage->SetNextPage(siblingPid);

			MINIBASE_BM->UnpinPage(pid, DIRTY);
			MINIBASE_BM->UnpinPage(pidNew, DIRTY);
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

		N->GetFirst(Ki1.key, Ki1.pid, tRid);

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

		//std::cout << "Ki = " << Ki.key << "  ki1 = " << Ki1.key << std::endl;
		//std::cout << "i = " << i << std::endl;

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

		// Recursively, delete entry
		do_delete(pid, Pi, entry, oldchildentry);

		//  everything is OK, Didn't delete any Index node
		if (oldchildentry == NULL)
		{
			return OK;
		}
		// Cao!!! delete some Index node
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
				std::cout << "Change root" << std::endl;
				rootPid = N->GetLeftLink();
				std::cout << "new rootpid = " << rootPid << std::endl;
				if (oldchildentry != NULL) {
					delete oldchildentry;
				}
				oldchildentry = NULL;
				MINIBASE_BM->UnpinPage(pid, DIRTY);
				MINIBASE_BM->FreePage(pid);
				return OK;
			}
			// Check for underflow
			else if ((N->IsAtLeastHalfFull() == true) || pid == rootPid)
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

				// Bring element from the right sibling
				if (P->GetLeftLink() == pid) // if this page is the left-most
				{
					P->GetFirst(right.key, right.pid, tRid);

					MINIBASE_BM->PinPage(right.pid, (Page *&)Spage);
					S = (BTIndexPage *)Spage;

					// S has extra entries
					if (S->IsAtLeastHalfFull() == true)
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
				// Bring element from the left sibling
				else
				{
					P->GetFirst(right.key, right.pid, tRid);
					left.pid = P->GetLeftLink();
					while (right.pid != pid)
					{
						left = right;
						P->GetNext(right.key, right.pid, tRid);
					}

					MINIBASE_BM->PinPage(left.pid, (Page *&)Spage);
					S = (BTIndexPage *)Spage;

					// S has extra entries
					if (S->IsAtLeastHalfFull() == true)
					{
						// Redistribution
						IndexEntry  tEntrySaved;
						S->GetFirst(tEntry.key, tEntry.pid, tRid);
						tEntrySaved = tEntry;
						while (S->GetNext(tEntry.key, tEntry.pid, tRid) != DONE) {
							tEntrySaved = tEntry;
						}
						S->Delete(tEntrySaved.key, tRid);

						P->Delete(right.key, tRid);
						P->Insert(tEntrySaved.key, right.pid, tRid);

						N->Insert(right.key, N->GetLeftLink(), tRid);
						N->SetLeftLink(tEntrySaved.pid);

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

		if ((L->IsAtLeastHalfFull() == true) || pid == rootPid)
		{
			std::cout << "delete leaf / root page element" << std::endl;
			if (oldchildentry != NULL) {
				delete oldchildentry;
			}
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

			// only the most left node will get element from its right sibling,
			// other nodes are always get from its left sibling
			if (P->GetLeftLink() == pid) // the most left node
			{
				P->GetFirst(right.key, right.pid, tRid);

				MINIBASE_BM->PinPage(right.pid, (Page *&)Spage);
				S = (BTLeafPage *)Spage;

				// S has extra entries
				if (S->IsAtLeastHalfFull() == true)
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
			// all nodes, except the most-left one, will get element from its left sibling
			else
			{
				P->GetFirst(right.key, right.pid, tRid);
				left.pid = P->GetLeftLink();
				while (right.pid != pid)
				{
					left = right;
					P->GetNext(right.key, right.pid, tRid);
				}

				std::cout << "left.pid = " << left.pid << " right = " << right.pid << std::endl;

				MINIBASE_BM->PinPage(left.pid, (Page *&)Spage);
				S = (BTLeafPage *)Spage;

				// S has extra entries
				if (S->IsAtLeastHalfFull() == true)
				{
					// Redistribution
					LeafEntry tEntrySaved;
					S->GetFirst(tEntry.key, tEntry.rid, tRid);
					tEntrySaved = tEntry;
					while (S->GetNext(tEntry.key, tEntry.rid, tRid) != DONE) {
						tEntrySaved = tEntry;
					}
					if (S->Delete(tEntrySaved.key, tEntrySaved.rid, tRid) == FAIL) {
						std::cout << "delete key = " << tEntrySaved.key << "FAIL!!" << std::endl;
					}

					L->Insert(tEntrySaved.key, tEntrySaved.rid, tRid);

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
					tPid = L->GetNextPage();

					if (tPid != -1)
					{
						SortedPage *tPage;
						BTLeafPage *tS;
						MINIBASE_BM->PinPage(tPid, (Page *&)tPage);
						tS = (BTLeafPage *)tPage;
						tS->SetPrevPage(left.pid);
						MINIBASE_BM->UnpinPage(tPid, DIRTY);
					}

					S->SetNextPage(tPid);

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
