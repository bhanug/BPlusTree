#include <string.h>
#include "btindex.h"


//-------------------------------------------------------------------
// BTIndexPage::Insert
//
// Input   : key - pointer to the key value to be inserted.
//           pageID - page id associated to that key.
// Output  : rid - record id of the (key, pageID) record inserted.
// Purpose : Insert the pair (key, pageID) into this index node.
// Return  : OK if insertion is succesfull, FAIL otherwise.
//-------------------------------------------------------------------

Status 
BTIndexPage::Insert (const int key, const PageID pageID, RecordID &rid)
{
	IndexEntry entry;
	Status s;
	
	entry.key = key;
	entry.pid = pageID;

	s = SortedPage::InsertRecord((char *)&entry, sizeof(IndexEntry), rid);
	if (s != OK)
	{
		std::cerr << "Fail to insert record into SortedPage\n";
		return FAIL;
	}
	
	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::Delete
//
// Input   : key  - pointer to the key value to be inserted.
// Output  : rid - record id of the (key, pageID) record deleted.
// Purpose : Delete the entry associated with key from this index node.
// Return  : OK if deletion is succesfull, FAIL otherwise. If FAIL is
//           returned, rid may contain garbage.
//-------------------------------------------------------------------

Status 
BTIndexPage::Delete (const int key, RecordID &rid)
{
	int i;
	IndexEntry *entry;
	
	// Scan through all the records in this page and find the
	// matching pair (key, dataRid).

	for (i = numOfSlots - 1; i >= 0; i--)
	{
		entry = (IndexEntry *)(data + slots[i].offset);
		if (entry->key == key)
		{
			// We delete it here.

			Status s;
			
			rid.pageNo = PageNo();
			rid.slotNo = i;
			s = SortedPage::DeleteRecord(rid);

			return s;
		}
	}
	
	return FAIL;
}


//-------------------------------------------------------------------
// BTIndexPage::GetFirst
//
// Input   : None
// Output  : rid - record id of the first entry
//           firstKey - pointer to the key value
//           firstPid - the page id
// Purpose : get the first pair (firstKey, firstPid) in the index page 
//           and it's rid.
// Return  : OK if a record is returned,  DONE if no record exists on 
//           this page.
//-------------------------------------------------------------------

Status 
BTIndexPage::GetFirst (int &firstKey, PageID &firstPid, RecordID &rid)
{
	// Initialize the record id of the first (key, dataRid) pair.  The
	// first record is always at slot position 0, since SortedPage always
	// compact it's records.  We can also use HeapPage::FirstRecord here
	// but it is not neccessary.

	rid.pageNo = this->pid;
	rid.slotNo = 0;

	// If there are no record in this page, just return DONE.
	
	if (numOfSlots == 0)
	{
		rid.pageNo = INVALID_PAGE;
		rid.slotNo = INVALID_SLOT;
		return DONE;
	}
	
	// Otherwise, we just copy the record into key and dataRid,
	// and returned.  The record is located at offset slots[0].offset
	// from the beginning of data area (pointed to by member data in 
	// HeapPage)

	IndexEntry entry;
	memcpy(&entry, (IndexEntry *)(data + slots[0].offset), sizeof(IndexEntry));
	firstKey = entry.key;
	firstPid = entry.pid;
	
	return OK;

}


//-------------------------------------------------------------------
// BTIndexPage::GetNext
//
// Input   : rid - record id of the current entry
// Output  : rid - record id of the next entry
//           nextKey - the key value of next entry
//           nextPid - the page id of next entry
// Purpose : Get the next pair (nextKey, nextPid) in the index page and 
//           it's rid.
// Return  : OK if there is a next record, DONE if no more.  If DONE is
//           returned, rid is set to invalid.
//-------------------------------------------------------------------

Status 
BTIndexPage::GetNext (int &nextKey, PageID &nextPid, RecordID &rid)
{
	// If we are at the end of records, return DONE.

	if (rid.slotNo + 1 >= numOfSlots)
	{
		rid.pageNo = INVALID_PAGE;
		rid.slotNo = INVALID_SLOT;
		return DONE;
	}

	// Increment the slotNo in rid to point to the next record in this
	// page.  We can do this for subclass of SorterPage since the records
	// in a sorted page is always compact.  
	
	rid.slotNo ++;

	// Otherwise, we just copy the record into key and dataRid,
	// and returned.  The record is located at offset 
	// slots[rid.slotNo].offset from the beginning of data area 
	// (pointed to by member data in HeapPage)

	IndexEntry entry;
	memcpy(&entry, (IndexEntry *)(data + slots[rid.slotNo].offset), sizeof(IndexEntry));
	nextKey = entry.key;
	nextPid = entry.pid;
	
	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::GetLeftLink
//
// Input   : None
// Output  : None
// Purpose : Return the page id of the page at the left of this page.
// Return  : The page id of the page at the left of this page.
//-------------------------------------------------------------------

PageID BTIndexPage::GetLeftLink ()
{
	return GetPrevPage();
}


//-------------------------------------------------------------------
// BTIndexPage::SetLeftLink
//
// Input   : pageID - new left link
// Output  : None
// Purpose : Set the page id of the page at the left of this page.
// Return  : None
//-------------------------------------------------------------------

void BTIndexPage::SetLeftLink (PageID pageID)
{
	SetPrevPage(pageID);
}


