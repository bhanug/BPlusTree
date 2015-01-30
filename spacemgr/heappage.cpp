#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "heappage.h"
#include "bufmgr.h"
#include "db.h"


//------------------------------------------------------------------
// Constructor of HeapPage
//
// Input     : Page ID
// Output    : None
//------------------------------------------------------------------

void HeapPage::Init(PageID pageNo)
{
	prevPage = INVALID_PAGE;
	nextPage = INVALID_PAGE;
	this->pid = pageNo;
	fillPtr = sizeof(data);
	freeSpace = sizeof(data);
	numOfSlots = 0;
	SLOT_SET_EMPTY(slots[0]);
}

void HeapPage::SetNextPage(PageID pageNo)
{
    nextPage = pageNo;
}

void HeapPage::SetPrevPage(PageID pageNo)
{
    prevPage = pageNo;
}

PageID HeapPage::GetNextPage()
{
    return nextPage;
}

PageID HeapPage::GetPrevPage()
{
    return prevPage;
}


//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length 
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page
// Return    : OK if everything went OK, DONE if sufficient space 
//             does not exist
//------------------------------------------------------------------

Status HeapPage::InsertRecord(char *recPtr, int length, RecordID& rid)
{
	// check if there is enough space.

	short spaceNeeded;
	spaceNeeded = length + sizeof(Slot);

   	if (spaceNeeded > freeSpace)
	{
		return DONE;
	}

	// OK, we have enough space.  Prepare for insertion

	fillPtr -= length;
	freeSpace -= length;

	// Now look for an empty slot.
	
	int i;
	for (i = 0; i < numOfSlots; i++)
	{
		if (SLOT_IS_EMPTY(slots[i]))
		{
			// Found an empty slot. Use it.

			SLOT_FILL(slots[i], fillPtr, length);
			break;
		}
	}

	if (i == numOfSlots)
	{
		// No more empty slots. Create new one.
	
		numOfSlots++;
		freeSpace -= sizeof(Slot);
		SLOT_FILL(slots[i], fillPtr, length);
	}

	// Next, copy the record into the data area.
	
	memcpy(&data[fillPtr], recPtr, length);

	// Output the record id
	
	rid.pageNo = pid;
	rid.slotNo = i;
	
    return OK;
}


//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID
// Output   : None
// Purpose  : Delete a record from the page
// Return   : OK if successful, FAIL otherwise  
//------------------------------------------------------------------ 

Status HeapPage::DeleteRecord(const RecordID& rid)
{
	short offset, len;

	// Check validity of rid

	if (rid.slotNo >= numOfSlots || rid.slotNo < 0)
	{
		std::cerr << "Invalid Slot No " << rid.slotNo << std::endl;
		return FAIL;
	}

	if (SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		std::cerr << "Slot " << rid.slotNo << " is empty." << std::endl;
		return FAIL;
	}

	// ASSERT : rid is valid.

	offset = slots[rid.slotNo].offset;
	len = slots[rid.slotNo].length;


	if (rid.slotNo == numOfSlots - 1)
	{
		// If we delete the record which occupy the last slot,
		// we can remove any empty slots at the end of the
		// slots directory.

		numOfSlots --;
		freeSpace += sizeof(Slot);
		while (SLOT_IS_EMPTY(slots[numOfSlots - 1]) && numOfSlots != 0)
		{
			numOfSlots --;
			freeSpace += sizeof(Slot);
		}
	}
	else
	{
		SLOT_SET_EMPTY(slots[rid.slotNo]);
	}

	// Now move all shift all records.  Expensive but we assume
	// deletion is rare.

	memmove( &data[fillPtr+len], &data[fillPtr], offset - fillPtr );

	// Update the slots directory.

	for (int i = 0; i < numOfSlots; i++)
	{
		if (slots[i].offset < offset)
		{
			slots[i].offset += len;
		}
	}

	freeSpace += len;
	fillPtr += len;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None
// Output   : record id of the first record on a page
// Purpose  : To find the first record on a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------

Status HeapPage::FirstRecord(RecordID& rid)
{
	int i;

	for (i = 0; i < numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY (slots[i]))
		{
			rid.slotNo = i;
			rid.pageNo = pid;
			return OK;
		}
	}

	return DONE;
}


//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record
// Output   : ID of the next record
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK
//------------------------------------------------------------------

Status HeapPage::NextRecord (RecordID curRid, RecordID& nextRid)
{
	int i;

	// Check validity of rid

	if (curRid.slotNo >= numOfSlots || curRid.slotNo < 0)
	{
		std::cerr << "Invalid Slot No " << curRid.slotNo << std::endl;
		return FAIL;
	}

	for (i = curRid.slotNo + 1; i < numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY (slots[i]))
		{
			nextRid.slotNo = i;
			nextRid.pageNo = pid;
			return OK;
		}
	}

	return DONE;
}


//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : Record ID
// Output   : Records length and a copy of the record itself
// Purpose  : To retrieve a _copy_ of a record with ID rid from a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------

Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& length)
{
	// Check validity of rid

	if (rid.slotNo >= numOfSlots || rid.slotNo < 0)
	{
		std::cerr << "Invalid Slot No " << rid.slotNo << std::endl;
		return FAIL;
	}

	if (SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		std::cerr << "Slot " << rid.slotNo << " is empty." << std::endl;
		return FAIL;
	}

	length = slots[rid.slotNo].length;
	memcpy (recPtr, &data[slots[rid.slotNo].offset], length);

    return OK;
}


//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID
// Output   : pointer to the record, record's length
// Purpose  : To output a _pointer_ to the record
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& length)
{
	// Check validity of rid

	if (rid.slotNo >= numOfSlots || rid.slotNo < 0)
	{
		std::cerr << "Invalid Slot No " << rid.slotNo << std::endl;
		return FAIL;
	}

	if (SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		std::cerr << "Slot " << rid.slotNo << " is empty." << std::endl;
		return FAIL;
	}

	length = slots[rid.slotNo].length;
	recPtr = &data[slots[rid.slotNo].offset];

    return OK;
}


//------------------------------------------------------------------
// HeapPage::AvailableSpace
//
// Input    : None
// Output   : None
// Purpose  : To return the amount of available space
// Return   : The amount of available space on the heap file page.
//------------------------------------------------------------------

int HeapPage::AvailableSpace(void)
{
	return freeSpace - sizeof(Slot);
}


//------------------------------------------------------------------
// HeapPage::IsEmpty
// 
// Input    : None
// Output   : None
// Purpose  : Check if there is any record in the page.
// Return   : TRUE if the HeapPage is empty, and false otherwise.
//------------------------------------------------------------------

Bool HeapPage::IsEmpty(void)
{
	// Since we shrink the slot directory when we delete,
	// an empty must have numOfSlots == 0.

	return (numOfSlots == 0);
}


void HeapPage::CompactSlotDir()
{
	int curr;
	int first;
	Bool move = FALSE;

	curr = 0;
	first = -1;

	while (curr < numOfSlots)
	{
		if (SLOT_IS_EMPTY(slots[curr]) && move == FALSE)
		{
			move = TRUE;
			first = curr;
		}
		else if (!(SLOT_IS_EMPTY(slots[curr])) && move)
		{
			slots[first].length = slots[curr].length;
			slots[first].offset = slots[curr].offset;
			SLOT_SET_EMPTY(slots[curr]);
			first++;
			while (!(SLOT_IS_EMPTY(slots[first])))
				first++;
		}
		curr++;
	}

	if (move == (Bool)TRUE)
	{
		freeSpace += sizeof(slots)*(numOfSlots - first);
		numOfSlots = first;
	}
}

int HeapPage::GetNumOfRecords()
{
	int i;
	int sum;

	sum = 0;
	for (i = 0; i < numOfSlots; i++)
	{
		if (!(SLOT_IS_EMPTY(slots[i])))
		{
			sum++;
		}
	}
	return sum;
}
