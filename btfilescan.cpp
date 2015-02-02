#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"

//-------------------------------------------------------------------
// BTreeFileScan::~BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Clean Up the B+ tree scan.
//-------------------------------------------------------------------

BTreeFileScan::~BTreeFileScan ()
{
	MINIBASE_BM->UnpinPage(curLeaf->PageNo());
}


//-------------------------------------------------------------------
// BTreeFileScan::GetNext
//
// Input   : None
// Output  : rid  - record id of the scanned record.
//           key  - key of the scanned record
// Purpose : Return the next record from the B+-tree index.
// Return  : OK if successful, DONE if no more records to read.
//-------------------------------------------------------------------

Status 
BTreeFileScan::GetNext (RecordID &rid, int &key)
{

	PageID nextPid;
	Page tpage;

	status = OK;


	if (highKey == NULL)
	{
		if (firstTime)
		{
			curLeaf->GetFirst(curKey, cur_rid, t_rid);

			if (lowKey != NULL)
			{
				while (curKey < *lowKey)
				{
					status = curLeaf->GetNext(curKey, cur_rid, t_rid);
					if (status == DONE)
					{
						nextPid = curLeaf->GetNextPage();
						MINIBASE_BM->UnpinPage(cur_pid, CLEAN);
						if (nextPid == INVALID_PAGE)
							return DONE;

						MINIBASE_BM->PinPage(nextPid, (Page *&)curLeaf);
						cur_pid = nextPid;

						curLeaf->GetFirst(curKey, cur_rid, t_rid);
						status = OK;

					}
				}
			}
			rid = cur_rid;
			key = curKey;
			firstTime = false;

			return status;

		}//end firsttime

		if (DONE == curLeaf->GetNext(curKey, cur_rid, t_rid))
		{

			nextPid = curLeaf->GetNextPage();
			MINIBASE_BM->UnpinPage(cur_pid, CLEAN);
			if (nextPid == INVALID_PAGE)
				return DONE;

			MINIBASE_BM->PinPage(nextPid, (Page *&)curLeaf);
			cur_pid = nextPid;

			curLeaf->GetFirst(curKey, cur_rid, t_rid);

		}



		rid = cur_rid;
		key = curKey;


	}//end high_key is NULL
	else
	{
		if (firstTime)
		{

			curLeaf->GetFirst(curKey, cur_rid, t_rid);

			if (lowKey != NULL)
			{
				while (curKey < *lowKey)
				{
					status = curLeaf->GetNext(curKey, cur_rid, t_rid);

					if (status == DONE)
					{
						nextPid = curLeaf->GetNextPage();
						MINIBASE_BM->UnpinPage(cur_pid, CLEAN);
						if (nextPid == INVALID_PAGE)
							return DONE;

						MINIBASE_BM->PinPage(nextPid, (Page *&)curLeaf);
						cur_pid = nextPid;

						curLeaf->GetFirst(curKey, cur_rid, t_rid);

						if (key > *highKey)
							return DONE;

						status = OK;
					}

				}
			}

			firstTime = false;
			rid = cur_rid;
			key = curKey;

			if (key > *highKey)
				return DONE;
			return status;

		}

		if (DONE == curLeaf->GetNext(curKey, cur_rid, t_rid))
		{

			nextPid = curLeaf->GetNextPage();
			MINIBASE_BM->UnpinPage(cur_pid, CLEAN);
			if (nextPid == INVALID_PAGE)
				return DONE;

			MINIBASE_BM->PinPage(nextPid, (Page *&)curLeaf);
			cur_pid = nextPid;

			curLeaf->GetFirst(curKey, cur_rid, t_rid);

		}


		rid = cur_rid;
		key = curKey;

		if (key > *highKey)
			return DONE;


	}//end high_key is value

	if (cur_rid.pageNo == -1 && t_rid.pageNo == -1)
		return DONE;

	return status;
}

//-------------------------------------------------------------------
// BTreeFileScan::DeleteCurrent
//
// Input   : None
// Output  : None
// Purpose : Delete the entry currently being scanned (i.e. returned
//           by previous call of GetNext())
// Return  : OK if successful, DONE if no more record to read.
//-------------------------------------------------------------------


Status 
BTreeFileScan::DeleteCurrent ()
{  
	return OK;
}


