#include "db.h"
#include "frame.h"


Frame::Frame()
{
	pid = INVALID_PAGE;
	data = new Page();
	memset(data, 0, sizeof(Page));
	pinCount = 0;
	dirty = FALSE;
}

Frame::~Frame()
{
	delete data;
}

void Frame::Pin()
{
	pinCount++;
}

void Frame::Unpin()
{
	pinCount--;
}

Bool Frame::NotPinned()
{
	return pinCount == 0;
}

Bool Frame::IsValid()
{
	return pid != INVALID_PAGE;
}

Status Frame::Write()
{
	Status s;

	if (dirty)
	{
		s = MINIBASE_DB->WritePage(pid, data);
		if (s == OK)
			EmptyIt();
		return s;
	}
	else
	{
		return OK;
	}
}

void Frame::DirtyIt()
{
	dirty = TRUE;
}

Bool Frame::IsDirty()
{
	return dirty;
}

void Frame::EmptyIt()
{
	pid = INVALID_PAGE;
	pinCount = 0;
	dirty = FALSE;
}

Status Frame::Read(PageID pageid)
{
	Status s;
	pid = pageid;
	s = MINIBASE_DB->ReadPage(pid, data);
	if (s != OK)
	{
		std::cerr << "Warning : Frame::Read cannot read page " << pid << std::endl;
		return FAIL;
	}
	return OK;
}


void Frame::SetPageID(PageID pageid)
{
	pid = pageid;
}


PageID Frame::GetPageID()
{
	return pid;
}

Status Frame::Free()
{
	Status s;

	if (pinCount > 1)
	{
		std::cerr << "   Free a page that is pinned more than once.\n";
		return FAIL;
	}
	else
	{
		if (pinCount == 1)
			Unpin();
		s = MINIBASE_DB->DeallocatePage(pid);
		if (s == OK)
			EmptyIt();
	}
	return s;
}


Bool Frame::HasPageID( PageID id )
{
	return pid == id;
}


Page *Frame::GetPage()
{
	return data;
}
