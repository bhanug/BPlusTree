
#include "hash.h"



//--------------------------------------------
//
// CLASS Map
//
//--------------------------------------------


Map::Map(PageID p, int f)
{
	pid = p;
	frameNo = f;
	next = NULL;
	prev = NULL;
}

Map::~Map()
{
	
}

void Map::AddBehind(Map *m)
{
	m->next = next;
	m->prev = this;
	if (next)
	{
		next->prev = m;
	}
	next = m;
}


void Map::DeleteMe()
{
	if (next)
		next->prev = prev;
	if (prev)
		prev->next = next;
}


Bool Map::HasPageID(PageID p)
{
	return pid == p;
}


int Map::FrameNo()
{
	return frameNo;
}


//--------------------------------------------
//
// CLASS MapIterator
//
//--------------------------------------------


MapIterator::MapIterator(Map *maps)
{
	current = maps->next; // skip the dummy
}

Map* MapIterator::operator() ()
{
	if (!current)
	{
		return NULL;
	}
	else
	{
		Map *tmp;
		tmp = current;
		current = current->next;
		return tmp;
	}
}


//--------------------------------------------
//
// CLASS Bucket
//
//--------------------------------------------

Bucket::Bucket()
{
	maps = new Map(-1, -1);
}


Bucket::~Bucket()
{
	EmptyIt();
}


void Bucket::Insert(PageID pid, int frameNo)
{
	Map *m = new Map(pid, frameNo);
	if (maps)
	{
		maps->AddBehind(m);
	}
	else
	{
		maps = m;
	}
}

Status Bucket::Delete(PageID pid)
{
	MapIterator next(maps);
	Map *curr;
	
	while (curr = next())
	{
		if (curr->HasPageID(pid))
		{
			curr->DeleteMe();
			delete curr;
			return OK;
		}
	}
	return FAIL;		
}

int Bucket::Find(PageID pid)
{
	MapIterator next(maps);
	Map *curr;
	
	while (curr = next())
	{
		if (curr->HasPageID(pid))
		{
			return curr->FrameNo();
		}
	}
	return INVALID_FRAME;
}


void Bucket::EmptyIt()
{
	MapIterator next(maps);
	Map *curr;
	
	while (curr = next())
	{
		curr->DeleteMe();
		delete curr;
	}
}


//--------------------------------------------
//
// CLASS HashTable
//
//--------------------------------------------


void HashTable::Insert(PageID pid, int frameNo)
{
	buckets[HASH(pid)].Insert(pid, frameNo);
}


Status HashTable::Delete(PageID pid)
{
	return buckets[HASH(pid)].Delete(pid);
}


int HashTable::LookUp(PageID pid)
{
	return buckets[HASH(pid)].Find(pid);
}

void HashTable::EmptyIt()
{
	int i;

	for (i = 0; i < NUM_OF_BUCKETS; i++)
	{
		buckets[i].EmptyIt();
	}
}