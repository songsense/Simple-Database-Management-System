#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;
BufferManager *BufferManager::_bf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

// Check a file's existence
bool PagedFileManager::fileExist(const char* fileName) {
	struct stat fileInfo;
	return stat(fileName, &fileInfo) == 0 ? true : false;
}

// This method creates a paged file called fileName. The file should not already exist.
RC PagedFileManager::createFile(const char *fileName)
{
	if (fileExist(fileName)) {
		// Check if a file has already existed
		PrintError("File exists in PagedFileManager::createFile");
		return FILE_EXIST;
	}
	// create the page file as requested
	FILE * pFile = fopen(fileName, "w");
	if (NULL != pFile) {
		fclose(pFile);
		return SUCC;
	} else {
		PrintFileStreamError("PagedFileManager::createFile: ");
		return FILE_OPEN_FAILURE;
	}

    return UNKNOWN_FAILURE;
}

// This method destroys the paged file whose name is fileName. The file should exist.
RC PagedFileManager::destroyFile(const char *fileName)
{
	if (false == fileExist(fileName)) {
		PrintError("PagedFileManager::destroyFile: File not exist");
		return FILE_NOT_EXIST;
	}
	if (remove(fileName) == 0) {
		return SUCC;
	}
	else {
		PrintFileStreamError("PagedFileManager::destroyFile");
		return FILE_REMOVE_FAILURE;
	}
    return UNKNOWN_FAILURE;
}

// This method opens the page file whose name is fileName. The file must already exist
// and it must have been created using the createFile method.
RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	// Check file's existence
	if (false == fileExist(fileName)) {
		PrintError("PagedFileManager::openFile: File not exist");
		return FILE_NOT_EXIST;
	}

	FILE *fp(NULL);
	// if there are more than one handle than opens the file, use that FILE instead
	if (get_refCounter(fileName, fp) > 0) {
		fileHandle.pFile = fp;
		return SUCC;
	}
	// Open file
	fileHandle.pFile = fopen(fileName, "r+");
	if (NULL == fileHandle.pFile) {
		PrintFileStreamError("PagedFileManager::openFile");
		return FILE_OPEN_FAILURE;
	}
	fileHandle.fileName.assign(fileName);
	
	// Increase refCounter
	inc_refCounter(fileName, fileHandle);
	return SUCC;
}

// This method closes the open file instance referred  by fileHandle.
// The file must have been opened using the openFile method.
// All of the file's pages are flushed to disk when the file is closed.
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	// Check if the file is opened by openFile method
	if (NULL == fileHandle.pFile) {
		PrintFileStreamError("PagedFileManager::closeFile");
		return FILE_NOT_OPEN_BY_HANDLE;
	} else {
		// Check whether if there is any error of the file
		if (ferror (fileHandle.pFile)) {
			PrintFileStreamError("PagedFileManager::closeFile");
			return FILE_STREAM_FAILURE;
		}
		//Check whether if the file was flushed to disk
		if (fflush(fileHandle.pFile) != 0) {
			PrintFileStreamError("PagedFileManager::closeFile");
			return FILE_STREAM_FAILURE;
		}
		FILE *fp(NULL);
		//Check the file's reference
		if (get_refCounter(fileHandle.fileName, fp) > 0) {
			dec_refCounter(fileHandle.fileName);
		} else {
			fclose(fileHandle.pFile);
			close_refCounter(fileHandle.fileName);
		}
		return SUCC;
	}
    return UNKNOWN_FAILURE;
}
// Reference increment
void PagedFileManager::inc_refCounter(const string &fileName, FileHandle &fileHandle) {
	unordered_map<string, FileInfo>::iterator itr = refCounter.find(fileName);
	//Check the current reference, if not exist,  set as 1
	if (itr == refCounter.end()) {
		FileInfo fileInfo(fileHandle.pFile, 1);
		refCounter.insert(pair<string, FileInfo>(fileName, fileInfo));
		FileSpaceManager fsManager(fileHandle);
		filesSpaceManager.insert(pair<string, FileSpaceManager>(fileName, fsManager));
	} else {
		itr->second.cnt += 1;
	}
}
// reference decrement
void PagedFileManager::dec_refCounter(const string &fileName) {
	unordered_map<string, FileInfo>::iterator itr = refCounter.find(fileName);
	//Check the current reference
	if (itr == refCounter.end()) {
		return;
	} else if (itr->second.cnt > 0){
		itr->second.cnt -= 1;
	} else {
		//if the reference is zero, delete the reference counter, close the file
		fclose(itr->second.fp);
		refCounter.erase(itr);
		// delete the file free space manager
		filesSpaceManager.erase(fileName);
	}
}
// Get reference count of a file
int PagedFileManager::get_refCounter(const string &fileName, FILE *&fp) {
	unordered_map<string, FileInfo>::iterator itr = refCounter.find(fileName);
	// If not exist, return 0, else return the reference
	if (itr == refCounter.end()) {
		fp = NULL;
		return 0;
	} else {
		fp = itr->second.fp;
		return itr->second.cnt;
	}
}
// Close the reference counter with given file name
void PagedFileManager::close_refCounter(const string &fileName) {
	refCounter.erase(fileName);
	filesSpaceManager.erase(fileName);
}
// Try to close those files with zero reference
void PagedFileManager::closeFileZeroRef() {
	unordered_map<string, FileInfo>::iterator itr = refCounter.begin();
	while (itr != refCounter.end()) {
		if (itr->second.cnt <= 0) {
			fclose(itr->second.fp);
			itr = refCounter.erase(itr);
			filesSpaceManager.erase(itr->first);
		} else {
			++itr;
		}
	}
}

FileHandle::FileHandle():pFile(NULL),fileName("")
{
}


FileHandle::~FileHandle()
{
	PagedFileManager::instance()->closeFile(*this);
}

// This method reads the page into the memory block pointed by data. The page should exist.
// Note the page number starts from 0.
RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum > getNumberOfPages()) {
    	PagedFileManager::instance()->PrintError("file does not exist in FileHandle::readPage");
    	return PAGE_NOT_EXIST;
    } else {
    	if (ferror(pFile)) {
    		perror("1. FileHandle::readPage");
    		return FILE_STREAM_FAILURE;
    	} else {
    		if (fseek(pFile, pageNum * PAGE_SIZE, SEEK_SET) != 0 ||		// seek file to specific page name
    				fread(data, PAGE_SIZE, 1, pFile) != 1 ){	// read page
    			PagedFileManager::instance()->PrintFileStreamError("2. FileHandle::readPage");
    			return FILE_STREAM_FAILURE;
    		} else {
    			return SUCC;
    		}
    	}
    }
}

// This method writes the data into a page specified by the pageNum.
// The page should exist. Note the page number starts from 0.
RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if (pageNum > getNumberOfPages()) {
		PagedFileManager::instance()->PrintError("file does not exist in FileHandle::writePage");
		return PAGE_NOT_EXIST;
	} else {
		if (ferror(pFile)) {
			PagedFileManager::instance()->PrintFileStreamError("1. FileHandle::writePage");
			return FILE_STREAM_FAILURE;
		} else {
			if (fseek(pFile, pageNum * PAGE_SIZE, SEEK_SET) != 0 ||		// seek to page num
					fwrite(data, PAGE_SIZE, 1, pFile) != 1 ){	// write page
				PagedFileManager::instance()->PrintFileStreamError("2. FileHandle::writePage");
    			return FILE_STREAM_FAILURE;
			} else {
				fflush(pFile);
				return SUCC;
			}
		}
	}
}

// This method appends a new page to the file, and writes the data into the new allocated page.
RC FileHandle::appendPage(const void *data)
{
    if (ferror(pFile)) {
    	PagedFileManager::instance()->PrintFileStreamError("FileHandle::appendPage");
    	return FILE_STREAM_FAILURE;
    } else {
    	if (fseek(pFile, PAGE_SIZE * getNumberOfPages(), SEEK_SET) != 0 ||	// seek to the end of the last page
    			fwrite(data, PAGE_SIZE, 1, pFile) != 1 ||			// write the page to the end
    			fflush(pFile) != 0){										// flush the file
    		PagedFileManager::instance()->PrintFileStreamError("FileHandle::appendPage");
    		return FILE_STREAM_FAILURE;
    	} else {
    		return SUCC;
    	}
    }
}

// This method returns the total number of pages in the file.
unsigned FileHandle::getNumberOfPages()
{
	if (ferror(pFile)) {
		PagedFileManager::instance()->PrintFileStreamError("FileHandle::getNumberOfPages");
		return 0;
	}
	if (fseek (pFile , 0 , SEEK_END) != 0) {
		PagedFileManager::instance()->PrintFileStreamError("FileHandle::getNumberOfPages");
		return 0;
	} else {
		return ftell(pFile) / PAGE_SIZE;
	}
}
// Get the free space associated with page num
unsigned FileHandle::getSpaceOfPage(PageNum pageNum, void *page) {
	RC rc = readPage(pageNum, page);
	if (rc != SUCC) {
		cerr << "Error to compute the size of page" << endl;
		return 0;
	} else {
		unsigned long sp = (unsigned long)(*((unsigned long*)(page) + PAGE_SIZE/sizeof(unsigned long) - 1ul));
		return PAGE_SIZE - sp;
		return sp - (unsigned long)(page);
	}
}

// File Space manager
FileSpaceManager::FileSpaceManager(FileHandle &fileHandle) {
	unsigned totalNumPage = fileHandle.getNumberOfPages();

	for (unsigned i = 0; i < totalNumPage; ++i) {
		char page[PAGE_SIZE];
		fileHandle.readPage(i, page);
		unsigned space = fileHandle.getSpaceOfPage(i, page);
		PageSpaceInfo psInfo(space, i);
		pageQueue.push(psInfo);
	}
}
FileSpaceManager::FileSpaceManager() {
}
// Get the page with the greatest free space size
RC FileSpaceManager::getPageSpaceInfo(const unsigned &recordSize, PageNum &pageNum) {
	if (pageQueue.empty())
		return FILE_SPACE_EMPTY;
	PageSpaceInfo pageSpaceInfo = pageQueue.top();
	if (pageSpaceInfo.freeSpaceSize < recordSize) {
		// the free space of the page is less than the record to be inserted
		return FILE_SPACE_NO_SPACE;
	}
	pageNum = pageSpaceInfo.pageNum;
	return SUCC;
}
// Pop the page space info
RC FileSpaceManager::popPageSpaceInfo() {
	if (pageQueue.empty())
		return FILE_SPACE_EMPTY;
	pageQueue.pop();
	return SUCC;
}
// Push the page space info
RC FileSpaceManager::pushPageSpaceInfo(const unsigned &freeSpaceSize, const unsigned &pageNum) {
	PageSpaceInfo sp(freeSpaceSize, pageNum);
	pageQueue.push(sp);
	return SUCC;
}






BufferManager::BufferManager() :
		buffer((char *)malloc(MAX_PAGE_IN_MEM * PAGE_SIZE)) {
	memset(buffer, 0, MAX_PAGE_IN_MEM * PAGE_SIZE);
	for (int i = 0; i < MAX_PAGE_IN_MEM; ++i) {
		availableFrameOffset.push_back(i * PAGE_SIZE);
	}
}
BufferManager::~BufferManager() {
	free(buffer);
}
BufferManager* BufferManager::instance()
{
    if(!_bf_manager) {
    	_bf_manager = new BufferManager();
    }

    return _bf_manager;
}
// determine if a page is stored in buffer manager
bool BufferManager::existPage(const string &fileName, PageNum pageNum) {
	FrameTable::iterator itrFrame = frameTable.find(fileName);
	if (itrFrame == frameTable.end()) {
		return false;
	} else {
		FrameInfo2ndLevel::iterator itrFrame2 = itrFrame->second.find(pageNum);
		if (itrFrame2 == itrFrame->second.end())
			return false;
		else
			return true;
	}
}
// Get a specific page
RC BufferManager::readPage(const string &fileName, PageNum pageNum, void *&data)  {
	FrameTable::iterator itrFrame = frameTable.find(fileName);
	if (itrFrame == frameTable.end()) {
		return BUFFER_FILE_NOT_HIT;
	} else {
		FrameInfo2ndLevel::iterator itrFrame2 = itrFrame->second.find(pageNum);
		if (itrFrame2 == itrFrame->second.end()) {
			return BUFFER_PAGENUM_NOT_HIT;
		}
		else {
			FrameOffset offset = itrFrame2->second;
			data = (void *)(buffer + offset);
			updateMRU();
			return SUCC;
		}
	}
}
// Write a specific page
// Note the page is dirty, must be saved to disk outside
RC BufferManager::writePage(const string &fileName, PageNum pageNum, void *data) {
	FrameTable::iterator itrFrame = frameTable.find(fileName);
	if (itrFrame == frameTable.end()) {
		return BUFFER_FILE_NOT_HIT;
	} else {
		FrameInfo2ndLevel::iterator itrFrame2 = itrFrame->second.find(pageNum);
		if (itrFrame2 == itrFrame->second.end()) {
			return BUFFER_PAGENUM_NOT_HIT;
		}
		else {
			FrameOffset offset = itrFrame2->second;
			memcpy(buffer + offset, data, PAGE_SIZE * sizeof(char));
			updateMRU();
			return SUCC;
		}
	}
	return SUCC;
}

// Load pages to the memory
RC BufferManager::loadPages(FileHandle *fileHandle, PageNum startPageNum, unsigned numPages2Load) {
	unsigned numPages = fileHandle->getNumberOfPages();
	numPages = startPageNum < numPages ? numPages-startPageNum : 0;
	numPages2Load = numPages2Load > numPages ? numPages: numPages2Load;
	numPages2Load = numPages2Load > MAX_PAGE_IN_MEM ? MAX_PAGE_IN_MEM : numPages2Load;
	if (numPages2Load + getNumTotalPages() > MAX_PAGE_IN_MEM) {
		unsigned numPagesReplaced = numPages2Load + getNumTotalPages() - MAX_PAGE_IN_MEM;
		for (int i = 0; i < numPagesReplaced; ++i) {
			FrameOffset offset = queueMRU.front();
			queueMRU.pop_back();
			availableFrameOffset.push_front(offset);
			// delete entry in tables
			InverseFrameTable::iterator itr = inverseFrameTable.find(offset);
			frameTable[itr->second.fileName].erase(itr->second.pageNum);
			if (frameTable[itr->second.fileName].empty())
				frameTable.erase(itr->second.fileName);
			inverseFrameTable.erase(itr);
		}
	}

	for (int i = startPageNum; i < startPageNum + numPages2Load; ++i) {
		if (i >= numPages)
			break;
		if (existPage(fileHandle->fileName, i))
			continue;
		FrameOffset offset = availableFrameOffset.front();
		availableFrameOffset.pop_front();
		FrameInfo frameInfo(fileHandle->fileName, i);
		inverseFrameTable.insert(pair<FrameOffset, FrameInfo>(offset, frameInfo));
		frameTable[fileHandle->fileName][i] = offset;
		fileHandle->readPage(i, buffer + offset);
	}
	return SUCC;
}
// update most recently used page
void BufferManager::updateMRU() {
	FrameOffset offset = queueMRU.front();
	queueMRU.pop_front();
	queueMRU.push_back(offset);
}
// number of used pages
unsigned BufferManager::getNumTotalPages() {
	return queueMRU.size();
}
