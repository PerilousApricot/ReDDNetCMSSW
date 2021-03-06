#include "Utilities/ReDDNetAdaptor/interface/ReDDNetFile.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include <cassert>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <dlfcn.h>

pthread_mutex_t ReDDNetFile::m_dlopen_lock = PTHREAD_MUTEX_INITIALIZER;


ReDDNetFile::ReDDNetFile (void)
  : m_fd (NULL),
    m_close (false),
	m_is_loaded(false)
{
	loadLibrary();	
}

ReDDNetFile::ReDDNetFile (void * fd)
  : m_fd (fd),
    m_close (true),
	m_is_loaded(false)
{
	loadLibrary();	
}

ReDDNetFile::ReDDNetFile (const char *name,
    	    int flags /* = IOFlags::OpenRead */,
    	    int perms /* = 066 */)
  : m_fd (NULL),
    m_close (false),
	m_is_loaded(false)
{   loadLibrary();
	open (name, flags, perms); }

ReDDNetFile::ReDDNetFile (const std::string &name,
    	    int flags /* = IOFlags::OpenRead */,
    	    int perms /* = 066 */)
  : m_fd (NULL),
    m_close (false),
	m_is_loaded(false)
{   loadLibrary();
	open (name.c_str (), flags, perms); }

ReDDNetFile::~ReDDNetFile (void)
{
  if (m_close)
    edm::LogError("ReDDNetFileError")
      << "Destructor called on ReDDNet file '" << m_name
      << "' but the file is still open";
  closeLibrary();
}

//	NAME = static_cast<TYPE>(dlsym(m_library_handle, #NAME));

#define REDD_LOAD_SYMBOL( NAME, TYPE ) 	dlerror();\
	NAME = reinterpret_cast<TYPE>(dlsym(m_library_handle, #NAME));\
	if ( (retval = dlerror()) ) {\
		throw cms::Exception("ReDDNetFile::loadLibrary()") <<\
			"Failed to load dlsym ReDDNet library: " << retval;\
	}\
	if ( NAME == NULL) {\
		throw cms::Exception("ReDDNetFile::loadLibrary()") <<\
			"Got a null pointer back from dlsym()\n";\
	}

void ReDDNetFile::loadLibrary() {
	
	//pthread_mutex_t ReDDNetFile::m_dlopen_lock = PTHREAD_MUTEX_INITIALIZER;

	ReDDNetFile::MutexWrapper lockObj( & this->m_dlopen_lock );
	// until ACCRE removes the java dependency from their client libs,
	// we'll dlopen() them so they don't need to be brought along with cmssw
	// if you're running ReDDNet at your site, you will have the libs anyway
	// TODO add wrappers to make this work in OSX as well (CMSSW's getting ported?)
	m_library_handle =
	     dlopen("libreddnet.so", RTLD_LAZY);
	if (m_library_handle == NULL) {
		throw cms::Exception("ReDDNetFile::loadLibrary()")
			<< "Can't dlopen() ReDDNet libraries: " << dlerror();
	}
	char * retval;
	REDD_LOAD_SYMBOL( redd_init, int(*)()); 
	REDD_LOAD_SYMBOL( redd_read, ssize_t(*)(void *, char*, ssize_t));
	REDD_LOAD_SYMBOL( redd_lseek64, off_t(*)(void*, off_t, int));
	REDD_LOAD_SYMBOL( redd_open, void*(*)(const char*,int,int));
	REDD_LOAD_SYMBOL( redd_write, ssize_t(*)(void *, const char *, ssize_t));
	REDD_LOAD_SYMBOL( redd_term, int(*)());
	REDD_LOAD_SYMBOL( redd_errno, long(*)());
	REDD_LOAD_SYMBOL( redd_strerror, const std::string & (*)(long));

	if ( (*redd_init)() ) {
		throw cms::Exception("ReDDNetFile::loadLibrary()")
			<< "Error in redd_init: " << (*redd_strerror)(0);
	}
	m_is_loaded = true;

}

void ReDDNetFile::closeLibrary() {
	// this is dumb syntax for a static member variable, but 
	// gcc was being even dumber, so I went this way
	try {
		ReDDNetFile::MutexWrapper lockObj( & this->m_dlopen_lock );
			// congrats. pthread_mutex_lock failed and we're in a destructor.
			// for as much as I like RAII, I don't know what to do here
			// Then again, if the mutex is jammed, things are already boned
			// Cry for a second, then continue with life, I guess
			// melo
			
		if ( m_is_loaded ) {
			if ( (*redd_term)() ) {
				throw cms::Exception("ReDDNetFile::closeLibrary()")
					<< "Error in redd_term: " << (*redd_strerror)(0);
			}
		}
		if ( m_library_handle != NULL ) {
			if ( dlclose( m_library_handle ) ) {
				throw cms::Exception("ReDDNetFile::closeLibrary()")
					<< "Error on dlclose(): " << dlerror();
			}
		}
	} catch (cms::Exception & e) {
		edm::LogError("ReDDNetFileError")
	      << "ReDDNetFile had an error in its destructor: " << e;
	}
	m_is_loaded = false;
}


//////////////////////////////////////////////////////////////////////
void
ReDDNetFile::create (const char *name,
		    bool exclusive /* = false */,
		    int perms /* = 066 */)
{
  open (name,
        (IOFlags::OpenCreate | IOFlags::OpenWrite | IOFlags::OpenTruncate
         | (exclusive ? IOFlags::OpenExclusive : 0)),
        perms);
}

void
ReDDNetFile::create (const std::string &name,
                    bool exclusive /* = false */,
                    int perms /* = 066 */)
{
  open (name.c_str (),
        (IOFlags::OpenCreate | IOFlags::OpenWrite | IOFlags::OpenTruncate
         | (exclusive ? IOFlags::OpenExclusive : 0)),
        perms);
}

void
ReDDNetFile::open (const std::string &name,
                  int flags /* = IOFlags::OpenRead */,
                  int perms /* = 066 */)
{ open (name.c_str (), flags, perms); }

void
ReDDNetFile::open (const char *name,
                  int flags /* = IOFlags::OpenRead */,
                  int perms /* = 066 */)
{
  m_name = name;

  // Actual open
  if ((name == 0) || (*name == 0))
    throw cms::Exception("ReDDNetFile::open()")
      << "Cannot open a file without a name";

  if ((flags & (IOFlags::OpenRead | IOFlags::OpenWrite)) == 0)
    throw cms::Exception("ReDDNetFile::open()")
      << "Must open file '" << name << "' at least for read or write";

  // If I am already open, close old file first
  if (m_fd != NULL && m_close)
    close ();

  // Translate our flags to system flags
  int openflags = 0;

  if ((flags & IOFlags::OpenRead) && (flags & IOFlags::OpenWrite))
    openflags |= O_RDWR;
  else if (flags & IOFlags::OpenRead)
    openflags |= O_RDONLY;
  else if (flags & IOFlags::OpenWrite)
    openflags |= O_WRONLY;

  if (flags & IOFlags::OpenNonBlock)
    openflags |= O_NONBLOCK;

  if (flags & IOFlags::OpenAppend)
    openflags |= O_APPEND;

  if (flags & IOFlags::OpenCreate)
    openflags |= O_CREAT;

  if (flags & IOFlags::OpenExclusive)
    openflags |= O_EXCL;

  if (flags & IOFlags::OpenTruncate)
    openflags |= O_TRUNC;

  void * newfd = NULL;
  if ((newfd = (*redd_open) (name, openflags, perms)) == NULL)
    throw cms::Exception("ReDDNetFile::open()")
      << "redd_open(name='" << name
      << "', flags=0x" << std::hex << openflags
      << ", permissions=0" << std::oct << perms << std::dec
      << ") => error '" << (*redd_strerror)((*redd_errno)())
      << "' (redd_errno=" << (*redd_errno)() << ")";

  m_fd = newfd;

  m_close = true;

  edm::LogInfo("ReDDNetFileInfo") << "Opened " << m_name;
}

void
ReDDNetFile::close (void)
{
  if (m_fd == NULL)
  {
    edm::LogError("ReDDNetFileError")
      << "ReDDNetFile::close(name='" << m_name
      << "') called but the file is not open";
    m_close = false;
    return;
  }

  if ((*redd_close) (m_fd) == -1)
    edm::LogWarning("ReDDNetFileWarning")
      << "redd_close(name='" << m_name
      << "') failed with error '" << (*redd_strerror) ((*redd_errno)())
      << "' (redd_errno=" << (*redd_errno)() << ")";

  m_close = false;
  m_fd = NULL;

  // Caused hang.  Will be added back after problem is fixed.
  // edm::LogInfo("ReDDNetFileInfo") << "Closed " << m_name;
}

void
ReDDNetFile::abort (void)
{
  if (m_fd != NULL)
    (*redd_close) (m_fd);

  m_close = false;
  m_fd = NULL;
}

IOSize
ReDDNetFile::read (void *into, IOSize n)
{
  IOSize done = 0;
  while (done < n)
  {
    ssize_t s = (*redd_read) (m_fd, (char *) into + done, n - done);
    if (s == -1)
      throw cms::Exception("ReDDNetFile::read()")
        << "redd_read(name='" << m_name << "', n=" << (n-done)
        << ") failed with error '" << (*redd_strerror)((*redd_errno)())
        << "' (redd_errno=" << (*redd_errno)() << ")";
    else if (s == 0)
      // end of file
      break;
   done += s;
  }

  return done;
}

IOSize
ReDDNetFile::write (const void *from, IOSize n)
{
  IOSize done = 0;
  while (done < n)
  {
    ssize_t s = (*redd_write) (m_fd, (const char *) from + done, n - done);
    if (s == -1)
      throw cms::Exception("ReDDNetFile::write()")
        << "redd_write(name='" << m_name << "', n=" << (n-done)
        << ") failed with error '" << (*redd_strerror)((*redd_errno)())
        << "' (redd_errno=" << (*redd_errno)() << ")";
    done += s;
  }

  return done;
}
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////
IOOffset
ReDDNetFile::position (IOOffset offset, Relative whence /* = SET */)
{
  if (m_fd == NULL)
    throw cms::Exception("ReDDNetFile::position()")
      << "ReDDNetFile::position() called on a closed file";
  if (whence != CURRENT && whence != SET && whence != END)
    throw cms::Exception("ReDDNetFile::position()")
      << "ReDDNetFile::position() called with incorrect 'whence' parameter";

  IOOffset	result;
  int		mywhence = (whence == SET ? SEEK_SET
		    	    : whence == CURRENT ? SEEK_CUR
			    : SEEK_END);

  if ((result = (*redd_lseek64) (m_fd, offset, mywhence)) == -1)
    throw cms::Exception("ReDDNetFile::position()")
      << "redd_lseek64(name='" << m_name << "', offset=" << offset
      << ", whence=" << mywhence << ") failed with error '"
      << (*redd_strerror) ((*redd_errno)()) << "' (redd_errno=" << (*redd_errno)() << ")";

  // FIXME: dCache returns incorrect value on SEEK_END.
  // Remove this hack when dcap has been fixed.
  if (whence == SEEK_END && (result = redd_lseek64 (m_fd, result, SEEK_SET)) == -1)
    throw cms::Exception("ReDDNetFile::position()")
      << "redd_lseek64(name='" << m_name << "', offset=" << offset
      << ", whence=" << SEEK_SET << ") failed with error '"
      << redd_strerror (redd_errno()) << "' (redd_errno=" << redd_errno() << ")";
  
  return result;
}

void
ReDDNetFile::resize (IOOffset /* size */)
{
  throw cms::Exception("ReDDNetFile::resize()")
    << "ReDDNetFile::resize(name='" << m_name << "') not implemented";
}

ReDDNetFile::MutexWrapper::MutexWrapper( pthread_mutex_t * target ) 
{
	m_lock = target;
	pthread_mutex_lock( m_lock ); // never fails
}

ReDDNetFile::MutexWrapper::~MutexWrapper()
{
	if (pthread_mutex_unlock( m_lock )) {
		// congrats. pthread_mutex_lock failed and we're in a constructor
		// for as much as I like RAII, I don't know what to do here
		// Then again, if the mutex is jammed, things are already boned
		// Cry for a second, then continue with life, I guess
    	// melo
		edm::LogError("ReDDNetFileError")
	      << "ReDDNetFile couldn't unlock a mutex";
	}
}
	
