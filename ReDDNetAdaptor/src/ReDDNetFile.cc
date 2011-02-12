#include "Utilities/ReDDNetAdaptor/interface/ReDDNetFile.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include <cassert>
#include <vector>
#include <unistd.h>
#include <fcntl.h>

ReDDNetFile::ReDDNetFile (void)
  : m_fd (NULL),
    m_close (false)
{}

ReDDNetFile::ReDDNetFile (IOFD fd)
  : m_fd (fd),
    m_close (true)
{}

ReDDNetFile::ReDDNetFile (const char *name,
    	    int flags /* = IOFlags::OpenRead */,
    	    int perms /* = 066 */)
  : m_fd (NULL),
    m_close (false)
{ open (name, flags, perms); }

ReDDNetFile::ReDDNetFile (const std::string &name,
    	    int flags /* = IOFlags::OpenRead */,
    	    int perms /* = 066 */)
  : m_fd (NULL),
    m_close (false)
{ open (name.c_str (), flags, perms); }

ReDDNetFile::~ReDDNetFile (void)
{
  if (m_close)
    edm::LogError("ReDDNetFileError")
      << "Destructor called on ReDDNet file '" << m_name
      << "' but the file is still open";
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
  redd_errno = 0;
  if ((newfd = redd_open (name, openflags, perms)) == -1)
    throw cms::Exception("ReDDNetFile::open()")
      << "redd_open(name='" << name
      << "', flags=0x" << std::hex << openflags
      << ", permissions=0" << std::oct << perms << std::dec
      << ") => error '" << redd_strerror(redd_errno)
      << "' (redd_errno=" << redd_errno << ")";

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

  if (redd_close (m_fd) == -1)
    edm::LogWarning("ReDDNetFileWarning")
      << "redd_close(name='" << m_name
      << "') failed with error '" << redd_strerror (redd_errno())
      << "' (redd_errno=" << redd_errno() << ")";

  m_close = false;
  m_fd = NULL;

  // Caused hang.  Will be added back after problem is fixed.
  // edm::LogInfo("ReDDNetFileInfo") << "Closed " << m_name;
}

void
ReDDNetFile::abort (void)
{
  if (m_fd != NULL)
    redd_close (m_fd);

  m_close = false;
  m_fd = NULL;
}

IOSize
ReDDNetFile::read (void *into, IOSize n)
{
  IOSize done = 0;
  while (done < n)
  {
    ssize_t s = redd_read (m_fd, (char *) into + done, n - done);
    if (s == -1)
      throw cms::Exception("ReDDNetFile::read()")
        << "redd_read(name='" << m_name << "', n=" << (n-done)
        << ") failed with error '" << redd_strerror(redd_errno())
        << "' (redd_errno=" << redd_errno() << ")";
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
    ssize_t s = redd_write (m_fd, (const char *) from + done, n - done);
    if (s == -1)
      throw cms::Exception("ReDDNetFile::write()")
        << "redd_write(name='" << m_name << "', n=" << (n-done)
        << ") failed with error '" << redd_strerror(redd_errno())
        << "' (redd_errno=" << redd_errno() << ")";
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

  redd_errno = 0;
  if ((result = redd_lseek (m_fd, offset, mywhence)) == -1)
    throw cms::Exception("ReDDNetFile::position()")
      << "redd_lseek64(name='" << m_name << "', offset=" << offset
      << ", whence=" << mywhence << ") failed with error '"
      << redd_strerror (redd_errno()) << "' (redd_errno=" << redd_errno() << ")";

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

