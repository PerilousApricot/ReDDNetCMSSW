#ifndef REDDNET_ADAPTOR_REDDNET_FILE_H
# define REDDNET_ADAPTOR_REDDNET_FILE_H

# include "Utilities/StorageFactory/interface/Storage.h"
# include "Utilities/StorageFactory/interface/IOFlags.h"
# include <string>
#include <pthread.h>
class ReDDNetFile : public Storage
{
public:
  ReDDNetFile (void);
  ReDDNetFile (void * fd);
  ReDDNetFile (const char *name, int flags = IOFlags::OpenRead, int perms = 0666);
  ReDDNetFile (const std::string &name, int flags = IOFlags::OpenRead, int perms = 0666);
  ~ReDDNetFile (void);

  virtual void	create (const char *name,
    			bool exclusive = false,
    			int perms = 0666);
  virtual void	create (const std::string &name,
    			bool exclusive = false,
    			int perms = 0666);
  virtual void	open (const char *name,
    		      int flags = IOFlags::OpenRead,
    		      int perms = 0666);
  virtual void	open (const std::string &name,
    		      int flags = IOFlags::OpenRead,
    		      int perms = 0666);

  using Storage::read;
  using Storage::write;
  using Storage::position;

  virtual IOSize	read (void *into, IOSize n);
  virtual IOSize	write (const void *from, IOSize n);

  virtual IOOffset	position (IOOffset offset, Relative whence = SET);
  virtual void		resize (IOOffset size);

  virtual void		close (void);
  virtual void		abort (void);
  
  class MutexWrapper {
	public:
	  MutexWrapper( pthread_mutex_t * lock );
	  ~MutexWrapper();
	  pthread_mutex_t * m_lock;
  };

  static pthread_mutex_t m_dlopen_lock;

private:
  void *		m_fd;
  bool			m_close;
  std::string		m_name;
  void loadLibrary();
  void closeLibrary();
  void * m_library_handle;
  bool m_is_loaded;
  int (*redd_init)();
  ssize_t (*redd_read)(void *, char*, ssize_t); 
  int (*redd_close)(void *);
  off_t (*redd_lseek64)(void *, off_t, int);
  void * (*redd_open)(const char *, int, int );
  ssize_t (*redd_write)(void *, const char *, ssize_t);
  int (*redd_term)();
  long (*redd_errno)();
  const std::string & (*redd_strerror)(long);
};

#endif // REDDNET_ADAPTOR_REDDNET_FILE_H

