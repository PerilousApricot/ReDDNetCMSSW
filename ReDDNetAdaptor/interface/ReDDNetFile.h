#ifndef REDDNET_ADAPTOR_REDDNET_FILE_H
# define REDDNET_ADAPTOR_REDDNET_FILE_H

# include "Utilities/StorageFactory/interface/Storage.h"
# include "Utilities/StorageFactory/interface/IOFlags.h"
# include <string>

class ReDDNetFile : public Storage
{
public:
  ReDDNetFile (void);
  ReDDNetFile (IOFD fd);
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

private:
  void *		m_fd;
  bool			m_close;
  std::string		m_name;
};

#endif // REDDNET_ADAPTOR_REDDNET_FILE_H

