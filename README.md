## SFTP folder sync script

poller is a script to sync contents of a specific SFTP folder to a given location at specified intervals.
It is relatively simple and features the following:

 - [X] files are pulled in alphabetical order
 - [X] light resumes (files can only be resumed while running)
 - [X] persistent sync state - only new files are downloaded
 - [ ] old files are removed
 - [ ] polls contents of the remote server at specified interval (5m, to be configurable)

Installation:

```bash
go get github.com/a-palchikov/poller
```

### Example:

```bash
./poller -dir=/home/user/data -client="SSH-2.0-OpenSSH_6.6.1" \
    -usr=USERNAME -pwd=PASSWORD -addr="ftp.myserver.com:22" -remote-dir="remote/path/to/sync"
```

