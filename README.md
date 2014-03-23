[![Build Status](https://drone.io/github.com/vladimirvivien/gowfs/status.png)](https://drone.io/github.com/vladimirvivien/gowfs/latest)

## gowfs 
gowfs is a Go client API for the Hadoop Web FileSystem (WebHDFS).  It provides typed access to remote HDFS resources via Go's JSON marshaling system.  gowfs follows the WebHDFS JSON protocol outline in  http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html.  It has been tested with Apache Hadoop 2.x.x - series.

### HDFS Setup
* Enable `dfs.webhdfs.enabled` property in your hsdfs-site.xml 
* Ensure `hadoop.http.staticuser.user` property is set in your core-site.xml.

### Usage
```
go get github.com/vladimirvivien/gowfs
```
```go
import github.com/vladimirvivien/gowfs
...
fs, err := gowfs.NewFileSystem(gowfs.Configuration{Addr: "localhost:50070", User: "hdfs"})
if err != nil{
	log.Fatal(err)
}
checksum, err := fs.GetFileChecksum(gowfs.Path{Name: "location/to/file"})
if err != nil {
	log.Fatal(err)
}
fmt.Println (checksum)
```
#### Example Projects
To see how the API is used, check out the gowfs-example repository at https://github.com/vladimirvivien/gowfs-examples.

#### GoDoc Package Documentation
GoDoc documentation - https://godoc.org/github.com/vladimirvivien/gowfs

### FileSystem Examples

#### Configuration{}
Use the `Configuration{}` struct to specify paramters for the file system.  You can create configuration either using a `Configuration{}` literal or using `NewConfiguration()` for defaults. 

```
conf := *gowfs.NewConfiguration()
conf.Addr = "localhost:50070"
conf.Username = "hdfs"
conf.ConnectionTime = time.Second * 15
conf.DisableKeepAlives = false 
```

#### FileSystem{}
Create a new `FileSystem{}` struct before you can make call to any functions. 
```
fs, err := gowfs.NewFileSystem(conf)
```

#### FileSystem.Create()
Create and store a remote file on the HDFS server.
See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Create
```
ok, err := fs.Create(
    bytes.NewBufferString("Hello webhdfs users!"),
	gowfs.Path{Name:"/remote/file"},
	false,
	0,
	0,
	0700,
	0,
)
```

#### FileSystem.Open()
Use the FileSystem.Open() to open and read a remote file from HDFS.  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Open
```
    data, err := fs.Open(gowfs.Path{Name:"/remote/file"}, 0, 512, 2048)
```

#### FileSystem.Append()
To append to an existing file on HDFS.  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Append
```
ok, err := fs.Append(
    bytes.NewBufferString("Hello webhdfs users!"),
    gowfs.Path{Name:"/remote/file"}, 4096)
```

#### FileSystem.Rename()
Use FileSystem.Rename() to rename HDFS files. See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Rename
```
ok, err := fs.Rename(gowfs.Path{Name:"/old/name"}, Path{Name:"/new/name"})
```

#### FileSystem.Delete()
To delete an HDFS file use FileSyste.Delete().  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Delete
```go
ok, err := fs.Delete(gowfs.Path{Name:"/remote/file/todelete"}, false)
```

#### FileSystem.GetFileStatus()
You can get status about an existing HDFS file using FileSystem.GetFileStatus(). See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.GetFileStatus

```go
fileStatus, err := fs.GetFileStatus(gowfs.Path{Name:"/remote/file"})
```
FileStatus is a struct with status info about remote file.
```go
type FileStatus struct {
	AccesTime int64
    BlockSize int64
    Group string
    Length int64
    ModificationTime int64
    Owner string
    PathSuffix string
    Permission string
    Replication int64
    Type string
}
```
You can get a list of file stats using FileSystem.ListStatus()
```go
stats, err := fs.ListStatus(gowfs.Path{Name:"/remote/directory"})
for _, stat := range stats {
    fmt.Println(stat.PathSuffix, stat.Length)
}
```

### FsShell Examples
While the `FileSystem` type has the low level functions, use the `FsShell` to access a higher level of abstraction when working with HDFS.  FsShell functions are integrated with the local file system for easier usage.

#### Create the FsShell
To create an FsShell, you need to have an existing instance of FileSystem.
```go
shell := gowfs.FsShell{FileSystem:fs}
```
#### FsShell.Put()
Use the put to upload a local file to an HDFS file system. See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.PutOne
```go
ok, err := shell.PutOne("local/file/name", "hdfs/file/path", true)
```
#### FsShell.Get()
Use the Get to retrieve remote HDFS file to local file system. See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.Get
```go
ok, err := shell.Get("hdfs/file/path", "local/file/name")
```

#### FsShell.AppendToFile()
Append local files to remote HDFS file or directory. See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.AppendToFile
```go
ok, err := shell.AppendToFile([]string{"local/file/1", "local/file/2"}, "remote/hdfs/path")
```
#### FsShell.Chown()
Change owner for remote file.  See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.Chown.
```go
ok, err := shell.Chown([]string{"/remote/hdfs/file"}, "owner2")
```

#### FsShell.Chgrp()
Change group of remote HDFS files.  See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.Chgrp
```go
ok, err := shell.Chgrp([]string{"/remote/hdfs/file"}, "superduper")
```

#### FsShell.Chmod()
Change file mod of remote HDFS files.  See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.Chmod
```go
ok, err := shell.Chmod([]string{"/remote/hdfs/file/"}, 0744)
```

### Local HDFS Test
You can test the API against your local HDFS installation using https://github.com/vladimirvivien/gowfs/tree/master/test-hdfs.  Follow the instructions there to run the local test.

### Limitations
1. Only "SIMPLE" security mode supported.
2. No support for kerberos.

### References
1. WebHDFS API - http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
2. FileSystemShell - http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#getmerge
