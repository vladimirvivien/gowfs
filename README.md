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

#### Create Configuration
Use the `Configuration` struct to specify paramters for the file system.  You can create configuration either using a `Configuration{}` literal or using NewConfiguration() for defaults. 

```
conf := *gowfs.NewConfiguration()
conf.Addr = "localhost:50070"
conf.Username = "hdfs"
conf.ConnectionTime = time.Second * 15
conf.DisableKeepAlives = false 
```

#### Create FileSystem 
Create a new `FileSystem` struct before you can make call to any functions. 
```
fs, err := gowfs.NewFileSystem(conf)
```

#### Create File
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

#### Open File
Use the FileSystem.Open() to open and read a remote file from HDFS.  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Open
```
    data, err := fs.Open(gowfs.Path{Name:"/remote/file"}, 0, 512, 2048)
```

#### Append to File
To append to an existing file on HDFS.  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Append
```
ok, err := fs.Append(
    bytes.NewBufferString("Hello webhdfs users!"),
    gowfs.Path{Name:"/remote/file"}, 4096)
```

#### Rename File
Use FileSystem.Rename() to rename HDFS files. See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Rename
```
ok, err := fs.Rename(gowfs.Path{Name:"/old/name"}, Path{Name:"/new/name"})
```

#### Delete File
To delete an HDFS file use FileSyste.Delete().  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Delete
```
ok, err := fs.Delete(gowfs.Path{Name:"/remote/file/todelete"}, false)
```

#### Set File Permission
Use FileSystem.SetPermission().  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.SetPermission
```
ok, err := fs.SetPermission(gowfs.Path{Name:"/remote/file"}, 0744)
```

### Limitations
1. Only "SIMPLE" security mode supported.
2. No support for kerberos.

### References
1. WebHDFS API - http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
2. FileSystemShell - http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#getmerge