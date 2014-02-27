[![Build Status](https://drone.io/github.com/vladimirvivien/gowfs/status.png)](https://drone.io/github.com/vladimirvivien/gowfs/latest)

## gowfs 
gowfs is a Go client API for the Hadoop Web FileSystem (WebHDFS).  It provides typed access to remote HDFS resources via Go's JSON marshaling system.  gowfs follows the WebHDFS JSON protocol outline in  http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html.  It has been tested with Apache Hadoop 2.x.x - series.

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
checksum, err := fs.GetFileChecksum(gowfs.Path{Path: "location/to/file"})
if err != nil {
	log.Fatal(err)
}
fmt.Println (checksum)
```
#### Example Projects
To see how the API is used, check out the gowfs-example repository at https://github.com/vladimirvivien/gowfs-examples.

#### GoDoc Package Documentation
GoDoc documentation - https://godoc.org/github.com/vladimirvivien/gowfs

### HDFS Setup
* Enable `dfs.webhdfs.enabled` property in your hsdfs-site.xml 
* Ensure `hadoop.http.staticuser.user` property is set in your core-site.xml.
* No support for kerberos right now.


### Limitations
1. Very early implementation.
2. Only "Simple" security mode supported.
3. Not all methods are implemeted at this time.
   Unimplemented methods will return an error.

##### Implemented
```
Open()
Rename()
Delete()
Create()
Append()
Concat()
SetOwner()
SetPermission()
SetReplication()
SetTimes()
MkDirs()
CreateSymlink()
GetFileStatus()
ListStatus()
GetContentSummary()
GetHomeDirectory()
GetFileChecksum()
```

##### Unimplemented
```
GetDelegationToken()
GetDelegationTokens()
RenewToken()
CancelDelegationToken()
```