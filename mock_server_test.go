package gowfs

import "fmt"
import "log"
import "net/http"
import "net/http/httptest"

const MockHost   	= "localhost"
const MockPort   	= "70080"
const MockTestUrl   = "http://"+MockHost+":"+MockPort+WebHdfsVer

func makeMockWebServer(handler func (rsp http.ResponseWriter, req * http.Request)) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(handler))
	fmt.Println ("Starting mock web server on", server.URL)
	return server // returns a started server, don't forget to close or else...
}


func getOpenFileServer() *httptest.Server{
  handler := func (rsp http.ResponseWriter, req *http.Request){
      q := req.URL.Query()
      if q.Get("op") != OP_OPEN {
        log.Fatal(`Server Missing expected URL parameter: op=` + OP_OPEN)
      }
      if q.Get("offset") != "0" {
          log.Fatalf("Expected param offset to be 0, but was %v", q.Get("offset"))
      }      
      if q.Get("length") != "512" {
          log.Fatalf("Expected param offset to be 512, but was %v", q.Get("length"))
      }
      if q.Get("buffersize") != "2048" {
          log.Fatalf("Expected param offset to be 2048, but was %v", q.Get("buffersize"))
      }

      fmt.Fprintf (rsp, "Hello, webhdfs user!")
  }
  return makeMockWebServer(handler)
}


func getMkDirsServer() *httptest.Server{
  handler := func (rsp http.ResponseWriter, req *http.Request){
      q := req.URL.Query()
      if q.Get("op") != OP_MKDIRS {
        log.Fatalf("Server Missing expected URL parameter: op= %v", OP_MKDIRS)
      }
      if q.Get("permission") != "744" {
          log.Fatalf("Expected param permission to be 744, but was %v", q.Get("permission"))
      }

      fmt.Fprintf (rsp, `{"Boolean":true}`)
  }
  return makeMockWebServer(handler)
}


func getCreateSymlinkServer() *httptest.Server{
  handler := func (rsp http.ResponseWriter, req *http.Request){
      q := req.URL.Query()
      if q.Get("op") != OP_CREATESYMLINK {
        log.Fatalf("Server Missing expected URL parameter: op= %v", OP_CREATESYMLINK)
      }
      if q.Get("destination") != "/test/orig" {
          log.Fatalf("Expected param destination to be /test/orig, but was %v", q.Get("destination"))
      }
      if q.Get("createParent") != "false" {
          log.Fatalf("Expected param createParent to be false, but was %v", q.Get("createParent"))
      }

      fmt.Fprintf (rsp, "")
  }
  return makeMockWebServer(handler)
}


const listStatusRsp =`
{
  "FileStatuses":
  {
    "FileStatus":
    [
      {
        "accessTime"      : 1320171722771,
        "blockSize"       : 33554432,
        "group"           : "supergroup",
        "length"          : 24930,
        "modificationTime": 1320171722771,
        "owner"           : "webuser",
        "pathSuffix"      : "a.patch",
        "permission"      : "644",
        "replication"     : 1,
        "type"            : "FILE"
      },
      {
        "accessTime"      : 0,
        "blockSize"       : 0,
        "group"           : "supergroup",
        "length"          : 0,
        "modificationTime": 1320895981256,
        "owner"           : "szetszwo",
        "pathSuffix"      : "bar",
        "permission"      : "711",
        "replication"     : 0,
        "type"            : "DIRECTORY"
      }
    ]
  }
}
`
func getListStatusServer() *httptest.Server {
	handler := func (rsp http.ResponseWriter, req *http.Request){
		q := req.URL.Query()
		if q.Get("op") != OP_LISTSTATUS {
			panic (`Server Missing expected URL parameter: op=` + OP_LISTSTATUS)
		}
		fmt.Fprintln (rsp, listStatusRsp)
	}
	return makeMockWebServer(handler)
}

const fileStatusRsp =`
{
  "FileStatus":
  {
    "accessTime"      : 0,
    "blockSize"       : 0,
    "group"           : "supergroup",
    "length"          : 0,
    "modificationTime": 1320173277227,
    "owner"           : "webuser",
    "pathSuffix"      : "",
    "permission"      : "777",
    "replication"     : 0,
    "type"            : "DIRECTORY" 
  }
}
`
func getFileStatusServer() *httptest.Server {
	handler := func (rsp http.ResponseWriter, req *http.Request){
		q := req.URL.Query()
		if q.Get("op") != OP_GETFILESTATUS {
			panic (`Server Missing expected URL parameter: op=` + OP_GETFILESTATUS)
		}
		fmt.Fprintln (rsp, fileStatusRsp)
	}
	return makeMockWebServer(handler)
}

const contentSummaryRsp =`
{
  "ContentSummary":
  {
    "directoryCount": 2,
    "fileCount"     : 1,
    "length"        : 24930,
    "quota"         : -1,
    "spaceConsumed" : 24930,
    "spaceQuota"    : -1
  }
}
`
func getContentSummaryServer() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
    q := req.URL.Query()
    if q.Get("op") != OP_GETCONTENTSUMMARY {
      panic (`Server Missing expected URL parameter: op=` + OP_GETCONTENTSUMMARY)
    }
    fmt.Fprintln (rsp, contentSummaryRsp)
  }
  return makeMockWebServer(handler)
}

const fileChecksumRsp = `
{
  "FileChecksum":
  {
    "algorithm": "MD5-of-1MD5-of-512CRC32",
    "bytes"    : "eadb10de24aa315748930df6e185c0d ...",
    "length"   : 28
  }
}
`
func getFileChecksumServer() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
    q := req.URL.Query()
    if q.Get("op") != OP_GETFILECHECKSUM {
      panic (`Server Missing expected URL parameter: op=` + OP_GETFILECHECKSUM)
    }
    fmt.Fprintln (rsp, fileChecksumRsp)
  }
  return makeMockWebServer(handler) 
}


