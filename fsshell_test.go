package gowfs

import "bytes"
import "os"
import "testing"
import "net/url"
import "net/http"
import "net/http/httptest"
import "fmt"
import "log"

import "strings"

func Test_AppendToFile(t *testing.T) {
	// setup test file
	f1, err := createTestFile("test-file.txt")
	if err != nil {
		panic(err)
	}
	defer os.Remove(f1.Name())

	f2, err := createTestFile("test-file2.txt")
	if err != nil {
		panic(err)
	}
	defer os.Remove(f2.Name())

	//start servers
	// two stages 1) server2 is invoked, then redirect to server1.URL
  	server1 := mockServerFor_Append()
  	servUrl, _ := url.Parse(server1.URL)
  	server2 := mockServerFor_OpenForAppend(servUrl)
  	defer server2.Close()
  	defer server1.Close()

  	url, _ := url.Parse(server2.URL)
  	fs,  _ := NewFileSystem(Configuration{Addr: url.Host })
	shell := FsShell{FileSystem: fs}

	shell.AppendToFile([]string{f1.Name(), f2.Name()}, "/testing/location")

}

func Test_Cat(t *testing.T) {
  	server1 := mockServerFor_FsShellOpen()
  	defer server1.Close()
  	url, _  := url.Parse(server1.URL)
  	fs,  _  := NewFileSystem(Configuration{Addr: url.Host })
	shell   := FsShell{FileSystem: fs}

	var output bytes.Buffer
	shell.Cat([]string{"/remote/file1", "/remote/file2"}, &output)
	msgCat := strings.TrimSpace(string(output.Bytes()))
	if msgCat != (fsShellOpenRsp + "\n" + fsShellOpenRsp) {
		t.Fatal("FsShell.Cat() not getting content in writer.")
	}

}

func Test_Chgrp(t *testing.T){
  	server1 := mockServerFor_FsShellChgrp()
	defer server1.Close()
  	url, _  := url.Parse(server1.URL)
  	fs,  _  := NewFileSystem(Configuration{Addr: url.Host })
	shell   := FsShell{FileSystem: fs}
	_, err := shell.Chgrp([]string{"/remote/file"}, "supergrp")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Chown(t *testing.T){
  	server1 := mockServerFor_FsShellChown()
	defer server1.Close()
  	url, _  := url.Parse(server1.URL)
  	fs,  _  := NewFileSystem(Configuration{Addr: url.Host })
	shell   := FsShell{FileSystem: fs}
	_, err := shell.Chown([]string{"/remote/file"}, "newowner")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Chmod(t *testing.T){
  	server1 := mockServerFor_FsShellChmod()
  	defer server1.Close()
  	url, _  := url.Parse(server1.URL)
  	fs,  _  := NewFileSystem(Configuration{Addr: url.Host })
	shell   := FsShell{FileSystem: fs}
	_, err := shell.Chmod([]string{"/remote/file"}, 0744)
	if err != nil {
		t.Fatal(err)
	}
}

// func Test_Exists(t *testing.T){
//   	server1 := mockServerFor_FileStatus()
//   	defer server1.Close()
//   	url, _  := url.Parse(server1.URL)
//   	fs,  _  := NewFileSystem(Configuration{Addr: url.Host })
// 	shell   := FsShell{FileSystem: fs}
// 	ok, err := shell.Exists("/remote/resource")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if !ok {
// 		t.Fatal("FsShell.Exists() - returns unexpected FileStatus.")
// 	}
// }

func Test_PutOne(t *testing.T) {
	f1, err := createTestFile("test-file.txt")
	if err != nil {
		panic(err)
	}
	defer os.Remove(f1.Name())

	//start servers
	//two stages 1) server2 called first, then redirected to server1.URL
  	server1 := mockServerFor_WriteFile()
  	servUrl, _ := url.Parse(server1.URL)
  	server2 := mockServerFor_PutOne(servUrl)
  	defer server2.Close()
  	defer server1.Close()

  	// call FsShell.PutOne(), which will redirect to server1.
	url, _ := url.Parse(server2.URL)
  	fs,  _ := NewFileSystem(Configuration{Addr: url.Host })
	shell := FsShell{FileSystem: fs}

	_, err = shell.Put(f1.Name(), "/test/remote/file", false)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_PutMany(t *testing.T) {
	f1, err := createTestFile("test-file.txt")
	if err != nil {
		panic(err)
	}
	defer os.Remove(f1.Name())
	
	// f2, err := createTestFile("test-file2.txt")
	// if err != nil {
	// 	panic(err)
	// }
	// defer os.Remove(f2.Name())

	//start servers
	//two stages 1) server2 called first, then redirected to server1.URL
  	server1 := mockServerFor_WriteFile()
  	servUrl, _ := url.Parse(server1.URL)
  	server2 := mockServerFor_PutOne(servUrl)
  	defer server2.Close()
  	defer server1.Close()

  	// call FsShell.PutOne(), which will redirect to server1.
	url, _ := url.Parse(server2.URL)
  	fs,  _ := NewFileSystem(Configuration{Addr: url.Host })
	shell := FsShell{FileSystem: fs}

	_, err = shell.PutMany ([]string{f1.Name()}, "/test/remote/dir", false)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Get(t *testing.T) {
  	server1 := mockServerFor_FsShellOpen()
  	defer server1.Close()
  	url, _  := url.Parse(server1.URL)
  	fs,  _  := NewFileSystem(Configuration{Addr: url.Host })
	shell   := FsShell{FileSystem: fs}

	shell.Get("/remote/file", "test-file.txt")
	file, err := os.Open("test-file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
  defer os.Remove(file.Name())

	data := make([]byte,len(fsShellOpenRsp))
	c, err := file.Read(data)
	if err != nil {
		t.Fatal(err)
	}
	if c != len(fsShellOpenRsp) {
		t.Fatal("Expeting ", len(fsShellOpenRsp), " bytes, but got ", c)
	}
}

func createTestFile(fileName string) (*os.File, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	file.WriteString("Hello webhdfs users!")
	file.Sync()

	return file, nil
}

// ******************************* Test Servers ****************************** //
const fsShellOpenRsp = `Hello! I am ready for the world.`
func mockServerFor_FsShellOpen() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	q := req.URL.Query()
    if q.Get("op") == OP_GETFILESTATUS {
    	fmt.Fprintln (rsp, fileStatusRsp)
    }
    if q.Get("op") == OP_OPEN{
    	fmt.Fprintln (rsp, fsShellOpenRsp)
    }
  }
  return httptest.NewServer(http.HandlerFunc(handler))	
}

func mockServerFor_FsShellChgrp() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	q := req.URL.Query()
    if q.Get("op") == OP_SETOWNER {
    	if q.Get("owner") != ""{
    		log.Fatalf("Expected owner to be empty, but was %v", q.Get("owner"))
    	}
    	if q.Get("group") == "" {
    		log.Fatalf("Expected group, but got %v ", q.Get("group"))
    	}
    	fmt.Fprintln (rsp, "")
    }else{
    	log.Fatalf("Expected op=%v, but got %v", OP_SETOWNER, q.Get("op"))
    }
  }
  return httptest.NewServer(http.HandlerFunc(handler))	
}

func mockServerFor_FsShellChown() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	q := req.URL.Query()
    if q.Get("op") == OP_SETOWNER {
    	if q.Get("owner") == ""{
    		log.Fatalf("Expected owner, but was empty%v", q.Get("owner"))
    	}
    	if q.Get("group") != "" {
    		log.Fatalf("Expected group empty, but got %v ", q.Get("group"))
    	}
    	fmt.Fprintln (rsp, "")
    }else{
    	log.Fatalf("Expected op=%v, but got %v", OP_SETOWNER, q.Get("op"))
    }
  }
  return httptest.NewServer(http.HandlerFunc(handler))	
}

func mockServerFor_FsShellChmod() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	q := req.URL.Query()
    if q.Get("op") == OP_SETPERMISSION {
    	if q.Get("permission") == ""{
    		log.Fatalf("Expected permission, but was %v", q.Get("permission"))
    	}
    	fmt.Fprintln (rsp, "")
    }else{
    	log.Fatalf("Expected op=%v, but got %v", OP_SETPERMISSION, q.Get("op"))
    }
  }
  return httptest.NewServer(http.HandlerFunc(handler))	
}

const fileNotFoundExceptionRsp =`
{
  "RemoteException":
  {
    "exception"    : "FileNotFoundException",
    "javaClassName": "java.io.FileNotFoundException",
    "message"      : "File does not exist: /foo/a.patch"
  }
}`
func mockServerFor_PutOne(redir *url.URL) *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	q := req.URL.Query()
    if q.Get("op") == OP_GETFILESTATUS {
    	fmt.Fprintln (rsp, fileNotFoundExceptionRsp)
    }
    if q.Get("op") == OP_CREATE{
      rsp.Header().Set("Location", redir.Scheme + "://" + redir.Host + req.URL.String())
      rsp.WriteHeader(http.StatusSeeOther)

      fmt.Fprintf (rsp, "")
    }
  }
  return httptest.NewServer(http.HandlerFunc(handler))	
}

