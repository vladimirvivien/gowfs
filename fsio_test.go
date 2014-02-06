package gowfs

import "testing"

import "net/url"
import "fmt"
import "io/ioutil"
import "log"
import "net/http"
import "strings"
import "net/http/httptest"

func Test_Create(t *testing.T){
	server := mockServerFor_CreatFile()
	defer server.Close()

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)

	u, err := fs.Create(
		Path{Path:"/testing/newfile"},
		false,
		0,
		0,
		0744,
		0,
	)

	if err != nil {
		t.Fatal (err)
	}

	if u.Path == "" {
		t.Errorf ("URL does not contain expected value, expecting %v", "/testing/newfile")
	}
}

func Test_WriteFile(t *testing.T) {
	server := mockServerFor_WriteFile() 
	defer server.Close()

	servUrl, _ := url.Parse(server.URL)
	conf := Configuration{Addr: servUrl.Host}
	fs, _ := NewFileSystem(conf)
	u, _ := conf.GetNameNodeUrl()
	u.Path = u.Path + "/testing/newfile"
	vals := url.Values{
		"op"			: []string{OP_CREATE},
		"blocksize"		: []string{"134217728"},
		"replication" 	: []string{"3"}, 
		"permission" 	: []string{"744"},
		"buffersize"	: []string{"4096"},
	}
	u.RawQuery = vals.Encode()

	data := []byte("Hello webhdfs users!")
	if _, err := fs.Write (data, Path{Path:u.Path, RefererUrl:*u}); err != nil {
		t.Fatal(err)
	}
}

func Test_OpenAndRead(t *testing.T) {
	server := getOpenFileServer()
	defer server.Close()

	url,_ := url.Parse(server.URL)

	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)
	
	data, err := fs.OpenAndRead(Path{Path:"/test"}, 0, 512, 2048)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close() // make sure to close.

	rcvdData, _ := ioutil.ReadAll(data)

	expectedData := []byte("Hello, webhdfs user!")
	if (string(rcvdData) != string(expectedData)){
		t.Errorf("Open() - Expecting binary response [%v], but got [%v]", 
			string(expectedData), string(rcvdData))
	}
}

func Test_OpenForAppend(t *testing.T){
	server := mockServerFor_OpenForAppend()
	defer server.Close()

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)

	u, err := fs.OpenForAppend(Path{Path:"/testing/existing.f"}, 4096)

	if err != nil {
		t.Fatal (err)
	}

	if u.Path == "" {
		t.Errorf ("URL does not contain expected value, expecting %v", "/testing/newfile")
	}
}

func Test_Append(t *testing.T) {
	server := mockServerFor_Append() 
	defer server.Close()

	servUrl, _ := url.Parse(server.URL)
	conf := Configuration{Addr: servUrl.Host}
	fs, _ := NewFileSystem(conf)
	u, _ := conf.GetNameNodeUrl()
	u.Path = u.Path + "/testing/existing.f"
	vals := url.Values{
		"op"			: []string{OP_APPEND},
		"buffersize"	: []string{"4096"},
	}
	u.RawQuery = vals.Encode()

	data := []byte("Hello webhdfs users!")
	if _, err := fs.Write (data, Path{Path:u.Path, RefererUrl:*u}); err != nil {
		t.Fatal(err)
	}
}

func Test_Concat(t *testing.T) {
	server := mockServerFor_Concat() 
	defer server.Close()

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)

	_, err := fs.Concat(Path{Path:"/testing/concat.f"}, []string{"a/b/c", "e/f/g"})

	if err != nil {
		t.Fatal (err)
	}

}

// ***************************** Mock Servers for Tests **********************//

func mockServerFor_CreatFile() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
      q := req.URL.Query()
      if q.Get("op") != OP_CREATE{
        log.Fatalf("Server Missing expected URL parameter: op=%v", OP_CREATE)
      }
      if q.Get("blocksize") != "134217728" {
          log.Fatalf("Expected param blocksize to be 134217728, but was %v", q.Get("blocksize"))
      }      
      if q.Get("replication") != "3" {
          log.Fatalf("Expected param replciation to be 3, but was %v", q.Get("replication"))
      }
      if q.Get("permission") != "744" {
          log.Fatalf("Expected param offset to be 744, but was %v", q.Get("permission"))
      }
      if q.Get("buffersize") != "4096" {
          log.Fatalf("Expected param offset to be 4096, but was %v", q.Get("buffersize"))
      }

      rsp.Header().Set("Location",req.URL.String())
      rsp.WriteHeader(http.StatusSeeOther)

      fmt.Fprintf (rsp, "")
  }

  return httptest.NewServer(http.HandlerFunc(handler))
}

func mockServerFor_WriteFile() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
      q := req.URL.Query()
      if q.Get("op") != OP_CREATE{
        log.Fatalf("Server Missing expected URL parameter: op=%v", OP_CREATE)
      }
      if q.Get("blocksize") != "134217728" {
          log.Fatalf("Expected param blocksize to be 134217728, but was %v", q.Get("blocksize"))
      }      
      if q.Get("replication") != "3" {
          log.Fatalf("Expected param replciation to be 3, but was %v", q.Get("replication"))
      }
      if q.Get("permission") != "744" {
          log.Fatalf("Expected param offset to be 744, but was %v", q.Get("permission"))
      }
      if q.Get("buffersize") != "4096" {
          log.Fatalf("Expected param offset to be 4096, but was %v", q.Get("buffersize"))
      }

      // ensure data maded it
      data, _ := ioutil.ReadAll(req.Body)
      defer req.Body.Close()

      if string(data) != "Hello webhdfs users!" {
          log.Fatalf("Expected data not posted to server. Server got %v", string(data))
      }

      rsp.WriteHeader(http.StatusCreated)
      fmt.Fprintf (rsp, "")
  }
  
  return makeMockWebServer(handler)
}


func mockServerFor_OpenForAppend() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
      q := req.URL.Query()
      if q.Get("op") != OP_APPEND{
        log.Fatalf("Server Missing expected URL parameter: op=%v", OP_APPEND)
      }
      if q.Get("buffersize") != "4096" {
          log.Fatalf("Expected param offset to be 4096, but was %v", q.Get("buffersize"))
      }

      rsp.Header().Set("Location",req.URL.String())
      rsp.WriteHeader(http.StatusSeeOther)

      fmt.Fprintf (rsp, "")
  }

  return httptest.NewServer(http.HandlerFunc(handler))
}

func mockServerFor_Append() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
      q := req.URL.Query()
      if q.Get("op") != OP_APPEND{
        log.Fatalf("Server Missing expected URL parameter: op=%v", OP_APPEND)
      }
      if q.Get("buffersize") != "4096" {
          log.Fatalf("Expected param offset to be 4096, but was %v", q.Get("buffersize"))
      }

      // ensure data maded it
      data, _ := ioutil.ReadAll(req.Body)
      defer req.Body.Close()

      if string(data) != "Hello webhdfs users!" {
          log.Fatalf("Expected data not posted to server. Server got %v", string(data))
      }

      rsp.WriteHeader(http.StatusCreated)
      fmt.Fprintf (rsp, "")
  }
  
  return makeMockWebServer(handler)
}

func mockServerFor_Concat() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
      q := req.URL.Query()
      if q.Get("op") != OP_CONCAT{
        log.Fatalf("Server Missing expected URL parameter: op=%v", OP_CONCAT)
      }
      if q.Get("sources") != strings.Join([]string{"a/b/c", "e/f/g"}, ",") {
          log.Fatalf("Expected param sources a/b/c, e/f/g, but was %v", q.Get("sources"))
      }

      fmt.Fprintf (rsp, "")
  }
  
  return makeMockWebServer(handler)
}