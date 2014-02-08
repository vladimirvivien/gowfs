package gowfs

import "testing"

import "net/url"
import "fmt"
import "log"
import "io/ioutil"
import "net/http"
import "strings"
import "net/http/httptest"

func Test_Create(t *testing.T){
	server := mockServerFor_CreatFile()
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)

	p, err := fs.Create(
		Path{Name:"/testing/newfile"},
		false,
		0,
		0,
		0744,
		0,
	)

	if err != nil {
		t.Fatal (err)
	}

	if p.Name == "" {
		t.Errorf ("URL does not contain expected value, expecting %v", "/testing/newfile")
	}
}

func Test_WriteFile(t *testing.T) {
	server := mockServerFor_WriteFile() 
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)

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
	if _, err := fs.Write (data, Path{Name:u.Path, RefererUrl:*u}); err != nil {
		t.Fatal(err)
	}
}

func Test_OpenAndRead(t *testing.T) {
	server := mockServerFor_OpenAndRead()
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)

	url,_ := url.Parse(server.URL)

	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)
	
	data, err := fs.OpenAndRead(Path{Name:"/test"}, 0, 512, 2048)
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
	t.Logf("Started httptest.Server on %v", server.URL)

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)

	p, err := fs.OpenForAppend(Path{Name:"/testing/existing.f"}, 4096)

	if err != nil {
		t.Fatal (err)
	}

	if p.Name == "" {
		t.Errorf ("URL does not contain expected value, expecting %v", "/testing/newfile")
	}
}

func Test_Append(t *testing.T) {
	server := mockServerFor_Append() 
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)

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
	if _, err := fs.Append (data, Path{Name:u.Path, RefererUrl:*u}); err != nil {
		t.Fatal(err)
	}
}

func Test_Concat(t *testing.T) {
	server := mockServerFor_Concat() 
	defer server.Close()
	t.Logf("Started httptest.Server on %v", server.URL)

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)

	_, err := fs.Concat(Path{Name:"/testing/concat.f"}, []string{"a/b/c", "e/f/g"})

	if err != nil {
		t.Fatal (err)
	}

}

// ***************************** Mock Servers for Tests **********************//

func mockServerFor_OpenAndRead() *httptest.Server{
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	  if req.Method != "GET"{
  	      log.Fatalf("Expecting Request.Method GET, but got %v", req.Method)
  	  }
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
  return httptest.NewServer(http.HandlerFunc(handler))
}


func mockServerFor_CreatFile() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	  if req.Method != "PUT"{
  	      log.Fatalf("Expecting Request.Method PUT, but got %v", req.Method)
  	  }  	
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
  	  if req.Method != "PUT"{
  	      log.Fatalf("Expecting Request.Method PUT, but got %v", req.Method)
  	  }  	

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
  
  return httptest.NewServer(http.HandlerFunc(handler))
}


func mockServerFor_OpenForAppend() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	  if req.Method != "POST"{
  	      log.Fatalf("Expecting Request.Method POST, but got %v", req.Method)
  	  }  	

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
  	  if req.Method != "POST"{
  	      log.Fatalf("Expecting Request.Method POST, but got %v", req.Method)
  	  }  	
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
  
  return httptest.NewServer(http.HandlerFunc(handler))
}

func mockServerFor_Concat() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	  if req.Method != "POST"{
  	      log.Fatalf("Expecting Request.Method POST, but got %v", req.Method)
  	  }  	

      q := req.URL.Query()
      if q.Get("op") != OP_CONCAT{
        log.Fatalf("Server Missing expected URL parameter: op=%v", OP_CONCAT)
      }
      if q.Get("sources") != strings.Join([]string{"a/b/c", "e/f/g"}, ",") {
          log.Fatalf("Expected param sources a/b/c, e/f/g, but was %v", q.Get("sources"))
      }

      fmt.Fprintf (rsp, "")
  }
  
  return httptest.NewServer(http.HandlerFunc(handler))
}