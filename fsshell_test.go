package gowfs

import "bytes"
import "os"
import "testing"
import "net/url"
import "net/http"
import "net/http/httptest"
import "fmt"

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
  	server1 := mockServerFor_FshellCat()
  	url, _  := url.Parse(server1.URL)
  	fs,  _  := NewFileSystem(Configuration{Addr: url.Host })
	shell   := FsShell{FileSystem: fs}

	var output bytes.Buffer
	shell.Cat([]string{"/remote/file1", "/remote/file2"}, &output)
	msgCat := strings.TrimSpace(string(output.Bytes()))
	if msgCat != (fsShellCatRsp + "\n" + fsShellCatRsp) {
		t.Fatal("FsShell.Cat() not getting content in writer.")
	}

  	defer server1.Close()
}

func createTestFile(fileName string) (*os.File, error) {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.WriteString("Hello webhdfs users!")
	file.Sync()

	return file, nil
}

// ******************* Test Servers ************************ //
const fsShellCatRsp = `Hello! I am ready for the world.`
func mockServerFor_FshellCat() *httptest.Server {
  handler := func (rsp http.ResponseWriter, req *http.Request){
  	q := req.URL.Query()
    if q.Get("op") == OP_GETFILESTATUS {
    	fmt.Fprintln (rsp, fileStatusRsp)
    }
    if q.Get("op") == OP_OPEN{
    	fmt.Fprintln (rsp, fsShellCatRsp)
    }
  }
  return httptest.NewServer(http.HandlerFunc(handler))	
}