package gowfs

import "io/ioutil"
import "net/url"
import "testing"


func Test_NewFileSystem(t *testing.T) {
	conf := Configuration{Addr:"localhost:8080"}
	fs, err := NewFileSystem(conf)
	if err != nil{
		t.Fatal(err)
	}
	if &fs.Config == nil {
		t.Fatal("Filesystem missing Configuration")
	}
	if &fs.client == nil {
		t.Fatal("http.Client not set")
	}
}

func Test_buildRequestUrl(t *testing.T){
	url1 := url.URL{Scheme:"http", Host:"localhost:8080", Path:"/webhdfs/v1/test"}
	conf := Configuration{Addr:url1.Host}

	u, err := buildRequestUrl (conf, &Path{"/test"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if url1.String() != u.String() {
		t.Errorf("Expecting url [%v], but got [%v]", url1.String(), u.String())
	}

	// test with params
	v := url.Values{}
	v.Add("op1", "OP_1")
	v.Add("op2", "OP_2")
	url1.RawQuery = v.Encode()

	params := map[string]string {
		"op1": "OP_1",
		"op2": "OP_2",
	}

	u, err = buildRequestUrl (conf, &Path{"/test"}, &params)
	if url1.String() != u.String() {
		t.Errorf("Expecting url [%v], but got [%v]", url1.String(), u.String())
	}	
}

func Test_Open(t *testing.T) {
	server := getOpenFileServer()
	defer server.Close()

	url,_ := url.Parse(server.URL)

	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)
	
	// data is io.ReaderCloser
	data, err := fs.Open(Path{Path:"/test"}, 0, 512, 2048)
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


func Test_MkDirs(t *testing.T){
	server := getMkDirsServer()
	defer server.Close()
	url,_ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)
	
	ok, err := fs.MkDirs(Path{Path:"/test"}, 0744)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("MkDirs - is not returning expected FileStatus value")
	}
}

func Test_CreateSymlink(t *testing.T) {
	server := getCreateSymlinkServer()
	defer server.Close()
	url,_ := url.Parse(server.URL)
	conf  := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)

	ok, err := fs.CreateSymlink(Path{Path:"/test/orig"}, Path{Path:"/symlink"}, false)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("MkDirs - is not returning expected FileStatus value")
	}
}

func Test_GetFileStatus(t *testing.T){
	server := getFileStatusServer()
	defer server.Close()

	url,_ := url.Parse(server.URL)

	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)
	
	fileStatus, err := fs.GetFileStatus(Path{Path:"/test"})
	if err != nil {
		t.Fatal(err)
	}

	if fileStatus.Permission != "777" || fileStatus.Type == "DIRECORY" {
		t.Fatal("GetFileStatus - is not returning expected FileStatus value")
	}
}

func Test_ListStatus(t *testing.T){
	server := getListStatusServer()
	defer server.Close()

	url,_ := url.Parse(server.URL)

	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)
	
	statuses, err := fs.ListStatus(Path{Path:"/test"})
	if err != nil {
		t.Fatal(err)
	}

	if len(statuses) != 2 {
		t.Errorf("ListStatus - expecting %d items, but got %d.", 2, len(statuses))
	}
}

func Test_GetContentSummary(t *testing.T) {
	server := getContentSummaryServer()
	defer server.Close()
	url, _ := url.Parse(server.URL)
	fs, _  := NewFileSystem(Configuration{Addr:url.Host})
	summary, err := fs.GetContentSummary(Path{Path:"/test"})
	if err != nil {
		t.Fatal (err)
	}
	if summary.SpaceConsumed != 24930 {
		t.Errorf("GetContentSummary - not returning expected values <<%v>>", summary)
	}
}

func Test_GetFileChecksum(t *testing.T) {
	server := getFileChecksumServer()
	defer server.Close()
	url, _ := url.Parse(server.URL)
	fs, _  := NewFileSystem(Configuration{Addr:url.Host})
	checksum, err := fs.GetFileChecksum(Path{Path:"/test"})
	if err != nil {
		t.Fatal (err)
	}
	if checksum.Algorithm != "MD5-of-1MD5-of-512CRC32" ||
	   checksum.Length != 28 {
		t.Errorf("GetFileChecksum - not returning expected values <<%v>>", checksum)
	}
}
