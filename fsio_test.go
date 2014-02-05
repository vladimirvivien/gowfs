package gowfs

import "net/url"
import "io/ioutil"
import "testing"

func Test_Create(t *testing.T){
	server := getCreateFileServer()
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
	server := getWriteFileServer()
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
	if _, err := fs.WriteFile (data, *u); err != nil {
		t.Fatal(err)
	}
}

func Test_Open(t *testing.T) {
	server := getOpenFileServer()
	defer server.Close()

	url,_ := url.Parse(server.URL)

	conf := Configuration{Addr: url.Host }
	fs, _ := NewFileSystem(conf)
	
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