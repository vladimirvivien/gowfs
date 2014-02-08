package gowfs

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

	u, err := buildRequestUrl (conf, &Path{Path:"/test"}, nil)
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

	u, err = buildRequestUrl (conf, &Path{Path:"/test"}, &params)
	if url1.String() != u.String() {
		t.Errorf("Expecting url [%v], but got [%v]", url1.String(), u.String())
	}	
}
