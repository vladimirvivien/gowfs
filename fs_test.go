package gowfs

import "net/url"
import "testing"
import "os/user"

func Test_NewFileSystem(t *testing.T) {
	conf := Configuration{Addr: "localhost:8080"}
	fs, err := NewFileSystem(conf)
	if err != nil {
		t.Fatal(err)
	}
	if &fs.Config == nil {
		t.Fatal("Filesystem missing Configuration")
	}
	if &fs.client == nil {
		t.Fatal("http.Client not set")
	}
}

func Test_buildRequestUrl(t *testing.T) {
	user, _ := user.Current()
	url1 := url.URL{Scheme: "http", Host: "localhost:8080", Path: "/webhdfs/v1/test"}

	q := url1.Query()
	q.Set("user.name", user.Username)
	url1.RawQuery = q.Encode()

	conf := Configuration{Addr: url1.Host}

	u, err := buildRequestUrl(conf, &Path{Name: "/test"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if url1 != *u {
		t.Errorf("Expecting url [%v], but got [%v]", url1.String(), u.String())
	}

	// test with params
	v := url.Values{}
	v.Add("op1", "OP_1")
	v.Add("op2", "OP_2")
	v.Add("user.name", user.Username)
	url1.RawQuery = v.Encode()

	params := map[string]string{
		"op1": "OP_1",
		"op2": "OP_2",
	}

	u, err = buildRequestUrl(conf, &Path{Name: "/test"}, &params)
	if url1 != *u {
		t.Errorf("Expecting url [%v], but got [%v]", url1.String(), u.String())
	}
}
