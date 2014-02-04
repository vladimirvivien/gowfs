package gowfs


import "testing"

func Test_GetNameNodeUrl(t *testing.T) {
	conf := Configuration{Addr:"localhost:8080", BasePath:"/test/gofs", User:"vvivien"}
	u, err := conf.GetNameNodeUrl()

	if err != nil {
		t.Fatal(err)
	}

	if u.Scheme != "http" {
		t.Errorf("Expecting url.Scheme http, but got %s", u.Scheme)
	}

	if u.Host != "localhost:8080" {
		t.Errorf("Expecting url.Host locahost:8080, but got %s", u.Host)
	}

	if u.Path != WebHdfsVer+conf.BasePath {
		t.Errorf("Expecting url.Path %s, but got %s", WebHdfsVer+conf.BasePath, u.Path)
	}

	if u.Query().Get("user.name") != conf.User {
		t.Errorf("Expecting param user.name=%s, but user.name=%s [url=%v]", conf.User, u.Query().Get("user.name"), u)
	}
}