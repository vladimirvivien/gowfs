package gowfs

import "fmt"
import "log"
import "net/url"
import "net/http"
import "net/http/httptest"
import _ "strconv"

import "testing"

func Test_GetDelegationToken(t *testing.T) {
	server := mockServerFor_GetDelegationToken()
	defer server.Close()
	t.Logf("Test_GetDelegationToken - Started httptest.Server on %v", server.URL)

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host}
	fs, _ := NewFileSystem(conf)

	token, err := fs.GetDelegationToken("hdfsuser")
	if err != nil {
		t.Fatal(err)
	}

	if token.UrlString == "" {
		t.Fatal("Token object does not contain data.")
	}
}

func Test_GetDelegationTokens(t *testing.T) {
	server := mockServerFor_GetDelegationTokens()
	defer server.Close()
	t.Logf("Test_GetDelegationTokens - Started httptest.Server on %v", server.URL)

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host}
	fs, _ := NewFileSystem(conf)

	tokens, err := fs.GetDelegationTokens("hdfsuser")
	if err != nil {
		t.Fatal(err)
	}

	if len(tokens) != 2 {
		t.Fatal("GetDelegationTokens - Token array not being returned.")
	}
}

func Test_RenewDelegationTokens(t *testing.T) {
	server := mockServerFor_RenewDelegationToken()
	defer server.Close()
	t.Logf("Test_RenewDelegationTokens - Started httptest.Server on %v", server.URL)

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host}
	fs, _ := NewFileSystem(conf)

	token, err := fs.RenewDelegationToken("KAAKSm9ie2radfaerzadfDcqdt14AfeE=")
	if err != nil {
		t.Fatal(err)
	}

	if token != 123456789 {
		t.Fatal("RenewDelegationToken - Not returning expected value.")
	}
}

func Test_CancelDelegationTokens(t *testing.T) {
	server := mockServerFor_CancelDelegationToken()
	defer server.Close()
	t.Logf("Test_CancelDelegationTokens - Started httptest.Server on %v", server.URL)

	url, _ := url.Parse(server.URL)
	conf := Configuration{Addr: url.Host}
	fs, _ := NewFileSystem(conf)

	ok, err := fs.CancelDelegationToken("KAAKSm9ie2radfaerzadfDcqdt14AfeE=")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("CancelDelegationToken - Not cancelling token value.")
	}
}

// *********************** Mock Servers ********************* //
const getDelegationTokenRsp = `
{
  "Token":
  {
    "urlString": "JQAIaG9y1afae2radfaerzcqdt14AfeE="
  }
}
`

func mockServerFor_GetDelegationToken() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			log.Fatalf("Expecting Request.Method PUT, but got %v", req.Method)
		}

		q := req.URL.Query()
		if q.Get("op") != OP_GETDELEGATIONTOKEN {
			log.Fatalf("Server Missing expected URL parameter: op= %v", OP_GETDELEGATIONTOKEN)
		}
		if q.Get("renewer") != "hdfsuser" {
			log.Fatalf("Expected param renewer to be hdfsuser, but was %v", q.Get("renewer"))
		}

		fmt.Fprintf(rsp, getDelegationTokenRsp)
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

const getDelegationTokensRsp = `
{
  "Tokens":
  {
    "Token":
    [
      {
        "urlString":"KAAKSm9ie2radfaerzadfDcqdt14AfeE="
      },
      {
        "urlString":"JQAIaG9y1afae2radfaerzcqdt14AfeE="
      }      
    ]
  }
}
`

func mockServerFor_GetDelegationTokens() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			log.Fatalf("Expecting Request.Method GET, but got %v", req.Method)
		}

		q := req.URL.Query()
		if q.Get("op") != OP_GETDELEGATIONTOKENS {
			log.Fatalf("Server Missing expected URL parameter: op= %v", OP_GETDELEGATIONTOKENS)
		}
		if q.Get("renewer") != "hdfsuser" {
			log.Fatalf("Expected param renewer to be hdfsuser, but was %v", q.Get("renewer"))
		}

		fmt.Fprintf(rsp, getDelegationTokensRsp)
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func mockServerFor_RenewDelegationToken() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			log.Fatalf("Expecting Request.Method PUT, but got %v", req.Method)
		}

		q := req.URL.Query()
		if q.Get("op") != OP_RENEWDELEGATIONTOKEN {
			log.Fatalf("Server Missing expected URL parameter: op= %v", OP_RENEWDELEGATIONTOKEN)
		}
		if q.Get("token") == "" {
			log.Fatalf("Expected param token, but was empty")
		}

		fmt.Fprintf(rsp, `{"long":123456789}`)
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func mockServerFor_CancelDelegationToken() *httptest.Server {
	handler := func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			log.Fatalf("Expecting Request.Method PUT, but got %v", req.Method)
		}

		q := req.URL.Query()
		if q.Get("op") != OP_CANCELDELEGATIONTOKEN {
			log.Fatalf("Server Missing expected URL parameter: op= %v", OP_CANCELDELEGATIONTOKEN)
		}
		if q.Get("token") == "" {
			log.Fatalf("Expected param token, but was empty")
		}

		fmt.Fprintf(rsp, "")
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}
