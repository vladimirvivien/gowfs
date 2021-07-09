/*
gowfs is Go bindings for the Hadoop HDFS over its WebHDFS interface.
gowfs uses JSON marshalling to expose typed values from HDFS.
See https://github.com/vladimirvivien/gowfs.
*/
package gowfs

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
)

const (
	OP_OPEN                  = "OPEN"
	OP_CREATE                = "CREATE"
	OP_APPEND                = "APPEND"
	OP_CONCAT                = "CONCAT"
	OP_RENAME                = "RENAME"
	OP_DELETE                = "DELETE"
	OP_SETPERMISSION         = "SETPERMISSION"
	OP_SETOWNER              = "SETOWNER"
	OP_SETREPLICATION        = "SETREPLICATION"
	OP_SETTIMES              = "SETTIMES"
	OP_MKDIRS                = "MKDIRS"
	OP_CREATESYMLINK         = "CREATESYMLINK"
	OP_LISTSTATUS            = "LISTSTATUS"
	OP_GETFILESTATUS         = "GETFILESTATUS"
	OP_GETCONTENTSUMMARY     = "GETCONTENTSUMMARY"
	OP_GETFILECHECKSUM       = "GETFILECHECKSUM"
	OP_GETDELEGATIONTOKEN    = "GETDELEGATIONTOKEN"
	OP_GETDELEGATIONTOKENS   = "GETDELEGATIONTOKENS"
	OP_RENEWDELEGATIONTOKEN  = "RENEWDELEGATIONTOKEN"
	OP_CANCELDELEGATIONTOKEN = "CANCELDELEGATIONTOKEN"
)

// Hack for in-lining multi-value functions
func µ(v ...interface{}) []interface{} {
	return v
}

// This type maps fields and functions to HDFS's FileSystem class.
type FileSystem struct {
	Config    Configuration
	client    http.Client
	transport *http.Transport
}

func NewFileSystem(conf Configuration) (*FileSystem, error) {
	fs := &FileSystem{
		Config: conf,
	}
	fs.transport = &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, conf.ConnectionTimeout)
			if err != nil {
				return nil, err
			}

			return c, nil
		},
		MaxIdleConnsPerHost:   conf.MaxIdleConnsPerHost,
		ResponseHeaderTimeout: conf.ResponseHeaderTimeout,
	}
	fs.client = http.Client{
		Transport: fs.transport,
	}
	return fs, nil
}

// Builds the canonical URL used for remote request
func buildRequestUrl(conf Configuration, p *Path, params *map[string]string) (*url.URL, error) {
	u, err := conf.GetNameNodeUrl()
	if err != nil {
		return nil, err
	}

	//prepare URL - add Path and "op" to URL
	if p != nil {
		if p.Name[0] == '/' {
			u.Path = u.Path + p.Name
		} else {
			u.Path = u.Path + "/" + p.Name
		}
	}

	q := u.Query()

	// attach params
	if params != nil {
		for key, val := range *params {
			q.Add(key, val)
		}
	}
	u.RawQuery = q.Encode()

	return u, nil
}

func makeHdfsData(data []byte) (HdfsJsonData, error) {
	if len(data) == 0 || data == nil {
		return HdfsJsonData{}, nil
	}
	var jsonData HdfsJsonData
	jsonErr := json.Unmarshal(data, &jsonData)

	if jsonErr != nil {
		return HdfsJsonData{}, jsonErr
	}

	// check for remote exception
	if jsonData.RemoteException.Exception != "" {
		return HdfsJsonData{}, jsonData.RemoteException
	}

	return jsonData, nil

}

func responseToHdfsData(rsp *http.Response) (HdfsJsonData, error) {
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return HdfsJsonData{}, err
	}
	return makeHdfsData(body)
}

func requestHdfsData(client http.Client, req http.Request) (HdfsJsonData, error) {
	rsp, err := client.Do(&req)
	if err != nil {
		return HdfsJsonData{}, err
	}
	defer rsp.Body.Close()
	hdfsData, err := responseToHdfsData(rsp)
	return hdfsData, err
}

func (fs *FileSystem) sendHttpRequest(method string, p *Path, params *map[string]string, body io.Reader, byTransport bool) (rsp *http.Response, err error) {
	var (
		attemptHosts []string
		u *url.URL
	)
	defer func() {
		if rc := recover(); rc != nil {
			debugStack := ""
			for _, v := range strings.Split(string(debug.Stack()), "\n") {
				debugStack += v + "\n"
			}
			log.Printf("panic: %v, %v\n", rc, debugStack)
			return
		}
	}()
start:
	u, err = buildRequestUrl(fs.Config, p, params)
	if err != nil {
		// 倘若url都拿不到了，直接返回即可
		return
	}
	//log.Printf("start send http request, url: %s \n", u.String())
	req, _ := http.NewRequest(method, u.String(), body)
	if byTransport {
		rsp, err = fs.transport.RoundTrip(req)
	} else {
		rsp, err = fs.client.Do(req)
	}
	if err != nil || rsp != nil && (rsp.StatusCode < 200 || rsp.StatusCode >= 400) {
		if err != nil {
			log.Printf("sendHttpRequest failed, url:%s, err: %v", u.String(), err.Error())
		} else {
			log.Printf("sendHttpRequest failed, url:%s, response code: %v", u.String(), rsp.StatusCode)
		}
		// 如果请求失败，则尝试其他addr
		addr := HdfsAddrQueue.Front().(string)
		attemptHosts = append(attemptHosts, addr)
		if len(attemptHosts) == HdfsAddrQueue.Len() {
			// 如果两个长度一样，则视为所有addr都无效，也返回
			log.Printf("has try all available ip, but unusefully, so terriable...")
			return
		} else {
			// 切换addr
			HdfsAddrQueue.Rotate(-1)
			if rsp != nil {
				rsp.Body.Close()
			}
			goto start
		}
	//} else {
	//	if rsp != nil {
	//		log.Printf("err is nil, response code: %d\n", rsp.StatusCode)
	//	} else {
	//		log.Printf("err is nil\n")
	//	}
	}
	return
}