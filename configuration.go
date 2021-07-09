package gowfs

import (
	"errors"
	"fmt"
	"github.com/gammazero/deque"
	"net/url"
	"os/user"
	"strings"
	"time"
)

const WebHdfsVer string = "/webhdfs/v1"

var HdfsAddrQueue deque.Deque


type Configuration struct {
	Addr                  string // host:port;host:port
	BasePath              string // initial base path to be appended
	UseHTTPS              bool   // 是否启用https请求hdfs
	User                  string // user.name to use to connect
	ConnectionTimeout     time.Duration
	DisableKeepAlives     bool
	DisableCompression    bool
	ResponseHeaderTimeout time.Duration
	MaxIdleConnsPerHost   int
}

func NewConfiguration(addrList, basePath, user string, https bool) *Configuration {
	for _, addr := range strings.Split(addrList, ";") {
		HdfsAddrQueue.PushBack(addr)
	}
	return &Configuration{
		Addr:                  addrList,
		BasePath:              basePath,
		UseHTTPS:              https,
		User:                  user,
		ConnectionTimeout:     time.Second * 17,
		DisableKeepAlives:     false,
		DisableCompression:    true,
		ResponseHeaderTimeout: time.Second * 17,
	}
}

func (conf *Configuration) GetNameNodeUrl() (*url.URL, error) {
	if &conf.Addr == nil {
		return nil, errors.New("configuration namenode address not set")
	}

	if HdfsAddrQueue.Len() <= 0 {
		return nil, errors.New("no available namenode address can be set")
	}

	scheme := "http"
	if conf.UseHTTPS {
		scheme = "https"
	}

	var addr string
	if HdfsAddrQueue.Len() > 0 {
		addr = HdfsAddrQueue.Front().(string)
	}
	urlStr := fmt.Sprintf("%s://%s%s%s", scheme, addr, WebHdfsVer, conf.BasePath)

	if &conf.User == nil || len(conf.User) == 0 {
		u, _ := user.Current()
		conf.User = u.Username
	}
	urlStr = urlStr + "?user.name=" + conf.User

	u, err := url.Parse(urlStr)

	if err != nil {
		return nil, err
	}

	return u, nil
}
