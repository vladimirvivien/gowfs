package gowfs

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Creates a new file and stores its content in HDFS.
// See HDFS FileSystem.create()
// For detail, http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
// See NOTE section on that page for impl detail.
func (fs *FileSystem) Create(
	data io.Reader,
	p Path,
	overwrite bool,
	blocksize uint64,
	replication uint16,
	permission os.FileMode,
	buffersize uint,
	contenttype string) (bool, error) {

	params := map[string]string{"op": OP_CREATE}
	params["overwrite"] = strconv.FormatBool(overwrite)

	if blocksize == 0 {
		params["blocksize"] = "134217728" // from hdfs-default.xml (ver 2)
	} else {
		params["blocksize"] = strconv.FormatInt(int64(blocksize), 10)
	}

	if replication == 0 {
		params["replication"] = "3"
	} else {
		params["replication"] = strconv.FormatInt(int64(replication), 10)
	}

	if permission <= 0 || permission > 1777 {
		params["permission"] = "0700"
	} else {
		params["permission"] = strconv.FormatInt(int64(permission), 8)
	}

	if buffersize == 0 {
		params["buffersize"] = "4096"
	} else {
		params["buffersize"] = strconv.FormatInt(int64(buffersize), 10)
	}

	// take over default transport to avoid redirect
	rsp, reqErr := fs.sendHttpRequest("PUT", &p, &params, nil, true)
	if reqErr != nil {
		return false, reqErr
	}
	defer rsp.Body.Close()

	// extract returned url in header.  -- 这里以返回header的URI为准，发生失败时不考虑更换HDFS Addr, 下面逻辑正常走
	loc := rsp.Header.Get("Location")
	u, err := url.ParseRequestURI(loc)
	if err != nil {
		return false, fmt.Errorf("FileSystem.Create(%s) - invalid redirect URL from server: %s", u, err.Error())
	}
	req, _ := http.NewRequest("PUT", u.String(), data)
	// set content type
	if contenttype != "" {
		req.Header.Set("Content-Type", contenttype)
	}
	rsp, err = fs.client.Do(req)
	if err != nil {
		fmt.Errorf("FileSystem.Create(%s) - bad url: %s", loc, err.Error())
		return false, err
	}

	if rsp.StatusCode != http.StatusCreated {
		defer rsp.Body.Close()
		_, err = responseToHdfsData(rsp)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("FileSystem.Create(%s) - File not created.  Server returned status %v", loc, rsp.StatusCode)
	}

	return true, nil
}

//Opens the specificed Path and returns its content to be accessed locally.
//See HDFS WebHdfsFileSystem.open()
// See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#HTTP_Query_Parameter_Dictionary
func (fs *FileSystem) Open(p Path, offset, length int64, buffSize int) (io.ReadCloser, error) {
	params := map[string]string{"op": OP_OPEN}

	if offset < 0 {
		params["offset"] = "0"
	} else {
		params["offset"] = strconv.FormatInt(offset, 10)
	}

	if length > 0 {
		params["length"] = strconv.FormatInt(length, 10)
	}

	if buffSize <= 0 {
		params["buffersize"] = "4096"
	} else {
		params["buffersize"] = strconv.Itoa(buffSize)
	}
	rsp, err := fs.sendHttpRequest("GET", &p, &params, nil, false)
	if err != nil {
		return nil, err
	}
	//defer rsp.Body.Close()
	// possible error
	if rsp.StatusCode != http.StatusOK {
		defer rsp.Body.Close()
		_, err = responseToHdfsData(rsp)
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("Open(%s) - File not opened.  Server returned status %v", p.Name, rsp.StatusCode)
	}

	return rsp.Body, nil
}

// Appends specified data to an existing file.
// See HDFS FileSystem.append()
// See http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File
// NOTE: Append() is known to have issues - see https://issues.apache.org/jira/browse/HDFS-4600
func (fs *FileSystem) Append(data io.Reader, p Path, buffersize int, contenttype string) (bool, error) {
	params := map[string]string{"op": OP_APPEND}

	if buffersize == 0 {
		params["buffersize"] = "4096"
	} else {
		params["buffersize"] = strconv.FormatInt(int64(buffersize), 10)
	}

	// take over default transport to avoid redirect
	rsp, reqErr := fs.sendHttpRequest("POST", &p, &params, nil, true)
	if reqErr != nil {
		return false, reqErr
	}
	defer rsp.Body.Close()

	// extract returned url in header. -- 这里以返回header的URI为准，发生失败时不考虑更换HDFS Addr, 下面逻辑正常走
	loc := rsp.Header.Get("Location")
	u, err := url.ParseRequestURI(loc)
	if err != nil {
		return false, fmt.Errorf("Append(%s) - did not receive a valid URL from server.", loc)
	}

	req, _ := http.NewRequest("POST", u.String(), data)
	// set content type
	if contenttype != "" {
		req.Header.Set("Content-Type", contenttype)
	}
	rsp, err = fs.client.Do(req)
	if err != nil {
		return false, err
	}

	if rsp.StatusCode != http.StatusOK {
		defer rsp.Body.Close()
		_, err = responseToHdfsData(rsp)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("Append(%s) - File not created.  Server returned status %v", loc, rsp.StatusCode)
	}

	return true, nil
}

// Concatenate (on the server) a list of given files paths to a new file.
// See HDFS FileSystem.concat()
func (fs *FileSystem) Concat(target Path, sources []string) (bool, error) {
	if (target == Path{}) {
		return false, fmt.Errorf("Concat() - The target path must be provided.")
	}
	params := map[string]string{"op": OP_CONCAT}
	params["sources"] = strings.Join(sources, ",")
	rsp, err := fs.sendHttpRequest("POST", &target, &params, nil, false)
	if err != nil {
		return false, err
	}
	defer rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK {
		defer rsp.Body.Close()
		_, err = responseToHdfsData(rsp)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("Concat(%s) - File not concatenated.  Server returned status %v", rsp.Request.URL, rsp.StatusCode)
	}
	return true, nil
}
