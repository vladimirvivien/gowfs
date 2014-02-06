package gowfs

import "fmt"
import "os"
import "io"
import "bytes"
import "strconv"
import "strings"
import "net/url"
import "net/http"

// Creates a file on HDFS to be written/appended (call WriteFile() below to tranfer content).
// See HDFS FileSystem.create() 
// For detail, http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
// See NOTE section on that page for impl detail.
func (fs *FileSystem) Create(
	p Path, 
	overwrite bool, 
	blocksize uint64, 
	replication uint16, 
	permission os.FileMode, 
	buffersize uint) (Path, error){

	params := map[string]string{"op":OP_CREATE}
	params["overwrite"] = strconv.FormatBool(overwrite)
	
	if blocksize == 0 {
		params["blocksize"] = "134217728" // from hdfs-default.xml (ver 2)
	}else{
		params["blocksize"] = strconv.FormatInt(int64(blocksize), 10)
	}

	if replication == 0 {
		params["replication"] = "3"
	}else{
		params["replication"] = strconv.FormatInt(int64(replication), 10)
	}

	if permission <= 0 || permission > 1777 {
		params["permission"] = "0700"
	}else{
		params["permission"] = strconv.FormatInt(int64(permission), 8)
	}

	if buffersize == 0 {
		params["buffersize"] = "4096"
	}else{
		params["buffersize"] = strconv.FormatInt(int64(buffersize), 10)
	}

	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return Path{}, err
	}

	// take over default transport to avoid redirect
	tr := &http.Transport{}
	req, _ := http.NewRequest("PUT", u.String(), nil)
	rsp, err := tr.RoundTrip(req)

	if err != nil {
		return Path{}, err
	}

	// extract returned url in header.
	loc := rsp.Header.Get("Location")

	if loc == "" {
		return Path{}, fmt.Errorf("Create() - did not receive URL for newly created HDFS file.")
	}

	u, err = url.Parse(loc)
	if err != nil {
		return Path{}, fmt.Errorf("Create() - did not receive a valid URL from server.")
	}

	return Path{Path:p.Path, RefererUrl:*u}, nil
}

// This function writes provied buffer to specified Path.RefererUrl.
// Call this function after a call to Create().
// For detail, http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
func (fs *FileSystem) Write(data []byte, p Path) (bool, error){
	locStr := p.RefererUrl.String()
	if locStr == "" {
		return false, fmt.Errorf("Write() - Parameter Path missing a URL referer.")
	}

	if _, err := url.Parse(locStr); err != nil {
		return false, err
	}

	rsp, err := http.Post(locStr, "application/octet-stream", bytes.NewBuffer(data))
	if  err != nil  {
		return false, err
	}

	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusCreated {
		return false, fmt.Errorf("File not created.  Server returned status %v", rsp.StatusCode)
	}

	return true, nil
}

//Opens the specificed Path and Read the content of the File. 
//See HDFS WebHdfsFileSystem.open()
// See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#HTTP_Query_Parameter_Dictionary
func (fs *FileSystem) OpenAndRead(p Path, offset, length int64, buffSize int) (io.ReadCloser, error){
	params := map[string]string{"op":OP_OPEN}

	if offset < 0{
		params["offset"] = "0"
	}else{
		params["offset"] = strconv.FormatInt(offset, 10)
	}

	if length > 0{
		params["length"] = strconv.FormatInt(length, 10)
	}

	if buffSize <= 0 {
		params["buffersize"] = "1024"
	}else{
		params["buffersize"] = strconv.Itoa(buffSize)
	}

	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return nil, err
	}

	rsp, err := makeHttpRequest(fs.client, *u)
	if err != nil {
		return nil, err
	}
	return rsp.Body, nil
}

// Opens an existing file (specified by Path) to append []byte.
// See HDFS FileSystem.append()
func (fs *FileSystem) OpenForAppend(p Path, buffersize int)(Path, error){
	params := map[string]string{"op":OP_APPEND}
	
	if buffersize == 0 {
		params["buffersize"] = "4096"
	}else{
		params["buffersize"] = strconv.FormatInt(int64(buffersize), 10)
	}

	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return Path{}, err
	}

	// take over default transport to avoid redirect
	tr := &http.Transport{}
	req, _ := http.NewRequest("PUT", u.String(), nil)
	rsp, err := tr.RoundTrip(req)

	if err != nil {
		return Path{}, err
	}

	// extract returned url in header.
	loc := rsp.Header.Get("Location")

	if loc == "" {
		return Path{}, fmt.Errorf("OpenForAppend() - did not receive URL for newly created HDFS file.")
	}

	u, err = url.Parse(loc)
	if err != nil {
		return Path{}, fmt.Errorf("OpenForAppend() - did not receive a valid URL from server.")
	}

	return Path{Path:p.Path, RefererUrl:*u}, nil
}

func (fs *FileSystem) Append(data []byte, p Path)(bool, error){
	locStr := p.RefererUrl.String()
	if locStr == "" {
		return false, fmt.Errorf("Append() - Parameter Path missing a URL referer.")
	}

	if _, err := url.Parse(locStr); err != nil {
		return false, err
	}

	rsp, err := http.Post(locStr, "application/octet-stream", bytes.NewBuffer(data))
	if  err != nil  {
		return false, err
	}

	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusCreated {
		return false, fmt.Errorf("File not created.  Server returned status %v", rsp.StatusCode)
	}

	return true, nil
}

// Concatenate (on the server) a list of given files paths to a new file.
// See HDFS FileSystem.concat()
func (fs *FileSystem) Concat(target Path, sources []string)(bool, error) {
	if (target == Path{}) {
		return false, fmt.Errorf("Concat() - The target path must be provided.")
	}
	params := map[string]string{"op":OP_CONCAT}
	params["sources"] = strings.Join (sources, ",")
	
	u, err := buildRequestUrl(fs.Config, &target, &params)
	if err != nil {
		return false, err
	}

	req, _ 	 := http.NewRequest("PUT", u.String(), nil)
	rsp, err := fs.client.Do(req)
	if err != nil {
		return false, err
	}
	if rsp.StatusCode != http.StatusOK && rsp.ContentLength != 0 {
		return false, fmt.Errorf("Concat() - Server returned unexpected result.")
	}
	return true, nil
}