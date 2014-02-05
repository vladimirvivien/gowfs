package gowfs

import "fmt"
import "os"
import "io"
import "bytes"
import "strconv"
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
	buffersize uint) (url.URL, error){

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
		return url.URL{}, err
	}

	// take over default transport to avoid redirect
	tr := &http.Transport{}
	req, _ := http.NewRequest("PUT", u.String(), nil)
	rsp, err := tr.RoundTrip(req)

	if err != nil {
		return url.URL{}, err
	}

	// extract returned url in header.
	loc := rsp.Header.Get("Location")

	if loc == "" {
		return url.URL{}, fmt.Errorf("Create() - did not receive URL for newly created HDFS file.")
	}

	u, err = url.Parse(loc)
	if err != nil {
		return url.URL{}, fmt.Errorf("Create() - did not receive a valid URL from server.")
	}

	return *u, nil
}

// This function writes provied buffer to specified URL.
// Call this function after a call to Create()
// For detail, http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
func (fs *FileSystem) WriteFile(data []byte, loc url.URL) (bool, error){
	locStr := loc.String()
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

func (fs *FileSystem) Append(data []byte, p Path, bufferSize int)(bool, error){
	return false, fmt.Errorf("Method Append() unimplemented.")
}

func (fs *FileSystem) Concat(orig Path, paths []Path)(bool, error) {
	return false, fmt.Errorf("Method Concat() unimplemented.") 
}

//Opens the specificed Path for reading.
//See HDFS WebHdfsFileSystem.open()
//NOTE:
//offset - valid values 0 or greater
//length - if 0 (will be set to null), meaning entire file will be returned.
//bufferSize - must be > 0 (if not, will be adjusted)
//
// See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#HTTP_Query_Parameter_Dictionary
func (fs *FileSystem) Open(p Path, offset, length int64, buffSize int) (io.ReadCloser, error){
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