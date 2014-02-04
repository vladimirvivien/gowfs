package gowfs

import "fmt"
import "os"
import "io"
import "strconv"
import "net/url"


func (fs *FileSystem) Create(p Path, overwrite bool, blocksize int64, replication int16, perm os.FileMode, bufferSize int) (url.URL, error){
	return url.URL{}, fmt.Errorf("Method Create() unimplemented.")
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
		params["bufferSize"] = "1024"
	}else{
		params["bufferSize"] = strconv.Itoa(buffSize)
	}

	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return nil, err
	}

	return requestRawHttp(fs.client, *u)
}