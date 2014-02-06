package gowfs

import "encoding/json"
import "net/http"
import "net/url"
import "io/ioutil"

const (
	OP_OPEN					= "OPEN"
	OP_CREATE 				= "CREATE"
	OP_APPEND				= "APPEND"
	OP_CONCAT				= "CONCAT"
	OP_MKDIRS				= "MKDIRS"
	OP_CREATESYMLINK		= "CREATESYMLINK"
	OP_LISTSTATUS 			= "LISTSTATUS"
	OP_GETFILESTATUS 		= "GETFILESTATUS" 
	OP_GETCONTENTSUMMARY	= "GETCONTENTSUMMARY"
	OP_GETFILECHECKSUM		= "GETFILECHECKSUM"
)

// This type maps fields and functions to HDFS's FileSystem class.
type FileSystem struct {
	Config Configuration
	client http.Client
}

func NewFileSystem(conf Configuration) (*FileSystem, error){
	fs := &FileSystem{
		Config: conf, 
	}
	fs.client = http.Client {}
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
		if (p.Path[0] == '/'){
			u.Path = u.Path + p.Path
		}else{
			u.Path = u.Path + "/" + p.Path
		}
	}

	q := u.Query()

	// attach params
	if params != nil {
		for key, val := range *params {
			q.Add(key, val)
		}
	}
	u.RawQuery=q.Encode()

	return u, nil
}

// Make http requests here
func makeHttpRequest(client http.Client, req url.URL) (http.Response, error){
	rsp, err := client.Get(req.String())
	if err != nil {
		return http.Response{}, err
	}
	return *rsp, nil
}

// returns typed HDFS data
func requestHdfsData(client http.Client, req url.URL) (HdfsJsonData, error) {
	
	rsp, err := makeHttpRequest(client, req)
	if err != nil {
		return HdfsJsonData{}, err
	}

	defer rsp.Body.Close()
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return HdfsJsonData{}, err
	}

	var jsonData HdfsJsonData
	jsonErr := json.Unmarshal(body, &jsonData)

	if jsonErr != nil {
		return HdfsJsonData{}, jsonErr
	}

	// check for remote exception
	if jsonData.RemoteException.Exception != ""{
		return HdfsJsonData{}, jsonData.RemoteException
	}

	return jsonData, nil
}


