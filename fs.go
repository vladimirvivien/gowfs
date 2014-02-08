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
	OP_RENAME				= "RENAME"
	OP_DELETE				= "DELETE"
	OP_SETPERMISSION		= "SETPERMISSION"
	OP_SETOWNER				= "SETOWNER"
	OP_SETREPLICATION		= "SETREPLICATION"
	OP_SETTIMES				= "SETTIMES"
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
		if (p.Name[0] == '/'){
			u.Path = u.Path + p.Name
		}else{
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
	u.RawQuery=q.Encode()

	return u, nil
}

func makeHdfsData(data []byte)(HdfsJsonData, error) {
	var jsonData HdfsJsonData
	jsonErr := json.Unmarshal(data, &jsonData)

	if jsonErr != nil {
		return HdfsJsonData{}, jsonErr
	}

	// check for remote exception
	if jsonData.RemoteException.Exception != ""{
		return HdfsJsonData{}, jsonData.RemoteException
	}

	return jsonData, nil

}

func requestHdfsData(client http.Client, req http.Request) (HdfsJsonData, error){
	rsp, err := client.Do(&req)
	if err != nil {
		return HdfsJsonData{}, err
	}

	defer rsp.Body.Close()
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return HdfsJsonData{}, err
	}
	return makeHdfsData(body)
}


