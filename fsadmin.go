package gowfs

import "fmt"
import "os"
import "strconv"
// Renames the specified path resource to a new name.
// See HDFS FileSystem.rename()
func (fs *FileSystem) Rename(origName Path, newName Path) (bool, error) {
	return false, fmt.Errorf("Method Rename() unimplemented.")
}

func (fs *FileSystem) Delete(p Path, recursive bool) (bool, error){
	return false, fmt.Errorf("Method Delete() unimplemented.")
}

func (fs *FileSystem) SetPermission(p Path, fm os.FileMode) (bool, error){
	return false, fmt.Errorf("Method SetPermission() unimplemented.")
}

func (fs *FileSystem) SetOwner(p Path, owner string, group string) (bool, error){
	return false, fmt.Errorf("Method SetOwner() unimplemented.")
}

func (fs *FileSystem) SetReplication(rep uint16)(bool, error){
	return false, fmt.Errorf("Method SetReplication() unimplemented.")
}

func (fs *FileSystem) SetTimes(p Path, accessTime int64, modTime int64)(bool, error){
	return false, fmt.Errorf("Method SetTimes() unimplemented.")
}

// Creates the specified directory(ies).
// See HDFS FileSystem.mkdirs()
func (fs *FileSystem) MkDirs(p Path, fm os.FileMode) (bool, error) {
	params := map[string]string{"op":OP_MKDIRS}

	if fm <= 0 || fm > 1777{
		return false, fmt.Errorf("MkDirs - Permission value [%v] out of range.", fm)
	}
	// convert to an octal
	params["permission"] = strconv.FormatInt(int64(fm), 8)
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return false, err
	}

 	hdfsData, err := requestHdfsData(fs.client, *u)
	if err != nil {
		return false, err
	}

	return hdfsData.Boolean, nil
}

// Creates a symlink where link -> destination
// See HDFS FileSystem.createSymlink()
// dest - the full path of the original resource 
// link - the symlink path to create
// createParent - when true, parent dirs are created if they don't exist
// See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#HTTP_Query_Parameter_Dictionary
func (fs *FileSystem) CreateSymlink(dest Path, link Path, createParent bool) (bool, error) {
	params := map[string]string{"op":OP_CREATESYMLINK}

	if dest.Path == "" || link.Path == "" {
		return false, fmt.Errorf("CreateSymlink - param dest and link cannot be empty.")
	}

	params["destination"] 	= dest.Path
	params["createParent"]	= strconv.FormatBool(createParent)
	u, err := buildRequestUrl(fs.Config, &link, &params)
	if err != nil {
		return false, err
	}

	rsp, err := requestRawHttp(fs.client, *u)
	defer rsp.Close()
	
	if err != nil  {
		return false, err
	}

	return true, nil
}


// Returns status for a given file.  The Path must represent a FILE
// on the remote system. (see HDFS FileSystem.getFileStatus())
func (fs *FileSystem) GetFileStatus(p Path) (FileStatus, error) {
	params := map[string]string{"op":OP_GETFILESTATUS}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return FileStatus{}, err
	}

 	hdfsData, err := requestHdfsData(fs.client, *u)
	if err != nil {
		return FileStatus{}, err
	}

	return hdfsData.FileStatus, nil
}

// Returns an array of FileStatus for a given file directory.
// For details, see HDFS FileSystem.listStatus()
func (fs *FileSystem) ListStatus(p Path) ([]FileStatus, error) {

	params := map[string]string{"op":OP_LISTSTATUS}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return nil, err
	}

	hdfsData, err := requestHdfsData(fs.client, *u)
	if err != nil {
		return nil, err
	}

	return hdfsData.FileStatuses.FileStatus, nil
}

//Returns ContentSummary for the given path.
//For detail, see HDFS FileSystem.getContentSummary()
func (fs *FileSystem) GetContentSummary(p Path) (ContentSummary, error) {
	params := map[string]string{"op":OP_GETCONTENTSUMMARY}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return ContentSummary{}, err
	}

	hdfsData, err := requestHdfsData(fs.client, *u)
	if err != nil {
		return ContentSummary{}, err
	}

	return hdfsData.ContentSummary, nil
}

func (fs *FileSystem) GetHomeDirectory() (Path, error) {
	return Path{}, fmt.Errorf("Method GetHomeDirectory(), not implemented yet.")
}

// Returns HDFS file checksum.
// For detail, see HDFS FileSystem.getFileChecksum()
func (fs *FileSystem) GetFileChecksum(p Path) (FileChecksum, error) {
	params := map[string]string{"op":OP_GETFILECHECKSUM}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return FileChecksum{}, err
	}
	hdfsData, err := requestHdfsData(fs.client, *u)
	if err != nil {
		return FileChecksum{}, err
	}
	return hdfsData.FileChecksum, nil
}