package gowfs

import "os"
import "bytes"
import "io"
import "io/ioutil"



const MAX_UP_CHUNK    int64 = 1 * (1024 * 1024) * 1024 // 1 GB.
const MAX_DOWN_CHUNK  int64 = 500 * (1024 * 1024)      // 500 MB

type FsShell struct {
	FileSystem *FileSystem
	WorkingPath string
}

// Appends the specified list of local files to the HDFS path.
func (shell FsShell) AppendToFile (filePaths []string, hdfsPath string) (bool, error) {
 
	for _, path := range filePaths {
		file, err := os.Open(path)
		
		if err != nil {
			return false, err
		}
		defer file.Close()

		data, _ , err := slirpLocalFile(*file, 0)
		if err != nil{
			return false, err
		}

		_, err = shell.FileSystem.Append(bytes.NewBuffer(data), Path{Name:hdfsPath}, 0)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// Returns a writer with the content of the specified files.
func (shell FsShell) Cat (hdfsPaths []string, writr io.Writer) error {
	for _, path := range hdfsPaths {
		stat, err := shell.FileSystem.GetFileStatus(Path{Name:path})
		if err != nil {
			return err
		}
		//TODO add code to chunk super large files.
		if stat.Length < MAX_DOWN_CHUNK {
			readr, err := shell.FileSystem.Open(Path{Name:path}, 0, stat.Length, 4096)
			if err != nil {
				return err
			}
			io.Copy(writr, readr)
		}
	}
	return nil
}

// Changes the group association of the given hsfs paths
// func (shell FsShell) Chgrp (hdfsPaths []string, grpName string) (bool, error) {
	
// }

// TODO: slirp file in x Gbyte chunks when file.Stat() >> X.
//       this is to avoid blow up memory on large files.
func slirpLocalFile(file os.File, offset int64)([]byte, int64, error){
	stat, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}

	if stat.Size() < MAX_UP_CHUNK  {
		data, err := ioutil.ReadFile(file.Name())
		if err != nil {
			return nil, 0, err
		}
		return data, 0, nil
	}// else chunck it

	return nil, 0, nil
}

//TODO: slirp file in X GBytes chucks from server to avoid blowing up network.
// func slirpRemoteFile (hdfsPath string, offset int64, totalSize int64)([]byte, int64, error) {

// }