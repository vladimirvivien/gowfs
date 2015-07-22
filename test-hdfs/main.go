package main

import "fmt"
import "flag"
import "log"
import "os"
import "path"
import "os/user"
import "strconv"
import "time"
import "vladimirvivien/gowfs"

var uname string

func init() {
	u, _ := user.Current()
	uname = u.Username
}

func main() {
	var nn = flag.String("namenode", "localhost:50070", "Namenode address")
	var path = flag.String("path", "/user/"+uname, "HDFS file path")
	var username = flag.String("user", uname, "HDFS user")
	var testData = flag.String("testdata", "./war-and-peace.txt", "Local test file to use")
	flag.Parse()

	conf := *gowfs.NewConfiguration()

	conf.Addr = *nn
	conf.User = *username
	fs, err := gowfs.NewFileSystem(conf)
	if err != nil {
		log.Fatal(err)
	}

	testConnection(fs)
	ls(fs, *path)
	testDir := *path + "/test"
	createTestDir(fs, testDir)
	remoteFile := uploadTestFile(fs, *testData, testDir)
	appendToRemoteFile(fs, *testData, remoteFile)
	newRemoteFile := testDir + "/" + "peace-and-war.txt"
	renameRemoteFile(fs, remoteFile, newRemoteFile)
	changeOwner(fs, newRemoteFile)
	changeGroup(fs, newRemoteFile)
	changeMod(fs, newRemoteFile)
	moveRemoteFileLocal(fs, newRemoteFile)
}

func testConnection(fs *gowfs.FileSystem) {
	_, err := fs.ListStatus(gowfs.Path{Name: "/"})
	if err != nil {
		log.Fatal("Unable to connect to server. ", err)
	}
	log.Printf("Connected to server %s... OK.\n", fs.Config.Addr)
}

func ls(fs *gowfs.FileSystem, hdfsPath string) {
	stats, err := fs.ListStatus(gowfs.Path{Name: hdfsPath})
	if err != nil {
		log.Fatal("Unable to list paths: ", err)
	}
	log.Printf("Found %d file(s) at %s\n", len(stats), hdfsPath)
	for _, stat := range stats {
		fmt.Printf(
			"%-11s %3s %s\t%s\t%11d %20v %s\n",
			formatFileMode(stat.Permission, stat.Type),
			formatReplication(stat.Replication, stat.Type),
			stat.Owner,
			stat.Group,
			stat.Length,
			formatModTime(stat.ModificationTime),
			stat.PathSuffix)
	}
}

func formatFileMode(webfsPerm string, fileType string) string {
	perm, _ := strconv.ParseInt(webfsPerm, 8, 16)
	fm := os.FileMode(perm)
	if fileType == "DIRECTORY" {
		fm = fm | os.ModeDir
	}
	return fm.String()
}

func formatReplication(rep int64, fileType string) string {
	repStr := strconv.FormatInt(rep, 8)
	if fileType == "DIRECTORY" {
		repStr = "-"
	}
	return repStr
}

func formatModTime(modTime int64) string {
	modTimeAdj := time.Unix((modTime / 1000), 0) // adjusted for Java Calendar in millis.
	return modTimeAdj.Format("2006-01-02 15:04:05")
}

func createTestDir(fs *gowfs.FileSystem, hdfsPath string) {
	path := gowfs.Path{Name: hdfsPath}
	ok, err := fs.MkDirs(path, 0744)
	if err != nil || !ok {
		log.Fatal("Unable to create test directory ", hdfsPath, ":", err)
	}
	log.Println("HDFS Path ", path.Name, " created.")
	ls(fs, path.Name)
}

func uploadTestFile(fs *gowfs.FileSystem, testFile, hdfsPath string) string {
	file, err := os.Open(testFile)
	if err != nil {
		log.Fatal("Unable to find local test file: ", err)
	}
	stat, _ := file.Stat()
	if stat.Mode().IsDir() {
		log.Fatal("Data file expected, directory found.")
	}
	log.Println("Test file ", stat.Name(), " found.")

	shell := gowfs.FsShell{FileSystem: fs}
	log.Println("Sending file ", file.Name(), " to HDFS location ", hdfsPath)
	ok, err := shell.Put(file.Name(), hdfsPath, true)
	if err != nil || !ok {
		log.Fatal("Failed during test file upload: ", err)
	}
	_, fileName := path.Split(file.Name())
	log.Println("File ", fileName, " Copied OK.")
	remoteFile := hdfsPath + "/" + fileName
	ls(fs, remoteFile)

	return remoteFile
}

func renameRemoteFile(fs *gowfs.FileSystem, oldName, newName string) {
	_, err := fs.Rename(gowfs.Path{Name: oldName}, gowfs.Path{Name: newName})
	if err != nil {
		log.Fatal("Unable to rename remote file ", oldName, " to ", newName)
	}
	log.Println("HDFS file ", oldName, " renamed to ", newName)
	ls(fs, newName)
}

func changeOwner(fs *gowfs.FileSystem, hdfsPath string) {
	shell := gowfs.FsShell{FileSystem: fs}
	_, err := shell.Chown([]string{hdfsPath}, "owner2")
	if err != nil {
		log.Fatal("Chown failed for ", hdfsPath, ": ", err.Error())
	}
	stat, err := fs.GetFileStatus(gowfs.Path{Name: hdfsPath})
	if err != nil {
		log.Fatal("Unable to validate chown() operation: ", err.Error())
	}
	if stat.Owner == "owner2" {
		log.Println("Chown for ", hdfsPath, " OK ")
		ls(fs, hdfsPath)
	} else {
		log.Fatal("Chown() failed.")
	}
}

func changeGroup(fs *gowfs.FileSystem, hdfsPath string) {
	shell := gowfs.FsShell{FileSystem: fs}
	_, err := shell.Chgrp([]string{hdfsPath}, "superduper")
	if err != nil {
		log.Fatal("Chgrp failed for ", hdfsPath, ": ", err.Error())
	}
	stat, err := fs.GetFileStatus(gowfs.Path{Name: hdfsPath})
	if err != nil {
		log.Fatal("Unable to validate chgrp() operation: ", err.Error())
	}
	if stat.Group == "superduper" {
		log.Println("Chgrp for ", hdfsPath, " OK ")
		ls(fs, hdfsPath)
	} else {
		log.Fatal("Chgrp() failed.")
	}
}

func changeMod(fs *gowfs.FileSystem, hdfsPath string) {
	shell := gowfs.FsShell{FileSystem: fs}
	_, err := shell.Chmod([]string{hdfsPath}, 0744)
	if err != nil {
		log.Fatal("Chmod() failed for ", hdfsPath, ": ", err.Error())
	}
	stat, err := fs.GetFileStatus(gowfs.Path{Name: hdfsPath})
	if err != nil {
		log.Fatal("Unable to validate Chmod() operation: ", err.Error())
	}
	if stat.Permission == "744" {
		log.Println("Chmod for ", hdfsPath, " OK ")
		ls(fs, hdfsPath)
	} else {
		log.Fatal("Chmod() failed.")
	}
}

func appendToRemoteFile(fs *gowfs.FileSystem, localFile, hdfsPath string) {
	stat, err := fs.GetFileStatus(gowfs.Path{Name: hdfsPath})
	if err != nil {
		log.Fatal("Unable to get file info for ", hdfsPath, ":", err.Error())
	}
	shell := gowfs.FsShell{FileSystem: fs}
	_, err = shell.AppendToFile([]string{localFile}, hdfsPath)
	if err != nil {
		log.Fatal("AppendToFile() failed: ", err.Error())
	}

	stat2, err := fs.GetFileStatus(gowfs.Path{Name: hdfsPath})
	if err != nil {
		log.Fatal("Something went wrong, unable to get file info:", err.Error())
	}
	if stat2.Length > stat.Length {
		log.Println("AppendToFile() for ", hdfsPath, " OK.")
		ls(fs, hdfsPath)
	} else {
		log.Fatal("AppendToFile failed. File size for ", hdfsPath, " expected to be larger.")
	}
}

func moveRemoteFileLocal(fs *gowfs.FileSystem, remoteFile string) {
	log.Println("Moving Remote file!!")
	shell := gowfs.FsShell{FileSystem: fs}
	remotePath, fileName := path.Split(remoteFile)
	_, err := shell.MoveToLocal(remoteFile, fileName)
	if err != nil {
		log.Fatal("MoveToLocal() failed: ", err.Error())
	}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("MoveToLocal() - local file can't be open. ")
	}
	defer file.Close()
	defer os.Remove(file.Name())

	_, err = fs.GetFileStatus(gowfs.Path{Name: remoteFile})
	if err == nil {
		log.Fatal("Expecing a FileNotFoundException, but file is found. ", remoteFile, ": ", err.Error())
	}
	log.Printf("Remote file %s has been removed Ok", remoteFile)
	ls(fs, remotePath)
}
