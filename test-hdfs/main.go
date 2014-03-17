package main

import "fmt"
import "flag"
import "log"
import "os"
import "path"
import "os/user"
import "vladimirvivien/gowfs"

var uname string

func init() {
	u,_ := user.Current()
	uname = u.Username
}

func main() {
	var nn   	 = flag.String("namenode", "localhost:50070", "Namenode address")
	var path 	 = flag.String("path", 	"/user/"+uname, "HDFS file path")
	var username = flag.String("user", uname, "HDFS user")
	var testData= flag.String("testdata", "./war-and-peace.txt", "Local test file to use")
	flag.Parse()

	conf := *gowfs.NewConfiguration()

	conf.Addr = *nn
	conf.User = *username
	fs, err := gowfs.NewFileSystem(conf)
	if err != nil{
		log.Fatal(err)
	}

	testConnection(fs)
	listStats(fs, *path)
	testDir := *path + "/test"
	createTestDir(fs, testDir)
	remoteFile := uploadTestFile(fs, *testData, testDir)
	newRemoteFile :=testDir +"/"+ "peace-and-war.txt"
	renameRemoteFile(fs, remoteFile, newRemoteFile)
}

func testConnection (fs *gowfs.FileSystem) {
	_, err := fs.ListStatus(gowfs.Path{Name:"/"})
	if err != nil {
		log.Fatal("Unable to connect to server. ", err)
	}
	log.Printf("Connected to server %s... OK.\n", fs.Config.Addr)
}

func listStats(fs *gowfs.FileSystem, hdfsPath string) {
	stats, err := fs.ListStatus(gowfs.Path{Name:hdfsPath})
	if err != nil {
		log.Fatal("Unable to list paths: ", err)
	}
	log.Printf("Found %d file(s) at %s\n", len(stats), hdfsPath)
	for _, stat := range stats {
		fmt.Printf ("%s%11d\t%s\n", stat.Type, stat.Length, stat.PathSuffix)
	}
}

func createTestDir(fs *gowfs.FileSystem, hdfsPath string) {
	path := gowfs.Path{Name:hdfsPath}
	ok, err := fs.MkDirs(path, 0744)
	if err != nil || !ok {
		log.Fatal("Unable to create test directory ", hdfsPath, ":", err)
	}
	log.Println ("HDFS Path ", path.Name, " created.")
	listStats(fs, path.Name)
}

func uploadTestFile(fs *gowfs.FileSystem, testFile, hdfsPath string) string {
	file, err := os.Open(testFile)
	if err != nil  {
		log.Fatal ("Unable to find local test file: ", err)
	}
	stat, _ := file.Stat()
	if stat.Mode().IsDir() {
		log.Fatal ("Data file expected, directory found.")
	}
	log.Println("Test file ", stat.Name(), " found.")

	shell := gowfs.FsShell{FileSystem:fs}
	log.Println("Sending file ", file.Name(), " to HDFS location ", hdfsPath)
	ok, err := shell.PutOne(file.Name(), hdfsPath, true)
	if err != nil || !ok {
		log.Fatal ("Failed during test file upload: ", err)
	}
	_, fileName := path.Split(file.Name())
	log.Println ("File ", fileName, " Copied OK.")
	remoteFile := hdfsPath + "/" + fileName
	listStats(fs, remoteFile)

	return remoteFile
}

func renameRemoteFile(fs *gowfs.FileSystem, oldName, newName string) {
	_, err := fs.Rename(gowfs.Path{Name:oldName}, gowfs.Path{Name:newName})
	if err != nil  {
		log.Fatal("Unable to rename remote file ", oldName, " to ", newName)
	}
	log.Println ("HDFS file ", oldName, " renamed to ", newName)
	listStats(fs, newName)
}

