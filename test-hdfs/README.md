### Integration Test
This directory contains tests to be executed against a running Hadoop File System.

#### Instructions
The following assumes your GOPATH is setup properly.
* Start your local Hadoop File System or make sure to have access to a remote running system.
* Ensure your HDFS system has `dfs.webhdfs.enabled` property enabled in hsdfs-site.xml
* Import gowfs with: `go get github.com/vladimirvivien/gowfs`
* Don't forget to pull down the actual gowfs project.
* Change your working directory to gowfs/test-hdfs
* curl/download file http://www.gutenberg.org/cache/epub/2600/pg2600.txt
* Save the downloaded file as war-and-peace.txt in the working directory.
* Build using `go build`.  This will create `test-hdfs` binary.

#### HDFS Setup
* Enable `dfs.webhdfs.enabled` property in your hsdfs-site.xml 
* Ensure `hadoop.http.staticuser.user` property is set in your core-site.xml.

#### Run Test
`./test-hdfs -namenode=<your namenode addr> -path=<hdfs path> -user=<your hdfs user>`

#### What Test Does
The `main.go` code will connect to the specified namenode and run several steps including
* Create a test directory in `-path`
* Upload file from above
* Rename the file
* Append to the file
* Change owner, group, and mode.
* Download the file and remove it from HDFS.