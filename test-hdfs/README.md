### Integration Test
This directory contains tests to be executed against a running Hadoop File System.

#### Instructions
The following assumes your GOPATH is setup properly.
* Start your local Hadoop File System or make sure to have access to a remote running system.
* Download the gowfs proejct from github.
* On your local system, change your working directory to gowfs/test-hdfs
* curl/download file http://www.gutenberg.org/cache/epub/2600/pg2600.txt
* Save the downloaded file as war-and-peace.txt in the working directory.
* Import gowfs with: go get github.com/vladimirvivien/gowfs
* Build the test files: go build vladimirvivien/gowfs/test-hdfs