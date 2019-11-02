# Google_File_System_Implementation

The following work was done to understand and implement Google File System based on the paper released by Google. A list of references can be found at the end of this document.

### Introduction
Google File system is a distributed file system that uses a lot of inexpensive (off the shelf) machines. However due to the unability to personally scale to such an extent, the implementation was done on a single machine (differnt chunk-servers were simulated). Since the file system is proprietory, the information provided by the paper [1], was used to attempt to implement the system. The implementation was done in Python. Some of the major operations that were included were:
1. Create File
2. Read
3. Delete
4. Append
5. Snapshot

The details of the operations (as well as other operations) are given in the file "GFS.pdf" which is a presentation briefly describing the project. As described in the paper, the system uses a master as the main node with multiple chunk-servers that stores the chunks (files are split into chunks).

### Repository structure
A brief description of the repository structure is as follows:
- **app**: This directory contains the code to create the master, client and the chunkservers
- **test**: This directory consistes of code that was used to test the implementation
- **GFS.pdf**: This file contains the details of the project (in brief).

References:
1. Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung, "The Google File System", Google, 2003, https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf
