package surfstore

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
)

var filenames_list []string
var local_fileMetaMap map[string](*FileMetaData)

// key: filename, element: map[hashvalue] = *Block
var local_fileHashBlock map[string](map[string]*Block)
var index_fileMetaDataMap map[string]*FileMetaData
var remote_index_fileInfoMap map[string]*FileMetaData

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")

	if local_fileMetaMap == nil {
		local_fileMetaMap = make(map[string]*FileMetaData)
	}
	if local_fileHashBlock == nil {
		local_fileHashBlock = make(map[string]map[string]*Block)
	}
	if index_fileMetaDataMap == nil {
		index_fileMetaDataMap = make(map[string]*FileMetaData)
	}
	if remote_index_fileInfoMap == nil {
		remote_index_fileInfoMap = make(map[string]*FileMetaData)
	}

	// log.Println("======Client Sync=====")
	// ===== get local index.txt
	// --check if index.txt exists
	index_filepath := ConcatPath(client.BaseDir, "index.txt")
	// index_filepath := "index.txt"
	_, err := os.Stat(index_filepath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("[ClientSync] index.txt does not exist. Creating one now")

			// create index.txt (in this case, base directory should have no files)
			// --creating an empty index.txt
			empty_f, err := os.Create(index_filepath)
			if err != nil {
				log.Println("[ClientSync] creating empty index.txt error.")
				log.Fatal(err)
			}
			empty_f.Close()

		} else {
			log.Println("[ClientSync] os.Stat() error.")
			log.Fatal(err)
		}
	}
	// --read index.txt
	index_f, err := os.Open(index_filepath)
	if err != nil {
		log.Println("[ClientSync] Failed to open")
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(index_f)
	scanner.Split(bufio.ScanLines)
	var text []string
	for scanner.Scan() {
		text = append(text, scanner.Text())
	}
	index_f.Close()
	// var index_fileMetaDataMap map[string]*FileMetaData
	for _, each_line := range text {
		// var f_meta_data *FileMetaData
		f_meta_data := NewFileMetaDataFromConfig(each_line)
		temp_filename := f_meta_data.Filename
		index_fileMetaDataMap[temp_filename] = f_meta_data
	}
	// ======= get real local file info
	// --read through base directory
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("[ClientSync] Couldn't read the base directory.")
		log.Fatal(err)
	}
	// var filenames_list []string
	// var local_fileMetaMap map[string](*FileMetaData)
	// // key: filename, element: map[hashvalue] = *Block
	// var local_fileHashBlock map[string](map[string]*Block)
	// --read through each file in base directory

	for _, file := range files {
		if file.IsDir() == false && file.Name() != "index.txt" {
			filenames_list = append(filenames_list, file.Name())
			// log.Println(file.Name())

			fp := ConcatPath(client.BaseDir, file.Name())
			f, err := os.Open(fp)
			if err != nil {
				fmt.Println("[ClientSync] Error opening file.")
			}
			defer f.Close()

			local_hashList := make([]string, 0)
			var block_s = client.BlockSize
			// --loop through each block data
			// -- and generate hash list for this current file
			for {
				b := make([]byte, block_s)
				readt, err := f.Read(b)
				if err != nil {
					// log.Println("[ClientSync]", err)
					// log.Println("---finished reading file")
					break
				}
				// get hash value for this block data
				hash_val := GetBlockHashString(b[:readt])

				// if local_fileHashBlock == nil {
				// 	local_fileHashBlock = make(map[string]map[string]*Block)
				// }
				if local_fileHashBlock[file.Name()] == nil {
					local_fileHashBlock[file.Name()] = make(map[string]*Block)
				}
				if local_fileHashBlock[file.Name()][hash_val] == nil {
					local_fileHashBlock[file.Name()][hash_val] = new(Block)
				}
				local_fileHashBlock[file.Name()][hash_val].BlockData = b[:readt]
				local_fileHashBlock[file.Name()][hash_val].BlockSize = int32(readt)
				local_hashList = append(local_hashList, hash_val)
			}

			// if local_fileMetaMap == nil {
			// 	local_fileMetaMap = make(map[string]*FileMetaData)
			// }
			if local_fileMetaMap[file.Name()] == nil {
				local_fileMetaMap[file.Name()] = new(FileMetaData)
			}
			local_fileMetaMap[file.Name()].Filename = file.Name()
			local_fileMetaMap[file.Name()].BlockHashList = local_hashList
			// check if filename is in local index
			if f_metaD, ok := index_fileMetaDataMap[file.Name()]; ok {
				local_fileMetaMap[file.Name()].Version = f_metaD.Version
			} else {
				local_fileMetaMap[file.Name()].Version = 1
			}

		}
	}
	// log.Println("getting remote index")
	// ======== Get remote index (updated FileInfoMap)
	// var remote_index_fileInfoMap map[string]*FileMetaData
	if err := client.GetFileInfoMap(&remote_index_fileInfoMap); err != nil {
		log.Println("[client-sync] get remote index error.")
		log.Fatal(err)
	}

	// log.Println("<<< remote index >>>")
	// if len(remote_index_fileInfoMap) == 0 {
	// 	log.Println("==> Nothing in remote index map")
	// }

	// for _, v := range remote_index_fileInfoMap {
	// 	log.Println("--------Filename:", v.Filename)
	// 	log.Println("Version:", v.Version)
	// 	// log.Println("HashList:", v.BlockHashList)
	// }
	// log.Println("")

	// =========COMPARE==========
	// now, compare base directory, local index, and remote index
	// log.Println("(looping base directory)")
	// first, loop through filenames in base directory
	//   check for 4 situations
	for fn, base_fileMetaData := range local_fileMetaMap {
		// check if in local index
		if f_local_metaData, ok := index_fileMetaDataMap[fn]; ok {
			// check if in remote index (YYY)
			if f_remote_metaData, okk := remote_index_fileInfoMap[fn]; okk {
				// check if modified (if base dir's hashlist != local index's hashlist)
				if !reflect.DeepEqual(base_fileMetaData.BlockHashList, f_local_metaData.BlockHashList) {
					// if modified, then base's version = local's version + 1
					(local_fileMetaMap[fn]).Version = f_local_metaData.Version + 1
					// check if base's version == remote's version + 1
					if local_fileMetaMap[fn].Version == (f_remote_metaData.Version + 1) {
						// the current file is lastest version, upload to remote
						upload_file(client, fn, "modified")
					} else { // download from remote to base & update local index
						// log.Println("[Client-Sync] version not match")
						download_file(client, fn)
					}

				} else {
					// download from remote to base & update local index
					// log.Println("downloading from remote")
					download_file(client, fn)
				}
			} else { // (YYN)
				// delete the file on base & update local index's hashlist to be ["0"]
				var delete_hashList []string
				delete_hashList = append(delete_hashList, "0")
				local_fileMetaMap[fn].Version = local_fileMetaMap[fn].Version + 1
				local_fileMetaMap[fn].BlockHashList = delete_hashList

				// actual deletion and update on local index will be preform at the end
			}
		} else {
			// check if in remote index (YNY)
			if _, okk := remote_index_fileInfoMap[fn]; okk {
				// download from remote to base & update local index
				download_file(client, fn)

			} else { // (YNN)
				// upload created file to remote & update local index

				// if local_fileMetaMap[fn].Version == 1 {
				// 	log.Println("[client-sync] [YNN] yes, the version of the file is 1")
				// }
				upload_file(client, fn, "created")

			}
		}
	}

	// log.Println("(looping local index)")
	// loop through filenames in local index
	for fn := range index_fileMetaDataMap {
		// check not in base directory
		if _, ok := local_fileMetaMap[fn]; !ok {

			// check if in remote index (NYY)
			if _, ok := remote_index_fileInfoMap[fn]; ok {
				// var delete_hashList1 []string
				// delete_hashList1 = append(delete_hashList1, "0")
				var deleted_file_version int32
				// if the file does not exist in local directory && marked as deleted in remote server
				// if this file is already deleted from previous action --> check local index
				if len(index_fileMetaDataMap[fn].BlockHashList) == 1 && index_fileMetaDataMap[fn].BlockHashList[0] == "0" {
					// if the file is already deleted in local index
					// nothing changes
					deleted_file_version = index_fileMetaDataMap[fn].Version
				} else {
					// since file is not in base directory, --> local index's version + 1
					deleted_file_version = index_fileMetaDataMap[fn].Version + 1
				}

				// check if base's version == remote's version + 1
				if deleted_file_version == remote_index_fileInfoMap[fn].Version+1 {
					// if so, deleted file in base directory is the lastest version
					// update remote to delete this file & update local index's hashlist
					// to be ["0"]
					var delete_hashList []string
					delete_hashList = append(delete_hashList, "0")

					if local_fileMetaMap[fn] == nil {
						local_fileMetaMap[fn] = new(FileMetaData)
					}
					local_fileMetaMap[fn].Filename = fn
					local_fileMetaMap[fn].Version = index_fileMetaDataMap[fn].Version + 1
					local_fileMetaMap[fn].BlockHashList = delete_hashList

					// log.Println("NYY -- version matches -> upload")
					upload_file(client, fn, "deleted")
				} else {
					// if not, another client sync before
					// download from remote to base & update local index
					// log.Println("NYY -- version doesn't match -> download")
					download_file(client, fn)
					// whether or not the remote file is modified or deleted
					//   it will be taken care in function download_file()
				}

			} else { // not in remote index (NYN)

				// we won't have this situation cuz if local index Y then remote index
				//   must have corresponding filename
				//   so, we can see it as NYY where we can find filename in remote index
				//	 while the hashlist is ["0"]
				continue
			}
		}
	}

	// log.Println("(looping remote index)")
	// loop through filenames in remote index
	for fn := range remote_index_fileInfoMap {
		// if not in base directory
		if _, ok := local_fileMetaMap[fn]; !ok {
			// if not in local index
			if _, ok := index_fileMetaDataMap[fn]; !ok {
				// (NNY)
				// download from remote to base & update local index
				download_file(client, fn)
			}
		}
	}

	// log.Println("(Updating local index here.)")
	// =========Update local index==========
	if err := WriteMetaFile(local_fileMetaMap, client.BaseDir); err != nil {
		log.Println("[client-sync] WriteMetaFile error.")
		log.Fatal(err)
	}

	// =========Update files in base directory========
	// 	 1. delete all files in base directory
	//   2. and then download needed files in base directory
	//    note: use hashlist in updated base filemetadatamap
	//         if hashlist is ["0"], then delete the file

	// delete all files in base directory

	// log.Println("(deleting all files here.)")
	for _, fn := range filenames_list {
		f_p := ConcatPath(client.BaseDir, fn)
		if err := os.Remove(f_p); err != nil {
			log.Println("[client-sync] Remove error")
			return
		}
	}

	// log.Println("(Creating needed files here.)")
	// create files in local_fileMetaMap
	//   except its hashlist is ["0"]
	for fn, f_metaData := range local_fileMetaMap {
		// check if the file is deleted
		if len(f_metaData.BlockHashList) == 1 && f_metaData.BlockHashList[0] == "0" {
			continue
		} else {
			// create the file with its block data
			desired_hashList := f_metaData.BlockHashList
			f_path := ConcatPath(client.BaseDir, fn)
			created_f, err := os.Create(f_path)
			if err != nil {
				log.Println("[client-sync] Create file error")
				return
			}

			for _, h := range desired_hashList {
				_, err := created_f.Write(local_fileHashBlock[fn][h].BlockData)
				if err != nil {
					log.Println("[client-sync] write file error")
					created_f.Close()
					return
				}
			}
			err = created_f.Close()
			if err != nil {
				log.Println("[client-sync] close file error")
				return
			}
		}
	}
}

// download from remote to base & update local index
func download_file(client RPCClient, filename string) {
	// (update local index) assign remote metadata to base metadata
	// log.Println(local_fileMetaMap[filename])
	if local_fileMetaMap[filename] == nil {
		local_fileMetaMap[filename] = new(FileMetaData)
	}

	// log.Println("remote version:", remote_index_fileInfoMap[filename].Version)
	(local_fileMetaMap[filename]).Filename = remote_index_fileInfoMap[filename].Filename
	(local_fileMetaMap[filename]).Version = remote_index_fileInfoMap[filename].Version
	(local_fileMetaMap[filename]).BlockHashList = remote_index_fileInfoMap[filename].BlockHashList

	// if the remote file is recorded as "deleted"
	//   then no need to getBlock
	if !(len((local_fileMetaMap[filename]).BlockHashList) == 1 && (local_fileMetaMap[filename]).BlockHashList[0] == "0") {
		// use GetBlock to get remote block data
		var remote_blockAddr string
		if err := client.GetBlockStoreAddr(&remote_blockAddr); err != nil {
			log.Println("[client-sync][download_file] GetBlockStoreAddr error.")
			log.Fatal(err)
		}
		// loop through block hash list to get each block
		for _, h := range remote_index_fileInfoMap[filename].BlockHashList {
			var remote_block Block
			if err := client.GetBlock(h, remote_blockAddr, &remote_block); err != nil {
				log.Println("[client-sync][download_file] GetBlock error.")
				log.Fatal(err)
			}

			if local_fileHashBlock[filename] == nil {
				local_fileHashBlock[filename] = make(map[string]*Block)
			}
			if local_fileHashBlock[filename][h] == nil {
				local_fileHashBlock[filename][h] = new(Block)
			}
			local_fileHashBlock[filename][h].BlockData = remote_block.BlockData
			local_fileHashBlock[filename][h].BlockSize = remote_block.BlockSize
		}
	}

}

// upload created or deleted file
// should first putblock then updatefile [should check version before calling if needed]
func upload_file(client RPCClient, filename string, type_of_file string) {
	// 1. putblock

	// if the file to be updated to remote is deleted by client,
	//   then no need to putblock, just updatefile
	if type_of_file != "deleted" {
		// --use putBlock to upload each block data to remote
		//   (use desired hashlist)
		var remote_blockAddr string
		if err := client.GetBlockStoreAddr(&remote_blockAddr); err != nil {
			log.Println("[client-sync][upload_file] GetBlockStoreAddr error.")
			log.Fatal(err)
		}
		desired_hashList := (local_fileMetaMap[filename]).BlockHashList
		for _, h := range desired_hashList {
			block_for_h := local_fileHashBlock[filename][h]
			var succ bool
			if err := client.PutBlock(block_for_h, remote_blockAddr, &succ); err != nil {
				log.Println("[client-sync][upload_file] PutBlock error.")
				log.Fatal(err)
			}
			if (succ) != true {
				// not sure about here
				log.Println("[client-sync][upload_file] putblock not successful")
			}
			// what to do with succ????????
		}
		// we will update files in base to local index together at the end
	}

	// 2. updatefile
	var output_version int32

	if err := client.UpdateFile(local_fileMetaMap[filename], &output_version); err != nil {
		log.Println("[client-sync][upload_file] update file error")
		log.Fatal(err)
	}
	if output_version == -1 {
		log.Println("[client-sync][upload_file] version -1")
		// the base file is not the lastest version
		// update the remote index
		if err := client.GetFileInfoMap(&remote_index_fileInfoMap); err != nil {
			log.Println("[client-sync] get 2nd remote index error.")
			log.Fatal(err)
		}
		download_file(client, filename)
	}
	// if fail, download from remote to base & update local index
	// if output_version == -1 {
	// 	if type_of_file == "created" || type_of_file == "deleted" {
	// 		log.Println("[Client-Sync][upload_file] ERROR: UpdateFile with " + type_of_file + " file should not return version -1")
	// 	} else { // "modified"
	// 		log.Println("[client-sync][upload_file] updating modified file but return version -1")
	// 	}
	// } else {
	// 	log.Println("[client-sync][upload_file] updateFile success")
	// }
}
