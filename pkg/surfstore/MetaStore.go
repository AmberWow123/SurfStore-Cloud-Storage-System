package surfstore

import (
	context "context"
	"log"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

var mu sync.Mutex

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	mu.Lock()
	// defer mu.Unlock()

	var return_map map[string]*FileMetaData

	if return_map == nil {
		return_map = make(map[string]*FileMetaData)
	}

	for k, v := range m.FileMetaMap {
		return_map[k] = v
	}

	mu.Unlock()

	return &FileInfoMap{
		FileInfoMap: return_map,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	mu.Lock()
	// defer mu.Unlock()

	input_filename := fileMetaData.Filename
	input_version := fileMetaData.Version
	input_hashList := fileMetaData.BlockHashList
	var delete_hashList []string
	delete_hashList = append(delete_hashList, "0")

	// check if filename inside the map
	if fMetaData, ok := m.FileMetaMap[input_filename]; ok {

		if m.FileMetaMap[input_filename] == nil {
			m.FileMetaMap[input_filename] = new(FileMetaData)
		}

		// if the inputfile is the lastest version
		if input_version == (fMetaData.Version + 1) {

			// if the input file is deleted, update remote fileMetaDataMap
			if len(input_hashList) == 1 && input_hashList[0] == "0" {
				m.FileMetaMap[input_filename].Version = m.FileMetaMap[input_filename].Version + 1
				m.FileMetaMap[input_filename].BlockHashList = delete_hashList
				mu.Unlock()
				return &Version{
					Version: m.FileMetaMap[input_filename].Version,
				}, nil

			} else if len(fMetaData.BlockHashList) == 1 && fMetaData.BlockHashList[0] == "0" {
				// if the file is previously deleted
				m.FileMetaMap[input_filename].Version = m.FileMetaMap[input_filename].Version + 1
				m.FileMetaMap[input_filename].BlockHashList = input_hashList
				mu.Unlock()
				return &Version{
					Version: m.FileMetaMap[input_filename].Version,
				}, nil
			} else {
				// if the file exists and is not deleted and the version matches
				m.FileMetaMap[input_filename].Version = input_version
				m.FileMetaMap[input_filename].BlockHashList = input_hashList
				mu.Unlock()
				return &Version{
					Version: m.FileMetaMap[input_filename].Version,
				}, nil
			}

		} else { // version doesn't match
			// return version -1
			log.Println("[MetaStore - UpdateFile] version doesn't match")
			mu.Unlock()
			return &Version{
				Version: -1,
			}, nil
		}
	}
	// log.Println("the file is non-existed")
	// if the file is non-existed --> input file is newly created
	var new_fileMetaData *FileMetaData
	if new_fileMetaData == nil {
		new_fileMetaData = new(FileMetaData)
	}
	new_fileMetaData.Filename = input_filename
	new_fileMetaData.Version = 1
	new_fileMetaData.BlockHashList = input_hashList

	m.FileMetaMap[input_filename] = new_fileMetaData
	mu.Unlock()
	return &Version{
		Version: m.FileMetaMap[input_filename].Version,
	}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	// panic("todo")
	mu.Lock()
	// defer mu.Unlock()
	mu.Unlock()
	return &BlockStoreAddr{
		Addr: m.BlockStoreAddr,
	}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
