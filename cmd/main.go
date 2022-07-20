package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/Wangsu-Cloud-Storage/wcs-go-sdk/src/lib/core"
	"github.com/Wangsu-Cloud-Storage/wcs-go-sdk/src/lib/utility"
	"github.com/zzzhr1990/v5-wcs-upload-util/uploader"
)

func main() {
	auth := utility.NewAuth("", "")
	config := core.NewConfig(false, "", "")
	// bm := core.NewBucketManager(auth, config, nil)
	// resp, err := bm.ListBucket()
	filePath := "Top.Gunner.Danger.Zone.2022.1080p.WEBRip.x265-RARBG.mp4"
	bucket := "cloud-disk"
	fileName := filepath.Base(filePath) + ".encrypt.bin"
	key := "hex/test/" + fileName
	etag, err := utility.ComputeFileEtag(filePath)
	if err != nil {
		fmt.Println("get file etag() failed:", err)
		return
	}
	fmt.Println("etag:", etag)

	deadline := time.Now().Add(time.Hour*24).Unix() * 1000
	policy := fmt.Sprintf(`{"scope": "%s:%s","deadline": "%d"}`, bucket, key, deadline)

	uploader.EncryptFunction = func(data []byte) []byte {
		result := make([]byte, len(data))
		for i, b := range data {
			result[i] = b ^ 1 // default should be 0x55
		}
		return result
	}
	resp, err := uploader.UploadFileConcurrent(auth, config, nil, filePath, policy, key, nil, 8)

	if err != nil {
		fmt.Println("uploadFile() failed:", err)
		return
	}

	fmt.Println(string(resp.Message))
}
