package model

import (
	"github.com/Wangsu-Cloud-Storage/wcs-go-sdk/src/lib/core"
)

type MakeFileResponse struct {
	core.CommonResult
	Key  string `json:"key"`
	Hash string `json:"hash"`
}
