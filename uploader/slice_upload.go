package uploader

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"github.com/Wangsu-Cloud-Storage/wcs-go-sdk/src/lib/core"
	"github.com/Wangsu-Cloud-Storage/wcs-go-sdk/src/lib/utility"
	"github.com/zzzhr1990/v5-wcs-upload-util/model"
)

/*
func SliceUploadFile(auth *utility.Auth, config *core.Config, client *http.Client) error {
	coreUploader := core.NewSliceUpload(auth, config, client)
	coreUploader.UploadFile()
}
*/
type Results struct {
	block_index int64
	status      bool
	result      core.SliceUploadResult
}

type Jobs struct {
	block_index    int64
	pos            int64
	chunk_size     int64
	upload_token   string
	key            string
	local_filename string
	retry_times    int
}

var (
	EncryptFunction func(data []byte) []byte = nil
)

func UploadFileConcurrent(auth *utility.Auth, config *core.Config, client *http.Client, localPath string, putPolicy string, key string, putExtra *core.PutExtra, poolSize int) (*model.MakeFileResponse, error) {
	coreUploader := core.NewSliceUpload(auth, config, client)
	if len(localPath) == 0 {
		return nil, errors.New("local_filename is empty")
	}
	if len(putPolicy) == 0 {
		return nil, errors.New("put_policy is empty")
	}
	if poolSize < 1 {
		return nil, errors.New("pool_size is invalid")
	}

	filename := key
	if len(filename) == 0 {
		filename = "goupload.tmp"
	}

	f, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	var block_size int64
	// 第一个分片不宜太大，因为可能遇到错误，上传太大是白费流量和时间！
	var first_chunk_size int64

	if fi.Size() < 1024 {
		block_size = fi.Size()
		first_chunk_size = fi.Size()
	} else {
		if fi.Size() < core.BlockSize {
			block_size = fi.Size()
		} else {
			block_size = core.BlockSize
		}
		first_chunk_size = 1024
	}

	first_chunk := make([]byte, first_chunk_size)
	n, err := f.Read(first_chunk)
	if nil != err {
		return nil, err
	}
	if first_chunk_size != int64(n) {
		err = errors.New("read size < request size")
		return nil, err
	}

	upload_token := auth.CreateUploadToken(putPolicy)
	dataToPost := first_chunk
	if EncryptFunction != nil {
		dataToPost = EncryptFunction(first_chunk)
	}
	response, err := coreUploader.MakeBlock(block_size, 0, dataToPost, upload_token, key)
	if nil != err {
		return nil, err
	}
	defer response.Body.Close()
	if http.StatusOK != response.StatusCode {
		body, _ := ioutil.ReadAll(response.Body)
		return nil, errors.New("MakeBlock failed, response code is " + strconv.Itoa(response.StatusCode) + ", response body is " + string(body))
	}
	body, _ := ioutil.ReadAll(response.Body)
	var result core.SliceUploadResult
	err = json.Unmarshal(body, &result)
	if nil != err {
		return nil, err
	}

	block_count := core.BlockCount(fi.Size())
	contexts := make([]string, block_count)
	contexts[0] = result.Context

	// 上传第 1 个 block 剩下的数据
	if block_size > first_chunk_size {
		first_block_left_size := block_size - first_chunk_size
		left_chunk := make([]byte, first_block_left_size)
		n, err = f.Read(left_chunk)
		if nil != err {
			return nil, err
		}
		if first_block_left_size != int64(n) {
			err = errors.New("read size < request size")
			return nil, err
		}
		dataToPost := left_chunk
		if EncryptFunction != nil {
			dataToPost = EncryptFunction(left_chunk)
		}
		response, err = coreUploader.Bput(contexts[0], first_chunk_size, dataToPost, upload_token, key)
		if nil != err {
			return nil, err
		}
		defer response.Body.Close()
		if http.StatusOK != response.StatusCode {
			body, _ := ioutil.ReadAll(response.Body)
			err = errors.New("Bput failed, response code is " + strconv.Itoa(response.StatusCode) + ", response body is " + string(body))
			return nil, err
		}
		body, _ := ioutil.ReadAll(response.Body)
		var result core.SliceUploadResult
		err = json.Unmarshal(body, &result)
		if nil != err {
			return nil, err
		}
		contexts[0] = result.Context

		// 上传后续 block 按配置的pool_size并发上传
		chjobs := make(chan *Jobs, 100)
		chresults := make(chan *Results, 10000)

		// 多消费者中的一个出现异常时，将flag置为true
		var stop_flag = false

		for w := 1; w <= poolSize; w++ {
			go worker(coreUploader, chjobs, chresults, &stop_flag)
		}

		// 每块上传任务提交到上传协程
		go func() {
			for block_index := int64(1); block_index < block_count; block_index++ {
				pos := block_size * block_index
				left_size := fi.Size() - pos
				var chunk_size int64

				if left_size > block_size {
					chunk_size = block_size
				} else {
					chunk_size = left_size
				}

				job := &Jobs{
					block_index:    block_index,
					chunk_size:     chunk_size,
					upload_token:   upload_token,
					key:            key,
					pos:            pos,
					local_filename: localPath,
					retry_times:    3,
				}
				chjobs <- job
			}
			close(chjobs)
		}()

		for i := int64(1); i < block_count; i++ {
			res := <-chresults
			var block_index = res.block_index
			if res.status {
				contexts[block_index] = res.result.Context
			} else {
				stop_flag = true
			}
		}
		close(chresults)

		if stop_flag {
			err = errors.New("upload chunk error")
			return nil, err
		}
	}

	response, err = coreUploader.MakeFile(fi.Size(), key, contexts, upload_token, putExtra)
	if err != nil {
		return nil, errors.New("makeFile failed, " + err.Error())
	}
	defer response.Body.Close()
	data, _ := ioutil.ReadAll(response.Body)
	serverResponse := &model.MakeFileResponse{}
	err = json.Unmarshal(data, serverResponse)
	if err != nil {
		// if response is not a standard JSON, server response must be internal error
		// it is unnecessary to detect server's response status (nginx may return 0)
		return nil, errors.New("makeFile failed, " + err.Error())
	}
	if http.StatusOK != response.StatusCode {
		return serverResponse, errors.New("makeFile failed, response code is " + strconv.Itoa(response.StatusCode) + ", response body is " + serverResponse.Message)
	}

	return serverResponse, nil
}

func worker(s *core.SliceUpload, jobs <-chan *Jobs, results chan<- *Results, stop_flag *bool) {
	for j := range jobs {
		var res Results
		if *stop_flag {
			res.block_index = j.block_index
			res.status = false
			results <- &res
			continue
		}
		f, err := os.Open(j.local_filename)
		if err != nil {
			taskBreak(f, res, j, results, stop_flag)
			continue
		}
		f.Seek(j.pos, 0)
		block := make([]byte, j.chunk_size)
		n, err := f.Read(block)
		if nil != err {
			taskBreak(f, res, j, results, stop_flag)
			continue
		}
		if j.chunk_size != int64(n) {
			taskBreak(f, res, j, results, stop_flag)
			continue
		}
		dataToPost := block
		if EncryptFunction != nil {
			dataToPost = EncryptFunction(block)
		}
		// 块上传重试机制
		rt := j.retry_times
		for {
			response, err := s.MakeBlock(j.chunk_size, j.block_index, dataToPost, j.upload_token, j.key)
			if err != nil {
				rt--
				if rt == 0 {
					taskBreak(f, res, j, results, stop_flag)
					break
				}
				continue
			}
			//defer response.Body.Close()
			if http.StatusOK != response.StatusCode {
				rt--
				if rt == 0 {
					taskBreak(f, res, j, results, stop_flag)
					response.Body.Close()
					break
				}
				response.Body.Close()
				continue
			}
			body, _ := ioutil.ReadAll(response.Body)
			response.Body.Close()
			var result core.SliceUploadResult
			err = json.Unmarshal(body, &result)
			if err != nil {
				rt--
				if rt == 0 {
					taskBreak(f, res, j, results, stop_flag)
					break
				}
				continue
			}
			res.block_index = j.block_index
			res.result = result
			res.status = true
			results <- &res
			f.Close()
			break
		}
	}
}

/**
程序异常退出时关闭资源
*/
func taskBreak(f *os.File, res Results, j *Jobs, results chan<- *Results, stop_flag *bool) {
	f.Close()
	res.block_index = j.block_index
	res.status = false
	results <- &res
	*stop_flag = true
}
