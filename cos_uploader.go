package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type MetadataCredential struct {
	TmpSecretId  string `json:"TmpSecretId"`
	TmpSecretKey string `json:"TmpSecretKey"`
	Token        string `json:"Token"`
	ExpiredTime  int64  `json:"ExpiredTime"`
	Code         string `json:"Code"`
}

type Credential struct {
	SecretId  string
	SecretKey string
	Token     string
	ExpireAt  int64
}

type COSUploader struct {
	role string

	bucketURL string

	mu   sync.RWMutex
	cred Credential

	client *cos.Client

	httpClient *http.Client
}

func NewCOSUploader(role string, bucketName string, region string) *COSUploader {
	bucketURL := fmt.Sprintf(
		"https://%s.cos.%s.myqcloud.com",
		bucketName,
		region,
	)
	u := &COSUploader{
		role:       role,
		bucketURL:  bucketURL,
		httpClient: &http.Client{Timeout: 2 * time.Second},
	}

	go u.autoRefresh()

	return u
}

func (u *COSUploader) metadataURL() string {

	return fmt.Sprintf(
		"http://metadata.tencentyun.com/latest/meta-data/cam/security-credentials/%s",
		u.role,
	)
}

func (u *COSUploader) fetchCredential() (*MetadataCredential, error) {

	resp, err := u.httpClient.Get(u.metadataURL())
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var data MetadataCredential

	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, err
	}

	if data.Code != "Success" {
		return nil, fmt.Errorf("metadata error")
	}

	return &data, nil
}

func (u *COSUploader) refresh() error {

	var lastErr error

	for i := 0; i < 3; i++ {

		data, err := u.fetchCredential()

		if err == nil {

			cred := Credential{
				SecretId:  data.TmpSecretId,
				SecretKey: data.TmpSecretKey,
				Token:     data.Token,
				ExpireAt:  data.ExpiredTime,
			}

			u.updateCredential(cred)

			return nil
		}

		lastErr = err

		time.Sleep(time.Second)
	}

	return lastErr
}

func (u *COSUploader) updateCredential(c Credential) {

	u.mu.Lock()
	defer u.mu.Unlock()

	u.cred = c

	u.buildCOSClient()
}

func (u *COSUploader) buildCOSClient() {

	bucketURL, _ := url.Parse(u.bucketURL)

	b := &cos.BaseURL{BucketURL: bucketURL}
	if len(u.cred.SecretId) > 8 {
		fmt.Printf("[parquet] new SecretID %s...%s\n", u.cred.SecretId[:4], u.cred.SecretId[len(u.cred.SecretId)-4:])
	} else {
		fmt.Printf("[parquet] new SecretID %s\n", u.cred.SecretId)
	}
	u.client = cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:     u.cred.SecretId,
			SecretKey:    u.cred.SecretKey,
			SessionToken: u.cred.Token,
		},
	})
}

func (u *COSUploader) autoRefresh() {

	for {

		err := u.refresh()

		if err != nil {

			fmt.Println("credential refresh failed:", err)

			time.Sleep(10 * time.Second)

			continue
		}

		u.mu.RLock()
		expire := u.cred.ExpireAt
		u.mu.RUnlock()

		expireTime := time.Unix(expire, 0)

		refreshTime := expireTime.Add(-15 * time.Minute)

		sleep := time.Until(refreshTime)

		if sleep <= 0 {
			sleep = time.Minute
		}

		time.Sleep(sleep)
	}
}

func (u *COSUploader) ForceRefresh() error {

	return u.refresh()
}

func isCredentialError(err error) bool {

	msg := err.Error()

	if strings.Contains(msg, "SignatureDoesNotMatch") {
		return true
	}

	if strings.Contains(msg, "Request has expired") {
		return true
	}

	if strings.Contains(msg, "InvalidAccessKeyId") {
		return true
	}

	return false
}

func (u *COSUploader) Upload(object string, data []byte) error {

	u.mu.RLock()
	client := u.client
	u.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("cos client not ready")
	}

	ctx := context.Background()

	key := strings.TrimPrefix(object, "/")

	_, err := client.Object.Put(
		ctx,
		key,
		bytes.NewReader(data),
		nil,
	)

	if err == nil {
		return nil
	}

	// credential error -> refresh + retry
	if isCredentialError(err) {

		err2 := u.ForceRefresh()
		if err2 != nil {
			return err
		}

		u.mu.RLock()
		client = u.client
		u.mu.RUnlock()

		_, err = client.Object.Put(
			ctx,
			key,
			bytes.NewReader(data),
			nil,
		)

		return err
	}

	return err
}
