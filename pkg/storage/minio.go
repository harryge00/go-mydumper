package storage

import (
	"context"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io/ioutil"
	"strings"

	"github.com/minio/minio-go/v7"
)

// MinioStorage represents minio object storage.
//
// export for using in tests.
type MinioStorage struct {
	ctx    context.Context
	client *minio.Client
	bucket string
}

// Write file to minio object storage.
func (m *MinioStorage) WriteFile(name string, data string) error {
	r := strings.NewReader(data)
	_, err := m.client.PutObject(m.ctx, m.bucket, name, r, r.Size(), minio.PutObjectOptions{})
	return err
}

// Read file from minio object storage.
func (m *MinioStorage) ReadFile(name string) ([]byte, error) {
	// Get the encrypted object
	reader, err := m.client.GetObject(m.ctx, m.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	res, err := ioutil.ReadAll(reader)
	return res, err
}

func NewMinioStorage(ctx context.Context, endpoint, bucket, accessKey, secretKey string, useSSL bool) (*MinioStorage, error) {
	options := &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	}
	minioClient, err := minio.New(endpoint, options)
	if err != nil {
		return nil, err
	}

	found, err := minioClient.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}

	// Should create bucket if bucket not exists.
	if !found {
		err := minioClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}
	return &MinioStorage{
		ctx:    ctx,
		client: minioClient,
		bucket: bucket,
	}, nil
}
