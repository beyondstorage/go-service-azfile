package azfile

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/beyondstorage/go-endpoint"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/credential"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
)

// Storage is the azfile client.
type Storage struct {
	client azfile.DirectoryURL

	workDir string

	defaultPairs DefaultStoragePairs
	features     StorageFeatures

	types.UnimplementedStorager
	types.UnimplementedDirer
}

// String implements Storager.String
func (s *Storage) String() string {
	return fmt.Sprintf("Storager azfile {WorkDir: %s}", s.workDir)
}

// NewStorager will create Storager only.
func NewStorager(pairs ...types.Pair) (types.Storager, error) {
	return newStorager(pairs...)
}

// newStorager will create a storage client.
func newStorager(pairs ...types.Pair) (store *Storage, err error) {
	defer func() {
		if err != nil {
			err = services.InitError{Op: "new_storager", Type: Type, Err: formatError(err), Pairs: pairs}
		}
	}()

	opt, err := parsePairStorageNew(pairs)
	if err != nil {
		return nil, err
	}

	store = &Storage{
		workDir: "/",
	}

	if opt.HasWorkDir {
		store.workDir = opt.WorkDir
	}

	ep, err := endpoint.Parse(opt.Endpoint)
	if err != nil {
		return nil, err
	}

	var uri string
	switch ep.Protocol() {
	case endpoint.ProtocolHTTP:
		uri, _, _ = ep.HTTP()
	case endpoint.ProtocolHTTPS:
		uri, _, _ = ep.HTTPS()
	default:
		return nil, services.PairUnsupportedError{Pair: ps.WithEndpoint(opt.Endpoint)}
	}

	primaryURL, _ := url.Parse(uri)

	cred, err := credential.Parse(opt.Credential)
	if err != nil {
		return nil, err
	}
	if cred.Protocol() != credential.ProtocolHmac {
		return nil, services.PairUnsupportedError{Pair: ps.WithCredential(opt.Credential)}
	}

	credValue, err := azfile.NewSharedKeyCredential(cred.Hmac())
	if err != nil {
		return nil, err
	}

	p := azfile.NewPipeline(credValue, azfile.PipelineOptions{
		Retry: azfile.RetryOptions{
			// Use a fixed back-off retry policy.
			Policy: 1,
			// A value of 1 means 1 try and no retries.
			MaxTries: 1,
			// Set a long enough timeout to adopt our timeout control.
			// This value could be adjusted to context deadline if request context has a deadline set.
			TryTimeout: 720 * time.Hour,
		},
	})

	workDir := strings.TrimPrefix(store.workDir, "/")
	store.client = azfile.NewDirectoryURL(*primaryURL, p).NewDirectoryURL(workDir)

	if opt.HasDefaultStoragePairs {
		store.defaultPairs = opt.DefaultStoragePairs
	}
	if opt.HasStorageFeatures {
		store.features = opt.StorageFeatures
	}

	return store, nil
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	if err == nil {
		return nil
	}

	return services.StorageError{
		Op:       op,
		Err:      formatError(err),
		Storager: s,
		Path:     path,
	}
}

// formatError converts errors returned by SDK into errors defined in go-storage and go-service-*.
// The original error SHOULD NOT be wrapped.
func formatError(err error) error {
	if _, ok := err.(services.InternalError); ok {
		return err
	}

	e, ok := err.(azfile.StorageError)

	if ok {
		switch azfile.StorageErrorCodeType(e.ServiceCode()) {
		case "":
			switch e.Response().StatusCode {
			case fileNotFound:
				return fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
			default:
				return fmt.Errorf("%w: %v", services.ErrUnexpected, err)
			}
		case azfile.StorageErrorCodeResourceNotFound:
			return fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
		case azfile.StorageErrorCodeInsufficientAccountPermissions:
			return fmt.Errorf("%w: %v", services.ErrPermissionDenied, err)
		default:
			return fmt.Errorf("%w: %v", services.ErrUnexpected, err)
		}
	}

	return fmt.Errorf("%w: %v", services.ErrUnexpected, err)
}

// getAbsPath will calculate object storage's abs path
func (s *Storage) getAbsPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return prefix + path
}

// getRelPath will get object storage's rel path.
func (s *Storage) getRelPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return strings.TrimPrefix(path, prefix)
}

func (s *Storage) newObject(done bool) *types.Object {
	return types.NewObject(s, done)
}

func (s *Storage) formatFileObject(v azfile.FileItem) (o *types.Object, err error) {
	o = s.newObject(true)
	o.ID = v.Name
	o.Path = s.getRelPath(v.Name)
	o.Mode |= types.ModeRead

	if v.Properties.ContentLength != 0 {
		o.SetContentLength(v.Properties.ContentLength)
	}

	return
}

func (s *Storage) formatDirObject(v azfile.DirectoryItem) (o *types.Object, err error) {
	o = s.newObject(true)
	o.ID = v.Name
	o.Path = s.getRelPath(v.Name)
	o.Mode |= types.ModeDir

	return
}

const (
	// File not found error.
	fileNotFound = 404
)

func checkError(err error, expect int) bool {
	e, ok := err.(azfile.StorageError)
	if !ok {
		return false
	}

	return e.Response().StatusCode == expect
}
