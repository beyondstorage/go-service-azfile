package azfile

import (
	"context"
	"encoding/base64"
	"io"
	"strconv"

	"github.com/Azure/azure-storage-file-go/azfile"

	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"
	"github.com/beyondstorage/go-storage/v4/services"
	. "github.com/beyondstorage/go-storage/v4/types"
)

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	rp := s.getAbsPath(path)

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			return
		}

		rp += "/"
		o = s.newObject(true)
		o.Mode |= ModeDir
	} else {
		o = s.newObject(false)
		o.Mode |= ModeRead
	}

	o.ID = rp
	o.Path = path

	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			err = services.PairUnsupportedError{Pair: ps.WithObjectMode(opt.ObjectMode)}
			return err
		}

		_, err = s.client.NewDirectoryURL(rp).Delete(ctx)
	} else {
		_, err = s.client.NewFileURL(rp).Delete(ctx)
	}

	if err != nil {
		if checkError(err, azfile.ServiceCodeResourceNotFound) {
			err = nil
		} else {
			return err
		}
	}

	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	input := &objectPageStatus{
		maxResults: 200,
		prefix:     s.getAbsPath(path),
	}

	if !opt.HasListMode {
		opt.ListMode = ListModePrefix
	}

	var nextFn NextObjectFunc

	switch {
	case opt.ListMode.IsDir():
		nextFn = s.nextObjectPageByDir
	case opt.ListMode.IsPrefix():
		nextFn = s.nextObjectPageByPrefix
	default:
		return nil, services.ListModeInvalidError{Actual: opt.ListMode}
	}

	return NewObjectIterator(ctx, nextFn, input), nil
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.WorkDir = s.workDir
	return meta
}

func (s *Storage) nextObjectPageByDir(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	options := azfile.ListFilesAndDirectoriesOptions{
		Prefix:     input.prefix,
		MaxResults: input.maxResults,
	}

	output, err := s.client.ListFilesAndDirectoriesSegment(ctx, input.marker, options)
	if err != nil {
		return err
	}

	for _, v := range output.DirectoryItems {
		o := s.newObject(true)
		o.ID = v.Name
		o.Path = s.getRelPath(v.Name)
		o.Mode |= ModeDir

		page.Data = append(page.Data, o)
	}

	for _, v := range output.FileItems {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if !output.NextMarker.NotDone() {
		return IterateDone
	}

	input.marker = output.NextMarker

	return nil
}

func (s *Storage) nextObjectPageByPrefix(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)
	options := azfile.ListFilesAndDirectoriesOptions{
		Prefix:     input.prefix,
		MaxResults: input.maxResults,
	}

	output, err := s.client.ListFilesAndDirectoriesSegment(ctx, input.marker, options)
	if err != nil {
		return err
	}

	for _, v := range output.DirectoryItems {
		o, err := s.formatDirObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	for _, v := range output.FileItems {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}
		page.Data = append(page.Data, o)
	}

	if !output.NextMarker.NotDone() {
		return IterateDone
	}

	input.marker = output.NextMarker

	return nil
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)

	offset := int64(0)
	if opt.HasOffset {
		offset = opt.Offset
	}

	count := int64(azfile.CountToEnd)
	if opt.HasSize {
		count = opt.Size
	}

	output, err := s.client.NewFileURL(rp).Download(ctx, offset, count, false)
	if err != nil {
		return 0, err
	}
	defer func() {
		cErr := output.Response().Body.Close()
		if cErr != nil {
			err = cErr
		}
	}()

	rc := output.Response().Body
	if opt.HasIoCallback {
		rc = iowrap.CallbackReadCloser(rc, opt.IoCallback)
	}

	return io.Copy(w, rc)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	rp := s.getAbsPath(path)

	var dirOutput *azfile.DirectoryGetPropertiesResponse
	var fileOutput *azfile.FileGetPropertiesResponse

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			err = services.PairUnsupportedError{Pair: ps.WithObjectMode(opt.ObjectMode)}
			return
		}

		dirOutput, err = s.client.NewDirectoryURL(rp).GetProperties(ctx)
	} else {
		fileOutput, err = s.client.NewFileURL(rp).GetProperties(ctx)
	}

	if err != nil {
		return nil, err
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		o.Mode |= ModeDir

		o.SetLastModified(dirOutput.LastModified())

		if v := string(dirOutput.ETag()); v != "" {
			o.SetEtag(v)
		}

		var sm ObjectSystemMetadata
		if v, err := strconv.ParseBool(dirOutput.IsServerEncrypted()); err == nil {
			sm.ServerEncrypted = v
		}
		o.SetSystemMetadata(sm)
	} else {
		o.Mode |= ModeRead

		o.SetContentLength(fileOutput.ContentLength())
		o.SetLastModified(fileOutput.LastModified())

		if v := string(fileOutput.ETag()); v != "" {
			o.SetEtag(v)
		}
		if v := fileOutput.ContentType(); v != "" {
			o.SetContentType(v)
		}
		if v := fileOutput.ContentMD5(); len(v) > 0 {
			o.SetContentMd5(base64.StdEncoding.EncodeToString(v))
		}

		var sm ObjectSystemMetadata
		if v, err := strconv.ParseBool(fileOutput.IsServerEncrypted()); err == nil {
			sm.ServerEncrypted = v
		}
		o.SetSystemMetadata(sm)
	}

	return o, nil
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	rp := s.getAbsPath(path)

	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	body := iowrap.SizedReadSeekCloser(r, size)

	_, err = s.client.NewFileURL(rp).UploadRange(ctx, 0, body, nil)
	if err != nil {
		return 0, err
	}

	return size, nil
}
