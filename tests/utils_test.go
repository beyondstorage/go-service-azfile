package tests

import (
	"os"
	"testing"

	azfile "github.com/beyondstorage/go-service-azfile"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/google/uuid"
)

func setupTest(t *testing.T) types.Storager {
	t.Log("Setup test for azfile")

	store, err := azfile.NewStorager(
		ps.WithCredential(os.Getenv("STORAGE_AZFILE_CREDENTIAL")),
		ps.WithEndpoint(os.Getenv("STORAGE_AZFILE_ENDPOINT")),
		ps.WithWorkDir("/"+uuid.New().String()+"/"),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}
	return store
}
