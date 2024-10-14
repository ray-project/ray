package swaggerFiles

import (
	"golang.org/x/net/webdav"
)

func NewHandler() *webdav.Handler {
	return &webdav.Handler{
		FileSystem: FS,
		LockSystem: webdav.NewMemLS(),
	}
}
