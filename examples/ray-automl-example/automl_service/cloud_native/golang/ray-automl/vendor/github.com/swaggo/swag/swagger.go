package swag

import (
	"errors"
	"fmt"
	"sync"
)

// Name is a unique name be used to register swag instance.
const Name = "swagger"

var (
	swaggerMu sync.RWMutex
	swags     map[string]Swagger
)

// Swagger is an interface to read swagger document.
type Swagger interface {
	ReadDoc() string
}

// Register registers swagger for given name.
func Register(name string, swagger Swagger) {
	swaggerMu.Lock()
	defer swaggerMu.Unlock()

	if swagger == nil {
		panic("swagger is nil")
	}

	if swags == nil {
		swags = make(map[string]Swagger)
	}

	if _, ok := swags[name]; ok {
		panic("Register called twice for swag: " + name)
	}

	swags[name] = swagger
}

// GetSwagger returns the swagger instance for given name.
// If not found, returns nil.
func GetSwagger(name string) Swagger {
	swaggerMu.RLock()
	defer swaggerMu.RUnlock()

	return swags[name]
}

// ReadDoc reads swagger document. An optional name parameter can be passed to read a specific document.
// The default name is "swagger".
func ReadDoc(optionalName ...string) (string, error) {
	swaggerMu.RLock()
	defer swaggerMu.RUnlock()

	if swags == nil {
		return "", errors.New("no swag has yet been registered")
	}

	name := Name
	if len(optionalName) != 0 && optionalName[0] != "" {
		name = optionalName[0]
	}

	swag, ok := swags[name]
	if !ok {
		return "", fmt.Errorf("no swag named \"%s\" was registered", name)
	}

	return swag.ReadDoc(), nil
}
