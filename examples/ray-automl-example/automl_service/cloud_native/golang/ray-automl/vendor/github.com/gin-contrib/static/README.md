# static middleware

[![Build Status](https://travis-ci.org/gin-contrib/static.svg)](https://travis-ci.org/gin-contrib/static)
[![codecov](https://codecov.io/gh/gin-contrib/static/branch/master/graph/badge.svg)](https://codecov.io/gh/gin-contrib/static)
[![Go Report Card](https://goreportcard.com/badge/github.com/gin-contrib/static)](https://goreportcard.com/report/github.com/gin-contrib/static)
[![GoDoc](https://godoc.org/github.com/gin-contrib/static?status.svg)](https://godoc.org/github.com/gin-contrib/static)

Static middleware

## Usage

### Start using it

Download and install it:

```sh
go get github.com/gin-contrib/static
```

Import it in your code:

```go
import "github.com/gin-contrib/static"
```

### Canonical example

See the [example](example)

[embedmd]:# (_example/simple/example.go go)
```go
package main

import (
	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	// if Allow DirectoryIndex
	//r.Use(static.Serve("/", static.LocalFile("/tmp", true)))
	// set prefix
	//r.Use(static.Serve("/static", static.LocalFile("/tmp", true)))

	r.Use(static.Serve("/", static.LocalFile("/tmp", false)))
	r.GET("/ping", func(c *gin.Context) {
		c.String(200, "test")
	})
	// Listen and Server in 0.0.0.0:8080
	r.Run(":8080")
}
```
