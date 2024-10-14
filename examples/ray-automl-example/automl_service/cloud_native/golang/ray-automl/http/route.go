package http

import (
	"fmt"
	"net/http"
	"os"

	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func (r *RestServer) setupRoute(path string) http.Handler {
	router := gin.Default()
	loggerConfig := gin.LoggerConfig{
		Formatter: func(params gin.LogFormatterParams) string {
			return fmt.Sprintf("%v | %3d | %13v | %15s | %-7s %s | %s\n",
				params.TimeStamp.Format("2006/01/02 - 15:04:05"),
				params.StatusCode,
				params.Latency,
				params.ClientIP,
				params.Method,
				params.Path,
				params.ErrorMessage,
			)
		},
		Output: os.Stdout,
	}

	if path != "" {
		gin.SetMode(gin.ReleaseMode)
		router.Use(static.Serve("/", static.LocalFile(path, false)))
	}

	router.Use(gin.LoggerWithConfig(loggerConfig))

	// all requests start with /api
	api := router.Group(RootPath)

	// v1
	v1 := api.Group(Version1)
	{
		trainer := v1.Group(Trainer)
		trainer.POST(Create, r.trainerCreateV1)
		trainer.POST(Delete, r.trainerDeleteV1)
	}

	{
		trainer := v1.Group(Worker)
		trainer.POST(Create, r.workerCreateV1)
		trainer.POST(Delete, r.workerDeleteV1)
	}

	health := api.Group(Health)
	{
		health.GET("/", HealthCheck)
	}

	// swagger
	swaggers := router.Group(Swagger)
	{
		swaggers.GET("/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	return router
}
