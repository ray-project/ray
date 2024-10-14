package http

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

// HealthCheck godoc
// @Summary Show the status of server.
// @Description get the status of server.
// @Tags root
// @Accept */*
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /api/health/ [get]
func HealthCheck(c *gin.Context) {
	res := map[string]interface{}{
		"data": "Server is up and running",
	}

	message := fmt.Sprintf("Ray-AutoML-Operator server is up and running")
	JSONSuccessResponse(c, res, message)
}
