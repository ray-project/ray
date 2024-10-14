package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Response used for restful API response in dashboard server
type Response struct {
	Success   bool        `json:"success"`
	Message   string      `json:"message"`
	Code      int         `json:"code"`
	Data      interface{} `json:"data" swaggerignore:"true"`
	Ip        string      `json:"ip"`
	Timestamp int64       `json:"timestamp"`
}

type JsonResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func NewJsonResponse(success bool, message string, data interface{}) *JsonResponse {
	return &JsonResponse{
		Success: success,
		Message: message,
		Data:    data,
	}
}

func JSONSuccessResponse(c *gin.Context, data interface{}, message string) {
	var code = http.StatusOK
	c.JSON(code, Response{
		Success:   true,
		Message:   message,
		Code:      code,
		Data:      data,
		Ip:        localIpAddr,
		Timestamp: time.Now().Unix(),
	})
}

func JSONFailedResponse(c *gin.Context, err error, message string) {
	var code = http.StatusBadRequest
	errResponse := ErrResponse{
		Err: err.Error(),
	}
	c.JSON(code, Response{
		Success:   false,
		Message:   message,
		Code:      code,
		Data:      errResponse,
		Ip:        localIpAddr,
		Timestamp: time.Now().Unix(),
	})
}

type ErrResponse struct {
	Err string `json:"err"`
}

var localIpAddr = GetLocalIPAddrV4()

func JSONSuccessTextResponse(c *gin.Context, text string) {
	var code = http.StatusOK
	c.String(code, text)
}
