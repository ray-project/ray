package swag

import (
	"go/ast"
	"go/token"
	"reflect"
	"strconv"
	"strings"
)

// ConstVariable a model to record a const variable
type ConstVariable struct {
	Name    *ast.Ident
	Type    ast.Expr
	Value   interface{}
	Comment *ast.CommentGroup
	File    *ast.File
	Pkg     *PackageDefinitions
}

var escapedChars = map[uint8]uint8{
	'n':  '\n',
	'r':  '\r',
	't':  '\t',
	'v':  '\v',
	'\\': '\\',
	'"':  '"',
}

// EvaluateEscapedChar parse escaped character
func EvaluateEscapedChar(text string) rune {
	if len(text) == 1 {
		return rune(text[0])
	}

	if len(text) == 2 && text[0] == '\\' {
		return rune(escapedChars[text[1]])
	}

	if len(text) == 6 && text[0:2] == "\\u" {
		n, err := strconv.ParseInt(text[2:], 16, 32)
		if err == nil {
			return rune(n)
		}
	}

	return 0
}

// EvaluateEscapedString parse escaped characters in string
func EvaluateEscapedString(text string) string {
	if !strings.ContainsRune(text, '\\') {
		return text
	}
	result := make([]byte, 0, len(text))
	for i := 0; i < len(text); i++ {
		if text[i] == '\\' {
			i++
			if text[i] == 'u' {
				i++
				char, err := strconv.ParseInt(text[i:i+4], 16, 32)
				if err == nil {
					result = AppendUtf8Rune(result, rune(char))
				}
				i += 3
			} else if c, ok := escapedChars[text[i]]; ok {
				result = append(result, c)
			}
		} else {
			result = append(result, text[i])
		}
	}
	return string(result)
}

// EvaluateDataConversion evaluate the type a explicit type conversion
func EvaluateDataConversion(x interface{}, typeName string) interface{} {
	switch value := x.(type) {
	case int:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case uint:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case int8:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case uint8:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case int16:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case uint16:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case int32:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		case "string":
			return string(value)
		}
	case uint32:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case int64:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case uint64:
		switch typeName {
		case "int":
			return int(value)
		case "byte":
			return byte(value)
		case "int8":
			return int8(value)
		case "int16":
			return int16(value)
		case "int32":
			return int32(value)
		case "int64":
			return int64(value)
		case "uint":
			return uint(value)
		case "uint8":
			return uint8(value)
		case "uint16":
			return uint16(value)
		case "uint32":
			return uint32(value)
		case "uint64":
			return uint64(value)
		case "rune":
			return rune(value)
		}
	case string:
		switch typeName {
		case "string":
			return value
		}
	}
	return nil
}

// EvaluateUnary evaluate the type and value of a unary expression
func EvaluateUnary(x interface{}, operator token.Token, xtype ast.Expr) (interface{}, ast.Expr) {
	switch operator {
	case token.SUB:
		switch value := x.(type) {
		case int:
			return -value, xtype
		case int8:
			return -value, xtype
		case int16:
			return -value, xtype
		case int32:
			return -value, xtype
		case int64:
			return -value, xtype
		}
	case token.XOR:
		switch value := x.(type) {
		case int:
			return ^value, xtype
		case int8:
			return ^value, xtype
		case int16:
			return ^value, xtype
		case int32:
			return ^value, xtype
		case int64:
			return ^value, xtype
		case uint:
			return ^value, xtype
		case uint8:
			return ^value, xtype
		case uint16:
			return ^value, xtype
		case uint32:
			return ^value, xtype
		case uint64:
			return ^value, xtype
		}
	}
	return nil, nil
}

// EvaluateBinary evaluate the type and value of a binary expression
func EvaluateBinary(x, y interface{}, operator token.Token, xtype, ytype ast.Expr) (interface{}, ast.Expr) {
	if operator == token.SHR || operator == token.SHL {
		var rightOperand uint64
		yValue := CanIntegerValue{reflect.ValueOf(y)}
		if yValue.CanUint() {
			rightOperand = yValue.Uint()
		} else if yValue.CanInt() {
			rightOperand = uint64(yValue.Int())
		}

		switch operator {
		case token.SHL:
			switch xValue := x.(type) {
			case int:
				return xValue << rightOperand, xtype
			case int8:
				return xValue << rightOperand, xtype
			case int16:
				return xValue << rightOperand, xtype
			case int32:
				return xValue << rightOperand, xtype
			case int64:
				return xValue << rightOperand, xtype
			case uint:
				return xValue << rightOperand, xtype
			case uint8:
				return xValue << rightOperand, xtype
			case uint16:
				return xValue << rightOperand, xtype
			case uint32:
				return xValue << rightOperand, xtype
			case uint64:
				return xValue << rightOperand, xtype
			}
		case token.SHR:
			switch xValue := x.(type) {
			case int:
				return xValue >> rightOperand, xtype
			case int8:
				return xValue >> rightOperand, xtype
			case int16:
				return xValue >> rightOperand, xtype
			case int32:
				return xValue >> rightOperand, xtype
			case int64:
				return xValue >> rightOperand, xtype
			case uint:
				return xValue >> rightOperand, xtype
			case uint8:
				return xValue >> rightOperand, xtype
			case uint16:
				return xValue >> rightOperand, xtype
			case uint32:
				return xValue >> rightOperand, xtype
			case uint64:
				return xValue >> rightOperand, xtype
			}
		}
		return nil, nil
	}

	evalType := xtype
	if evalType == nil {
		evalType = ytype
	}

	xValue := CanIntegerValue{reflect.ValueOf(x)}
	yValue := CanIntegerValue{reflect.ValueOf(y)}
	if xValue.Kind() == reflect.String && yValue.Kind() == reflect.String {
		return xValue.String() + yValue.String(), evalType
	}

	var targetValue reflect.Value
	if xValue.Kind() != reflect.Int {
		targetValue = reflect.New(xValue.Type()).Elem()
	} else {
		targetValue = reflect.New(yValue.Type()).Elem()
	}

	switch operator {
	case token.ADD:
		if xValue.CanInt() && yValue.CanInt() {
			targetValue.SetInt(xValue.Int() + yValue.Int())
		} else if xValue.CanUint() && yValue.CanUint() {
			targetValue.SetUint(xValue.Uint() + yValue.Uint())
		} else if xValue.CanInt() && yValue.CanUint() {
			targetValue.SetUint(uint64(xValue.Int()) + yValue.Uint())
		} else if xValue.CanUint() && yValue.CanInt() {
			targetValue.SetUint(xValue.Uint() + uint64(yValue.Int()))
		}
	case token.SUB:
		if xValue.CanInt() && yValue.CanInt() {
			targetValue.SetInt(xValue.Int() - yValue.Int())
		} else if xValue.CanUint() && yValue.CanUint() {
			targetValue.SetUint(xValue.Uint() - yValue.Uint())
		} else if xValue.CanInt() && yValue.CanUint() {
			targetValue.SetUint(uint64(xValue.Int()) - yValue.Uint())
		} else if xValue.CanUint() && yValue.CanInt() {
			targetValue.SetUint(xValue.Uint() - uint64(yValue.Int()))
		}
	case token.MUL:
		if xValue.CanInt() && yValue.CanInt() {
			targetValue.SetInt(xValue.Int() * yValue.Int())
		} else if xValue.CanUint() && yValue.CanUint() {
			targetValue.SetUint(xValue.Uint() * yValue.Uint())
		} else if xValue.CanInt() && yValue.CanUint() {
			targetValue.SetUint(uint64(xValue.Int()) * yValue.Uint())
		} else if xValue.CanUint() && yValue.CanInt() {
			targetValue.SetUint(xValue.Uint() * uint64(yValue.Int()))
		}
	case token.QUO:
		if xValue.CanInt() && yValue.CanInt() {
			targetValue.SetInt(xValue.Int() / yValue.Int())
		} else if xValue.CanUint() && yValue.CanUint() {
			targetValue.SetUint(xValue.Uint() / yValue.Uint())
		} else if xValue.CanInt() && yValue.CanUint() {
			targetValue.SetUint(uint64(xValue.Int()) / yValue.Uint())
		} else if xValue.CanUint() && yValue.CanInt() {
			targetValue.SetUint(xValue.Uint() / uint64(yValue.Int()))
		}
	case token.REM:
		if xValue.CanInt() && yValue.CanInt() {
			targetValue.SetInt(xValue.Int() % yValue.Int())
		} else if xValue.CanUint() && yValue.CanUint() {
			targetValue.SetUint(xValue.Uint() % yValue.Uint())
		} else if xValue.CanInt() && yValue.CanUint() {
			targetValue.SetUint(uint64(xValue.Int()) % yValue.Uint())
		} else if xValue.CanUint() && yValue.CanInt() {
			targetValue.SetUint(xValue.Uint() % uint64(yValue.Int()))
		}
	case token.AND:
		if xValue.CanInt() && yValue.CanInt() {
			targetValue.SetInt(xValue.Int() & yValue.Int())
		} else if xValue.CanUint() && yValue.CanUint() {
			targetValue.SetUint(xValue.Uint() & yValue.Uint())
		} else if xValue.CanInt() && yValue.CanUint() {
			targetValue.SetUint(uint64(xValue.Int()) & yValue.Uint())
		} else if xValue.CanUint() && yValue.CanInt() {
			targetValue.SetUint(xValue.Uint() & uint64(yValue.Int()))
		}
	case token.OR:
		if xValue.CanInt() && yValue.CanInt() {
			targetValue.SetInt(xValue.Int() | yValue.Int())
		} else if xValue.CanUint() && yValue.CanUint() {
			targetValue.SetUint(xValue.Uint() | yValue.Uint())
		} else if xValue.CanInt() && yValue.CanUint() {
			targetValue.SetUint(uint64(xValue.Int()) | yValue.Uint())
		} else if xValue.CanUint() && yValue.CanInt() {
			targetValue.SetUint(xValue.Uint() | uint64(yValue.Int()))
		}
	case token.XOR:
		if xValue.CanInt() && yValue.CanInt() {
			targetValue.SetInt(xValue.Int() ^ yValue.Int())
		} else if xValue.CanUint() && yValue.CanUint() {
			targetValue.SetUint(xValue.Uint() ^ yValue.Uint())
		} else if xValue.CanInt() && yValue.CanUint() {
			targetValue.SetUint(uint64(xValue.Int()) ^ yValue.Uint())
		} else if xValue.CanUint() && yValue.CanInt() {
			targetValue.SetUint(xValue.Uint() ^ uint64(yValue.Int()))
		}
	}
	return targetValue.Interface(), evalType
}
