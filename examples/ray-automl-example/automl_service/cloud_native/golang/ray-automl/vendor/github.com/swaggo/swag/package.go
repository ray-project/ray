package swag

import (
	"go/ast"
	"go/token"
	"reflect"
	"strconv"
)

// PackageDefinitions files and definition in a package.
type PackageDefinitions struct {
	// files in this package, map key is file's relative path starting package path
	Files map[string]*ast.File

	// definitions in this package, map key is typeName
	TypeDefinitions map[string]*TypeSpecDef

	// const variables in this package, map key is the name
	ConstTable map[string]*ConstVariable

	// const variables in order in this package
	OrderedConst []*ConstVariable

	// package name
	Name string

	// package path
	Path string
}

// ConstVariableGlobalEvaluator an interface used to evaluate enums across packages
type ConstVariableGlobalEvaluator interface {
	EvaluateConstValue(pkg *PackageDefinitions, cv *ConstVariable, recursiveStack map[string]struct{}) (interface{}, ast.Expr)
	EvaluateConstValueByName(file *ast.File, pkgPath, constVariableName string, recursiveStack map[string]struct{}) (interface{}, ast.Expr)
	FindTypeSpec(typeName string, file *ast.File) *TypeSpecDef
}

// NewPackageDefinitions new a PackageDefinitions object
func NewPackageDefinitions(name, pkgPath string) *PackageDefinitions {
	return &PackageDefinitions{
		Name:            name,
		Path:            pkgPath,
		Files:           make(map[string]*ast.File),
		TypeDefinitions: make(map[string]*TypeSpecDef),
		ConstTable:      make(map[string]*ConstVariable),
	}
}

// AddFile add a file
func (pkg *PackageDefinitions) AddFile(pkgPath string, file *ast.File) *PackageDefinitions {
	pkg.Files[pkgPath] = file
	return pkg
}

// AddTypeSpec add a type spec.
func (pkg *PackageDefinitions) AddTypeSpec(name string, typeSpec *TypeSpecDef) *PackageDefinitions {
	pkg.TypeDefinitions[name] = typeSpec
	return pkg
}

// AddConst add a const variable.
func (pkg *PackageDefinitions) AddConst(astFile *ast.File, valueSpec *ast.ValueSpec) *PackageDefinitions {
	for i := 0; i < len(valueSpec.Names) && i < len(valueSpec.Values); i++ {
		variable := &ConstVariable{
			Name:    valueSpec.Names[i],
			Type:    valueSpec.Type,
			Value:   valueSpec.Values[i],
			Comment: valueSpec.Comment,
			File:    astFile,
		}
		pkg.ConstTable[valueSpec.Names[i].Name] = variable
		pkg.OrderedConst = append(pkg.OrderedConst, variable)
	}
	return pkg
}

func (pkg *PackageDefinitions) evaluateConstValue(file *ast.File, iota int, expr ast.Expr, globalEvaluator ConstVariableGlobalEvaluator, recursiveStack map[string]struct{}) (interface{}, ast.Expr) {
	switch valueExpr := expr.(type) {
	case *ast.Ident:
		if valueExpr.Name == "iota" {
			return iota, nil
		}
		if pkg.ConstTable != nil {
			if cv, ok := pkg.ConstTable[valueExpr.Name]; ok {
				return globalEvaluator.EvaluateConstValue(pkg, cv, recursiveStack)
			}
		}
	case *ast.SelectorExpr:
		pkgIdent, ok := valueExpr.X.(*ast.Ident)
		if !ok {
			return nil, nil
		}
		return globalEvaluator.EvaluateConstValueByName(file, pkgIdent.Name, valueExpr.Sel.Name, recursiveStack)
	case *ast.BasicLit:
		switch valueExpr.Kind {
		case token.INT:
			// hexadecimal
			if len(valueExpr.Value) > 2 && valueExpr.Value[0] == '0' && valueExpr.Value[1] == 'x' {
				if x, err := strconv.ParseInt(valueExpr.Value[2:], 16, 64); err == nil {
					return int(x), nil
				} else if x, err := strconv.ParseUint(valueExpr.Value[2:], 16, 64); err == nil {
					return x, nil
				} else {
					panic(err)
				}
			}

			//octet
			if len(valueExpr.Value) > 1 && valueExpr.Value[0] == '0' {
				if x, err := strconv.ParseInt(valueExpr.Value[1:], 8, 64); err == nil {
					return int(x), nil
				} else if x, err := strconv.ParseUint(valueExpr.Value[1:], 8, 64); err == nil {
					return x, nil
				} else {
					panic(err)
				}
			}

			//a basic literal integer is int type in default, or must have an explicit converting type in front
			if x, err := strconv.ParseInt(valueExpr.Value, 10, 64); err == nil {
				return int(x), nil
			} else if x, err := strconv.ParseUint(valueExpr.Value, 10, 64); err == nil {
				return x, nil
			} else {
				panic(err)
			}
		case token.STRING:
			if valueExpr.Value[0] == '`' {
				return valueExpr.Value[1 : len(valueExpr.Value)-1], nil
			}
			return EvaluateEscapedString(valueExpr.Value[1 : len(valueExpr.Value)-1]), nil
		case token.CHAR:
			return EvaluateEscapedChar(valueExpr.Value[1 : len(valueExpr.Value)-1]), nil
		}
	case *ast.UnaryExpr:
		x, evalType := pkg.evaluateConstValue(file, iota, valueExpr.X, globalEvaluator, recursiveStack)
		if x == nil {
			return x, evalType
		}
		return EvaluateUnary(x, valueExpr.Op, evalType)
	case *ast.BinaryExpr:
		x, evalTypex := pkg.evaluateConstValue(file, iota, valueExpr.X, globalEvaluator, recursiveStack)
		y, evalTypey := pkg.evaluateConstValue(file, iota, valueExpr.Y, globalEvaluator, recursiveStack)
		if x == nil || y == nil {
			return nil, nil
		}
		return EvaluateBinary(x, y, valueExpr.Op, evalTypex, evalTypey)
	case *ast.ParenExpr:
		return pkg.evaluateConstValue(file, iota, valueExpr.X, globalEvaluator, recursiveStack)
	case *ast.CallExpr:
		//data conversion
		if len(valueExpr.Args) != 1 {
			return nil, nil
		}
		arg := valueExpr.Args[0]
		if ident, ok := valueExpr.Fun.(*ast.Ident); ok {
			name := ident.Name
			if name == "uintptr" {
				name = "uint"
			}
			if IsGolangPrimitiveType(name) {
				value, _ := pkg.evaluateConstValue(file, iota, arg, globalEvaluator, recursiveStack)
				value = EvaluateDataConversion(value, name)
				return value, nil
			} else if name == "len" {
				value, _ := pkg.evaluateConstValue(file, iota, arg, globalEvaluator, recursiveStack)
				return reflect.ValueOf(value).Len(), nil
			}
			typeDef := globalEvaluator.FindTypeSpec(name, file)
			if typeDef == nil {
				return nil, nil
			}
			return arg, valueExpr.Fun
		} else if selector, ok := valueExpr.Fun.(*ast.SelectorExpr); ok {
			typeDef := globalEvaluator.FindTypeSpec(fullTypeName(selector.X.(*ast.Ident).Name, selector.Sel.Name), file)
			if typeDef == nil {
				return nil, nil
			}
			return arg, typeDef.TypeSpec.Type
		}
	}
	return nil, nil
}
