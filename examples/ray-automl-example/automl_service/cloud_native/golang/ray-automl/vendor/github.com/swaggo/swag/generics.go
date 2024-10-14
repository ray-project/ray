//go:build go1.18
// +build go1.18

package swag

import (
	"errors"
	"fmt"
	"go/ast"
	"strings"
	"unicode"

	"github.com/go-openapi/spec"
)

type genericTypeSpec struct {
	ArrayDepth int
	TypeSpec   *TypeSpecDef
	Name       string
}

type formalParamType struct {
	Name string
	Type string
}

func (t *genericTypeSpec) TypeName() string {
	if t.TypeSpec != nil {
		return t.TypeSpec.TypeName()
	}
	return t.Name
}

func (pkgDefs *PackagesDefinitions) parametrizeGenericType(file *ast.File, original *TypeSpecDef, fullGenericForm string) *TypeSpecDef {
	if original == nil || original.TypeSpec.TypeParams == nil || len(original.TypeSpec.TypeParams.List) == 0 {
		return original
	}

	name, genericParams := splitGenericsTypeName(fullGenericForm)
	if genericParams == nil {
		return nil
	}

	//generic[x,y any,z any] considered, TODO what if the type is not `any`, but a concrete one, such as `int32|int64` or an certain interface{}
	var formals []formalParamType
	for _, field := range original.TypeSpec.TypeParams.List {
		for _, ident := range field.Names {
			formal := formalParamType{Name: ident.Name}
			if ident, ok := field.Type.(*ast.Ident); ok {
				formal.Type = ident.Name
			}
			formals = append(formals, formal)
		}
	}
	if len(genericParams) != len(formals) {
		return nil
	}
	genericParamTypeDefs := map[string]*genericTypeSpec{}

	for i, genericParam := range genericParams {
		arrayDepth := 0
		for {
			if len(genericParam) <= 2 || genericParam[:2] != "[]" {
				break
			}
			genericParam = genericParam[2:]
			arrayDepth++
		}

		typeDef := pkgDefs.FindTypeSpec(genericParam, file)
		if typeDef != nil {
			genericParam = typeDef.TypeName()
			if _, ok := pkgDefs.uniqueDefinitions[genericParam]; !ok {
				pkgDefs.uniqueDefinitions[genericParam] = typeDef
			}
		}

		genericParamTypeDefs[formals[i].Name] = &genericTypeSpec{
			ArrayDepth: arrayDepth,
			TypeSpec:   typeDef,
			Name:       genericParam,
		}
	}

	name = fmt.Sprintf("%s%s-", string(IgnoreNameOverridePrefix), original.TypeName())
	var nameParts []string
	for _, def := range formals {
		if specDef, ok := genericParamTypeDefs[def.Name]; ok {
			var prefix = ""
			if specDef.ArrayDepth == 1 {
				prefix = "array_"
			} else if specDef.ArrayDepth > 1 {
				prefix = fmt.Sprintf("array%d_", specDef.ArrayDepth)
			}
			nameParts = append(nameParts, prefix+specDef.TypeName())
		}
	}

	name += strings.Replace(strings.Join(nameParts, "-"), ".", "_", -1)

	if typeSpec, ok := pkgDefs.uniqueDefinitions[name]; ok {
		return typeSpec
	}

	parametrizedTypeSpec := &TypeSpecDef{
		File:    original.File,
		PkgPath: original.PkgPath,
		TypeSpec: &ast.TypeSpec{
			Name: &ast.Ident{
				Name:    name,
				NamePos: original.TypeSpec.Name.NamePos,
				Obj:     original.TypeSpec.Name.Obj,
			},
			Doc:    original.TypeSpec.Doc,
			Assign: original.TypeSpec.Assign,
		},
	}
	pkgDefs.uniqueDefinitions[name] = parametrizedTypeSpec

	parametrizedTypeSpec.TypeSpec.Type = pkgDefs.resolveGenericType(original.File, original.TypeSpec.Type, genericParamTypeDefs)

	return parametrizedTypeSpec
}

// splitGenericsTypeName splits a generic struct name in his parts
func splitGenericsTypeName(fullGenericForm string) (string, []string) {
	//remove all spaces character
	fullGenericForm = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, fullGenericForm)

	// split only at the first '[' and remove the last ']'
	if fullGenericForm[len(fullGenericForm)-1] != ']' {
		return "", nil
	}

	genericParams := strings.SplitN(fullGenericForm[:len(fullGenericForm)-1], "[", 2)
	if len(genericParams) == 1 {
		return "", nil
	}

	// generic type name
	genericTypeName := genericParams[0]

	depth := 0
	genericParams = strings.FieldsFunc(genericParams[1], func(r rune) bool {
		if r == '[' {
			depth++
		} else if r == ']' {
			depth--
		} else if r == ',' && depth == 0 {
			return true
		}
		return false
	})
	if depth != 0 {
		return "", nil
	}

	return genericTypeName, genericParams
}

func (pkgDefs *PackagesDefinitions) getParametrizedType(genTypeSpec *genericTypeSpec) ast.Expr {
	if genTypeSpec.TypeSpec != nil && strings.Contains(genTypeSpec.Name, ".") {
		parts := strings.SplitN(genTypeSpec.Name, ".", 2)
		return &ast.SelectorExpr{
			X:   &ast.Ident{Name: parts[0]},
			Sel: &ast.Ident{Name: parts[1]},
		}
	}

	//a primitive type name or a type name in current package
	return &ast.Ident{Name: genTypeSpec.Name}
}

func (pkgDefs *PackagesDefinitions) resolveGenericType(file *ast.File, expr ast.Expr, genericParamTypeDefs map[string]*genericTypeSpec) ast.Expr {
	switch astExpr := expr.(type) {
	case *ast.Ident:
		if genTypeSpec, ok := genericParamTypeDefs[astExpr.Name]; ok {
			retType := pkgDefs.getParametrizedType(genTypeSpec)
			for i := 0; i < genTypeSpec.ArrayDepth; i++ {
				retType = &ast.ArrayType{Elt: retType}
			}
			return retType
		}
	case *ast.ArrayType:
		return &ast.ArrayType{
			Elt:    pkgDefs.resolveGenericType(file, astExpr.Elt, genericParamTypeDefs),
			Len:    astExpr.Len,
			Lbrack: astExpr.Lbrack,
		}
	case *ast.MapType:
		return &ast.MapType{
			Map:   astExpr.Map,
			Key:   pkgDefs.resolveGenericType(file, astExpr.Key, genericParamTypeDefs),
			Value: pkgDefs.resolveGenericType(file, astExpr.Value, genericParamTypeDefs),
		}
	case *ast.StarExpr:
		return &ast.StarExpr{
			Star: astExpr.Star,
			X:    pkgDefs.resolveGenericType(file, astExpr.X, genericParamTypeDefs),
		}
	case *ast.IndexExpr, *ast.IndexListExpr:
		fullGenericName, _ := getGenericFieldType(file, expr, genericParamTypeDefs)
		typeDef := pkgDefs.FindTypeSpec(fullGenericName, file)
		if typeDef != nil {
			return typeDef.TypeSpec.Name
		}
	case *ast.StructType:
		newStructTypeDef := &ast.StructType{
			Struct:     astExpr.Struct,
			Incomplete: astExpr.Incomplete,
			Fields: &ast.FieldList{
				Opening: astExpr.Fields.Opening,
				Closing: astExpr.Fields.Closing,
			},
		}

		for _, field := range astExpr.Fields.List {
			newField := &ast.Field{
				Type:    field.Type,
				Doc:     field.Doc,
				Names:   field.Names,
				Tag:     field.Tag,
				Comment: field.Comment,
			}

			newField.Type = pkgDefs.resolveGenericType(file, field.Type, genericParamTypeDefs)

			newStructTypeDef.Fields.List = append(newStructTypeDef.Fields.List, newField)
		}
		return newStructTypeDef
	}
	return expr
}

func getExtendedGenericFieldType(file *ast.File, field ast.Expr, genericParamTypeDefs map[string]*genericTypeSpec) (string, error) {
	switch fieldType := field.(type) {
	case *ast.ArrayType:
		fieldName, err := getExtendedGenericFieldType(file, fieldType.Elt, genericParamTypeDefs)
		return "[]" + fieldName, err
	case *ast.StarExpr:
		return getExtendedGenericFieldType(file, fieldType.X, genericParamTypeDefs)
	case *ast.Ident:
		if genericParamTypeDefs != nil {
			if typeSpec, ok := genericParamTypeDefs[fieldType.Name]; ok {
				return typeSpec.Name, nil
			}
		}
		if fieldType.Obj == nil {
			return fieldType.Name, nil
		}

		tSpec := &TypeSpecDef{
			File:     file,
			TypeSpec: fieldType.Obj.Decl.(*ast.TypeSpec),
			PkgPath:  file.Name.Name,
		}
		return tSpec.TypeName(), nil
	default:
		return getFieldType(file, field, genericParamTypeDefs)
	}
}

func getGenericFieldType(file *ast.File, field ast.Expr, genericParamTypeDefs map[string]*genericTypeSpec) (string, error) {
	var fullName string
	var baseName string
	var err error
	switch fieldType := field.(type) {
	case *ast.IndexListExpr:
		baseName, err = getGenericTypeName(file, fieldType.X)
		if err != nil {
			return "", err
		}
		fullName = baseName + "["

		for _, index := range fieldType.Indices {
			fieldName, err := getExtendedGenericFieldType(file, index, genericParamTypeDefs)
			if err != nil {
				return "", err
			}

			fullName += fieldName + ","
		}

		fullName = strings.TrimRight(fullName, ",") + "]"
	case *ast.IndexExpr:
		baseName, err = getGenericTypeName(file, fieldType.X)
		if err != nil {
			return "", err
		}

		indexName, err := getExtendedGenericFieldType(file, fieldType.Index, genericParamTypeDefs)
		if err != nil {
			return "", err
		}

		fullName = fmt.Sprintf("%s[%s]", baseName, indexName)
	}

	if fullName == "" {
		return "", fmt.Errorf("unknown field type %#v", field)
	}

	var packageName string
	if !strings.Contains(baseName, ".") {
		if file.Name == nil {
			return "", errors.New("file name is nil")
		}
		packageName, _ = getFieldType(file, file.Name, genericParamTypeDefs)
	}

	return strings.TrimLeft(fmt.Sprintf("%s.%s", packageName, fullName), "."), nil
}

func getGenericTypeName(file *ast.File, field ast.Expr) (string, error) {
	switch fieldType := field.(type) {
	case *ast.Ident:
		if fieldType.Obj == nil {
			return fieldType.Name, nil
		}

		tSpec := &TypeSpecDef{
			File:     file,
			TypeSpec: fieldType.Obj.Decl.(*ast.TypeSpec),
			PkgPath:  file.Name.Name,
		}
		return tSpec.TypeName(), nil
	case *ast.ArrayType:
		tSpec := &TypeSpecDef{
			File:     file,
			TypeSpec: fieldType.Elt.(*ast.Ident).Obj.Decl.(*ast.TypeSpec),
			PkgPath:  file.Name.Name,
		}
		return tSpec.TypeName(), nil
	case *ast.SelectorExpr:
		return fmt.Sprintf("%s.%s", fieldType.X.(*ast.Ident).Name, fieldType.Sel.Name), nil
	}
	return "", fmt.Errorf("unknown type %#v", field)
}

func (parser *Parser) parseGenericTypeExpr(file *ast.File, typeExpr ast.Expr) (*spec.Schema, error) {
	switch expr := typeExpr.(type) {
	// suppress debug messages for these types
	case *ast.InterfaceType:
	case *ast.StructType:
	case *ast.Ident:
	case *ast.StarExpr:
	case *ast.SelectorExpr:
	case *ast.ArrayType:
	case *ast.MapType:
	case *ast.FuncType:
	case *ast.IndexExpr, *ast.IndexListExpr:
		name, err := getExtendedGenericFieldType(file, expr, nil)
		if err == nil {
			if schema, err := parser.getTypeSchema(name, file, false); err == nil {
				return schema, nil
			}
		}

		parser.debug.Printf("Type definition of type '%T' is not supported yet. Using 'object' instead. (%s)\n", typeExpr, err)
	default:
		parser.debug.Printf("Type definition of type '%T' is not supported yet. Using 'object' instead.\n", typeExpr)
	}

	return PrimitiveSchema(OBJECT), nil
}
