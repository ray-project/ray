package swag

import (
	"encoding/json"
	"fmt"
	"go/ast"
	goparser "go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-openapi/spec"
	"golang.org/x/tools/go/loader"
)

// RouteProperties describes HTTP properties of a single router comment.
type RouteProperties struct {
	HTTPMethod string
	Path       string
}

// Operation describes a single API operation on a path.
// For more information: https://github.com/swaggo/swag#api-operation
type Operation struct {
	parser              *Parser
	codeExampleFilesDir string
	spec.Operation
	RouterProperties []RouteProperties
}

var mimeTypeAliases = map[string]string{
	"json":                  "application/json",
	"xml":                   "text/xml",
	"plain":                 "text/plain",
	"html":                  "text/html",
	"mpfd":                  "multipart/form-data",
	"x-www-form-urlencoded": "application/x-www-form-urlencoded",
	"json-api":              "application/vnd.api+json",
	"json-stream":           "application/x-json-stream",
	"octet-stream":          "application/octet-stream",
	"png":                   "image/png",
	"jpeg":                  "image/jpeg",
	"gif":                   "image/gif",
}

var mimeTypePattern = regexp.MustCompile("^[^/]+/[^/]+$")

// NewOperation creates a new Operation with default properties.
// map[int]Response.
func NewOperation(parser *Parser, options ...func(*Operation)) *Operation {
	if parser == nil {
		parser = New()
	}

	result := &Operation{
		parser:           parser,
		RouterProperties: []RouteProperties{},
		Operation: spec.Operation{
			OperationProps: spec.OperationProps{
				ID:           "",
				Description:  "",
				Summary:      "",
				Security:     nil,
				ExternalDocs: nil,
				Deprecated:   false,
				Tags:         []string{},
				Consumes:     []string{},
				Produces:     []string{},
				Schemes:      []string{},
				Parameters:   []spec.Parameter{},
				Responses: &spec.Responses{
					VendorExtensible: spec.VendorExtensible{
						Extensions: spec.Extensions{},
					},
					ResponsesProps: spec.ResponsesProps{
						Default:             nil,
						StatusCodeResponses: make(map[int]spec.Response),
					},
				},
			},
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{},
			},
		},
		codeExampleFilesDir: "",
	}

	for _, option := range options {
		option(result)
	}

	return result
}

// SetCodeExampleFilesDirectory sets the directory to search for codeExamples.
func SetCodeExampleFilesDirectory(directoryPath string) func(*Operation) {
	return func(o *Operation) {
		o.codeExampleFilesDir = directoryPath
	}
}

// ParseComment parses comment for given comment string and returns error if error occurs.
func (operation *Operation) ParseComment(comment string, astFile *ast.File) error {
	commentLine := strings.TrimSpace(strings.TrimLeft(comment, "/"))
	if len(commentLine) == 0 {
		return nil
	}

	fields := FieldsByAnySpace(commentLine, 2)
	attribute := fields[0]
	lowerAttribute := strings.ToLower(attribute)
	var lineRemainder string
	if len(fields) > 1 {
		lineRemainder = fields[1]
	}
	switch lowerAttribute {
	case descriptionAttr:
		operation.ParseDescriptionComment(lineRemainder)
	case descriptionMarkdownAttr:
		commentInfo, err := getMarkdownForTag(lineRemainder, operation.parser.markdownFileDir)
		if err != nil {
			return err
		}

		operation.ParseDescriptionComment(string(commentInfo))
	case summaryAttr:
		operation.Summary = lineRemainder
	case idAttr:
		operation.ID = lineRemainder
	case tagsAttr:
		operation.ParseTagsComment(lineRemainder)
	case acceptAttr:
		return operation.ParseAcceptComment(lineRemainder)
	case produceAttr:
		return operation.ParseProduceComment(lineRemainder)
	case paramAttr:
		return operation.ParseParamComment(lineRemainder, astFile)
	case successAttr, failureAttr, responseAttr:
		return operation.ParseResponseComment(lineRemainder, astFile)
	case headerAttr:
		return operation.ParseResponseHeaderComment(lineRemainder, astFile)
	case routerAttr:
		return operation.ParseRouterComment(lineRemainder)
	case securityAttr:
		return operation.ParseSecurityComment(lineRemainder)
	case deprecatedAttr:
		operation.Deprecate()
	case xCodeSamplesAttr:
		return operation.ParseCodeSample(attribute, commentLine, lineRemainder)
	default:
		return operation.ParseMetadata(attribute, lowerAttribute, lineRemainder)
	}

	return nil
}

// ParseCodeSample godoc.
func (operation *Operation) ParseCodeSample(attribute, _, lineRemainder string) error {
	if lineRemainder == "file" {
		data, err := getCodeExampleForSummary(operation.Summary, operation.codeExampleFilesDir)
		if err != nil {
			return err
		}

		var valueJSON interface{}

		err = json.Unmarshal(data, &valueJSON)
		if err != nil {
			return fmt.Errorf("annotation %s need a valid json value", attribute)
		}

		// don't use the method provided by spec lib, because it will call toLower() on attribute names, which is wrongly
		operation.Extensions[attribute[1:]] = valueJSON

		return nil
	}

	// Fallback into existing logic
	return operation.ParseMetadata(attribute, strings.ToLower(attribute), lineRemainder)
}

// ParseDescriptionComment godoc.
func (operation *Operation) ParseDescriptionComment(lineRemainder string) {
	if operation.Description == "" {
		operation.Description = lineRemainder

		return
	}

	operation.Description += "\n" + lineRemainder
}

// ParseMetadata godoc.
func (operation *Operation) ParseMetadata(attribute, lowerAttribute, lineRemainder string) error {
	// parsing specific meta data extensions
	if strings.HasPrefix(lowerAttribute, "@x-") {
		if len(lineRemainder) == 0 {
			return fmt.Errorf("annotation %s need a value", attribute)
		}

		var valueJSON interface{}

		err := json.Unmarshal([]byte(lineRemainder), &valueJSON)
		if err != nil {
			return fmt.Errorf("annotation %s need a valid json value", attribute)
		}

		// don't use the method provided by spec lib, because it will call toLower() on attribute names, which is wrongly
		operation.Extensions[attribute[1:]] = valueJSON
	}

	return nil
}

var paramPattern = regexp.MustCompile(`(\S+)\s+(\w+)\s+([\S. ]+?)\s+(\w+)\s+"([^"]+)"`)

func findInSlice(arr []string, target string) bool {
	for _, str := range arr {
		if str == target {
			return true
		}
	}

	return false
}

// ParseParamComment parses params return []string of param properties
// E.g. @Param	queryText		formData	      string	  true		        "The email for login"
//
//	[param name]    [paramType] [data type]  [is mandatory?]   [Comment]
//
// E.g. @Param   some_id     path    int     true        "Some ID".
func (operation *Operation) ParseParamComment(commentLine string, astFile *ast.File) error {
	matches := paramPattern.FindStringSubmatch(commentLine)
	if len(matches) != 6 {
		return fmt.Errorf("missing required param comment parameters \"%s\"", commentLine)
	}

	name := matches[1]
	paramType := matches[2]
	refType := TransToValidSchemeType(matches[3])

	// Detect refType
	objectType := OBJECT

	if strings.HasPrefix(refType, "[]") {
		objectType = ARRAY
		refType = strings.TrimPrefix(refType, "[]")
		refType = TransToValidSchemeType(refType)
	} else if IsPrimitiveType(refType) ||
		paramType == "formData" && refType == "file" {
		objectType = PRIMITIVE
	}

	var enums []interface{}
	if !IsPrimitiveType(refType) {
		schema, _ := operation.parser.getTypeSchema(refType, astFile, false)
		if schema != nil && len(schema.Type) == 1 && schema.Enum != nil {
			if objectType == OBJECT {
				objectType = PRIMITIVE
			}
			refType = TransToValidSchemeType(schema.Type[0])
			enums = schema.Enum
		}
	}

	requiredText := strings.ToLower(matches[4])
	required := requiredText == "true" || requiredText == requiredLabel
	description := matches[5]

	param := createParameter(paramType, description, name, objectType, refType, required, enums, operation.parser.collectionFormatInQuery)

	switch paramType {
	case "path", "header":
		switch objectType {
		case ARRAY:
			if !IsPrimitiveType(refType) {
				return fmt.Errorf("%s is not supported array type for %s", refType, paramType)
			}
		case OBJECT:
			return fmt.Errorf("%s is not supported type for %s", refType, paramType)
		}
	case "query", "formData":
		switch objectType {
		case ARRAY:
			if !IsPrimitiveType(refType) && !(refType == "file" && paramType == "formData") {
				return fmt.Errorf("%s is not supported array type for %s", refType, paramType)
			}
		case PRIMITIVE:
			break
		case OBJECT:
			schema, err := operation.parser.getTypeSchema(refType, astFile, false)
			if err != nil {
				return err
			}

			if len(schema.Properties) == 0 {
				return nil
			}

			items := schema.Properties.ToOrderedSchemaItems()

			for _, item := range items {
				name, prop := item.Name, &item.Schema
				if len(prop.Type) == 0 {
					prop = operation.parser.getUnderlyingSchema(prop)
					if len(prop.Type) == 0 {
						continue
					}
				}

				var formName = name
				if item.Schema.Extensions != nil {
					if nameVal, ok := item.Schema.Extensions[formTag]; ok {
						formName = nameVal.(string)
					}
				}

				switch {
				case prop.Type[0] == ARRAY && prop.Items.Schema != nil &&
					len(prop.Items.Schema.Type) > 0 && IsSimplePrimitiveType(prop.Items.Schema.Type[0]):

					param = createParameter(paramType, prop.Description, formName, prop.Type[0], prop.Items.Schema.Type[0], findInSlice(schema.Required, name), enums, operation.parser.collectionFormatInQuery)

				case IsSimplePrimitiveType(prop.Type[0]):
					param = createParameter(paramType, prop.Description, formName, PRIMITIVE, prop.Type[0], findInSlice(schema.Required, name), enums, operation.parser.collectionFormatInQuery)
				default:
					operation.parser.debug.Printf("skip field [%s] in %s is not supported type for %s", name, refType, paramType)

					continue
				}

				param.Nullable = prop.Nullable
				param.Format = prop.Format
				param.Default = prop.Default
				param.Example = prop.Example
				param.Extensions = prop.Extensions
				param.CommonValidations.Maximum = prop.Maximum
				param.CommonValidations.Minimum = prop.Minimum
				param.CommonValidations.ExclusiveMaximum = prop.ExclusiveMaximum
				param.CommonValidations.ExclusiveMinimum = prop.ExclusiveMinimum
				param.CommonValidations.MaxLength = prop.MaxLength
				param.CommonValidations.MinLength = prop.MinLength
				param.CommonValidations.Pattern = prop.Pattern
				param.CommonValidations.MaxItems = prop.MaxItems
				param.CommonValidations.MinItems = prop.MinItems
				param.CommonValidations.UniqueItems = prop.UniqueItems
				param.CommonValidations.MultipleOf = prop.MultipleOf
				param.CommonValidations.Enum = prop.Enum
				operation.Operation.Parameters = append(operation.Operation.Parameters, param)
			}

			return nil
		}
	case "body":
		if objectType == PRIMITIVE {
			param.Schema = PrimitiveSchema(refType)
		} else {
			schema, err := operation.parseAPIObjectSchema(commentLine, objectType, refType, astFile)
			if err != nil {
				return err
			}

			param.Schema = schema
		}
	default:
		return fmt.Errorf("%s is not supported paramType", paramType)
	}

	err := operation.parseParamAttribute(commentLine, objectType, refType, &param)
	if err != nil {
		return err
	}

	operation.Operation.Parameters = append(operation.Operation.Parameters, param)

	return nil
}

const (
	formTag             = "form"
	jsonTag             = "json"
	bindingTag          = "binding"
	defaultTag          = "default"
	enumsTag            = "enums"
	exampleTag          = "example"
	schemaExampleTag    = "schemaExample"
	formatTag           = "format"
	validateTag         = "validate"
	minimumTag          = "minimum"
	maximumTag          = "maximum"
	minLengthTag        = "minLength"
	maxLengthTag        = "maxLength"
	multipleOfTag       = "multipleOf"
	readOnlyTag         = "readonly"
	extensionsTag       = "extensions"
	collectionFormatTag = "collectionFormat"
)

var regexAttributes = map[string]*regexp.Regexp{
	// for Enums(A, B)
	enumsTag: regexp.MustCompile(`(?i)\s+enums\(.*\)`),
	// for maximum(0)
	maximumTag: regexp.MustCompile(`(?i)\s+maxinum|maximum\(.*\)`),
	// for minimum(0)
	minimumTag: regexp.MustCompile(`(?i)\s+mininum|minimum\(.*\)`),
	// for default(0)
	defaultTag: regexp.MustCompile(`(?i)\s+default\(.*\)`),
	// for minlength(0)
	minLengthTag: regexp.MustCompile(`(?i)\s+minlength\(.*\)`),
	// for maxlength(0)
	maxLengthTag: regexp.MustCompile(`(?i)\s+maxlength\(.*\)`),
	// for format(email)
	formatTag: regexp.MustCompile(`(?i)\s+format\(.*\)`),
	// for extensions(x-example=test)
	extensionsTag: regexp.MustCompile(`(?i)\s+extensions\(.*\)`),
	// for collectionFormat(csv)
	collectionFormatTag: regexp.MustCompile(`(?i)\s+collectionFormat\(.*\)`),
	// example(0)
	exampleTag: regexp.MustCompile(`(?i)\s+example\(.*\)`),
	// schemaExample(0)
	schemaExampleTag: regexp.MustCompile(`(?i)\s+schemaExample\(.*\)`),
}

func (operation *Operation) parseParamAttribute(comment, objectType, schemaType string, param *spec.Parameter) error {
	schemaType = TransToValidSchemeType(schemaType)

	for attrKey, re := range regexAttributes {
		attr, err := findAttr(re, comment)
		if err != nil {
			continue
		}

		switch attrKey {
		case enumsTag:
			err = setEnumParam(param, attr, objectType, schemaType)
		case minimumTag, maximumTag:
			err = setNumberParam(param, attrKey, schemaType, attr, comment)
		case defaultTag:
			err = setDefault(param, schemaType, attr)
		case minLengthTag, maxLengthTag:
			err = setStringParam(param, attrKey, schemaType, attr, comment)
		case formatTag:
			param.Format = attr
		case exampleTag:
			err = setExample(param, schemaType, attr)
		case schemaExampleTag:
			err = setSchemaExample(param, schemaType, attr)
		case extensionsTag:
			param.Extensions = setExtensionParam(attr)
		case collectionFormatTag:
			err = setCollectionFormatParam(param, attrKey, objectType, attr, comment)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func findAttr(re *regexp.Regexp, commentLine string) (string, error) {
	attr := re.FindString(commentLine)

	l, r := strings.Index(attr, "("), strings.Index(attr, ")")
	if l == -1 || r == -1 {
		return "", fmt.Errorf("can not find regex=%s, comment=%s", re.String(), commentLine)
	}

	return strings.TrimSpace(attr[l+1 : r]), nil
}

func setStringParam(param *spec.Parameter, name, schemaType, attr, commentLine string) error {
	if schemaType != STRING {
		return fmt.Errorf("%s is attribute to set to a number. comment=%s got=%s", name, commentLine, schemaType)
	}

	n, err := strconv.ParseInt(attr, 10, 64)
	if err != nil {
		return fmt.Errorf("%s is allow only a number got=%s", name, attr)
	}

	switch name {
	case minLengthTag:
		param.MinLength = &n
	case maxLengthTag:
		param.MaxLength = &n
	}

	return nil
}

func setNumberParam(param *spec.Parameter, name, schemaType, attr, commentLine string) error {
	switch schemaType {
	case INTEGER, NUMBER:
		n, err := strconv.ParseFloat(attr, 64)
		if err != nil {
			return fmt.Errorf("maximum is allow only a number. comment=%s got=%s", commentLine, attr)
		}

		switch name {
		case minimumTag:
			param.Minimum = &n
		case maximumTag:
			param.Maximum = &n
		}

		return nil
	default:
		return fmt.Errorf("%s is attribute to set to a number. comment=%s got=%s", name, commentLine, schemaType)
	}
}

func setEnumParam(param *spec.Parameter, attr, objectType, schemaType string) error {
	for _, e := range strings.Split(attr, ",") {
		e = strings.TrimSpace(e)

		value, err := defineType(schemaType, e)
		if err != nil {
			return err
		}

		switch objectType {
		case ARRAY:
			param.Items.Enum = append(param.Items.Enum, value)
		default:
			param.Enum = append(param.Enum, value)
		}
	}

	return nil
}

func setExtensionParam(attr string) spec.Extensions {
	extensions := spec.Extensions{}

	for _, val := range splitNotWrapped(attr, ',') {
		parts := strings.SplitN(val, "=", 2)
		if len(parts) == 2 {
			extensions.Add(parts[0], parts[1])

			continue
		}

		if len(parts[0]) > 0 && string(parts[0][0]) == "!" {
			extensions.Add(parts[0][1:], false)

			continue
		}

		extensions.Add(parts[0], true)
	}

	return extensions
}

func setCollectionFormatParam(param *spec.Parameter, name, schemaType, attr, commentLine string) error {
	if schemaType == ARRAY {
		param.CollectionFormat = TransToValidCollectionFormat(attr)

		return nil
	}

	return fmt.Errorf("%s is attribute to set to an array. comment=%s got=%s", name, commentLine, schemaType)
}

func setDefault(param *spec.Parameter, schemaType string, value string) error {
	val, err := defineType(schemaType, value)
	if err != nil {
		return nil // Don't set a default value if it's not valid
	}

	param.Default = val

	return nil
}

func setSchemaExample(param *spec.Parameter, schemaType string, value string) error {
	val, err := defineType(schemaType, value)
	if err != nil {
		return nil // Don't set a example value if it's not valid
	}
	// skip schema
	if param.Schema == nil {
		return nil
	}

	switch v := val.(type) {
	case string:
		//  replaces \r \n \t in example string values.
		param.Schema.Example = strings.NewReplacer(`\r`, "\r", `\n`, "\n", `\t`, "\t").Replace(v)
	default:
		param.Schema.Example = val
	}

	return nil
}

func setExample(param *spec.Parameter, schemaType string, value string) error {
	val, err := defineType(schemaType, value)
	if err != nil {
		return nil // Don't set a example value if it's not valid
	}

	param.Example = val

	return nil
}

// defineType enum value define the type (object and array unsupported).
func defineType(schemaType string, value string) (v interface{}, err error) {
	schemaType = TransToValidSchemeType(schemaType)

	switch schemaType {
	case STRING:
		return value, nil
	case NUMBER:
		v, err = strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("enum value %s can't convert to %s err: %s", value, schemaType, err)
		}
	case INTEGER:
		v, err = strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("enum value %s can't convert to %s err: %s", value, schemaType, err)
		}
	case BOOLEAN:
		v, err = strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("enum value %s can't convert to %s err: %s", value, schemaType, err)
		}
	default:
		return nil, fmt.Errorf("%s is unsupported type in enum value %s", schemaType, value)
	}

	return v, nil
}

// ParseTagsComment parses comment for given `tag` comment string.
func (operation *Operation) ParseTagsComment(commentLine string) {
	for _, tag := range strings.Split(commentLine, ",") {
		operation.Tags = append(operation.Tags, strings.TrimSpace(tag))
	}
}

// ParseAcceptComment parses comment for given `accept` comment string.
func (operation *Operation) ParseAcceptComment(commentLine string) error {
	return parseMimeTypeList(commentLine, &operation.Consumes, "%v accept type can't be accepted")
}

// ParseProduceComment parses comment for given `produce` comment string.
func (operation *Operation) ParseProduceComment(commentLine string) error {
	return parseMimeTypeList(commentLine, &operation.Produces, "%v produce type can't be accepted")
}

// parseMimeTypeList parses a list of MIME Types for a comment like
// `produce` (`Content-Type:` response header) or
// `accept` (`Accept:` request header).
func parseMimeTypeList(mimeTypeList string, typeList *[]string, format string) error {
	for _, typeName := range strings.Split(mimeTypeList, ",") {
		if mimeTypePattern.MatchString(typeName) {
			*typeList = append(*typeList, typeName)

			continue
		}

		aliasMimeType, ok := mimeTypeAliases[typeName]
		if !ok {
			return fmt.Errorf(format, typeName)
		}

		*typeList = append(*typeList, aliasMimeType)
	}

	return nil
}

var routerPattern = regexp.MustCompile(`^(/[\w./\-{}+:$]*)[[:blank:]]+\[(\w+)]`)

// ParseRouterComment parses comment for given `router` comment string.
func (operation *Operation) ParseRouterComment(commentLine string) error {
	matches := routerPattern.FindStringSubmatch(commentLine)
	if len(matches) != 3 {
		return fmt.Errorf("can not parse router comment \"%s\"", commentLine)
	}

	signature := RouteProperties{
		Path:       matches[1],
		HTTPMethod: strings.ToUpper(matches[2]),
	}

	if _, ok := allMethod[signature.HTTPMethod]; !ok {
		return fmt.Errorf("invalid method: %s", signature.HTTPMethod)
	}

	operation.RouterProperties = append(operation.RouterProperties, signature)

	return nil
}

// ParseSecurityComment parses comment for given `security` comment string.
func (operation *Operation) ParseSecurityComment(commentLine string) error {
	var (
		securityMap    = make(map[string][]string)
		securitySource = commentLine[strings.Index(commentLine, "@Security")+1:]
	)

	for _, securityOption := range strings.Split(securitySource, "||") {
		securityOption = strings.TrimSpace(securityOption)

		left, right := strings.Index(securityOption, "["), strings.Index(securityOption, "]")

		if !(left == -1 && right == -1) {
			scopes := securityOption[left+1 : right]

			var options []string

			for _, scope := range strings.Split(scopes, ",") {
				options = append(options, strings.TrimSpace(scope))
			}

			securityKey := securityOption[0:left]
			securityMap[securityKey] = append(securityMap[securityKey], options...)
		} else {
			securityKey := strings.TrimSpace(securityOption)
			securityMap[securityKey] = []string{}
		}
	}

	operation.Security = append(operation.Security, securityMap)

	return nil
}

// findTypeDef attempts to find the *ast.TypeSpec for a specific type given the
// type's name and the package's import path.
// TODO: improve finding external pkg.
func findTypeDef(importPath, typeName string) (*ast.TypeSpec, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	conf := loader.Config{
		ParserMode: goparser.SpuriousErrors,
		Cwd:        cwd,
	}

	conf.Import(importPath)

	lprog, err := conf.Load()
	if err != nil {
		return nil, err
	}

	// If the pkg is vendored, the actual pkg path is going to resemble
	// something like "{importPath}/vendor/{importPath}"
	for k := range lprog.AllPackages {
		realPkgPath := k.Path()

		if strings.Contains(realPkgPath, "vendor/"+importPath) {
			importPath = realPkgPath
		}
	}

	pkgInfo := lprog.Package(importPath)

	if pkgInfo == nil {
		return nil, fmt.Errorf("package was nil")
	}

	// TODO: possibly cache pkgInfo since it's an expensive operation
	for i := range pkgInfo.Files {
		for _, astDeclaration := range pkgInfo.Files[i].Decls {
			generalDeclaration, ok := astDeclaration.(*ast.GenDecl)
			if ok && generalDeclaration.Tok == token.TYPE {
				for _, astSpec := range generalDeclaration.Specs {
					typeSpec, ok := astSpec.(*ast.TypeSpec)
					if ok {
						if typeSpec.Name.String() == typeName {
							return typeSpec, nil
						}
					}
				}
			}
		}
	}

	return nil, fmt.Errorf("type spec not found")
}

var responsePattern = regexp.MustCompile(`^([\w,]+)\s+([\w{}]+)\s+([\w\-.\\{}=,\[\s\]]+)\s*(".*)?`)

// ResponseType{data1=Type1,data2=Type2}.
var combinedPattern = regexp.MustCompile(`^([\w\-./\[\]]+){(.*)}$`)

func (operation *Operation) parseObjectSchema(refType string, astFile *ast.File) (*spec.Schema, error) {
	return parseObjectSchema(operation.parser, refType, astFile)
}

func parseObjectSchema(parser *Parser, refType string, astFile *ast.File) (*spec.Schema, error) {
	switch {
	case refType == NIL:
		return nil, nil
	case refType == INTERFACE:
		return PrimitiveSchema(OBJECT), nil
	case refType == ANY:
		return PrimitiveSchema(OBJECT), nil
	case IsGolangPrimitiveType(refType):
		refType = TransToValidSchemeType(refType)

		return PrimitiveSchema(refType), nil
	case IsPrimitiveType(refType):
		return PrimitiveSchema(refType), nil
	case strings.HasPrefix(refType, "[]"):
		schema, err := parseObjectSchema(parser, refType[2:], astFile)
		if err != nil {
			return nil, err
		}

		return spec.ArrayProperty(schema), nil
	case strings.HasPrefix(refType, "map["):
		// ignore key type
		idx := strings.Index(refType, "]")
		if idx < 0 {
			return nil, fmt.Errorf("invalid type: %s", refType)
		}

		refType = refType[idx+1:]
		if refType == INTERFACE || refType == ANY {
			return spec.MapProperty(nil), nil
		}

		schema, err := parseObjectSchema(parser, refType, astFile)
		if err != nil {
			return nil, err
		}

		return spec.MapProperty(schema), nil
	case strings.Contains(refType, "{"):
		return parseCombinedObjectSchema(parser, refType, astFile)
	default:
		if parser != nil { // checking refType has existing in 'TypeDefinitions'
			schema, err := parser.getTypeSchema(refType, astFile, true)
			if err != nil {
				return nil, err
			}

			return schema, nil
		}

		return RefSchema(refType), nil
	}
}

func parseFields(s string) []string {
	nestLevel := 0

	return strings.FieldsFunc(s, func(char rune) bool {
		if char == '{' {
			nestLevel++

			return false
		} else if char == '}' {
			nestLevel--

			return false
		}

		return char == ',' && nestLevel == 0
	})
}

func parseCombinedObjectSchema(parser *Parser, refType string, astFile *ast.File) (*spec.Schema, error) {
	matches := combinedPattern.FindStringSubmatch(refType)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid type: %s", refType)
	}

	schema, err := parseObjectSchema(parser, matches[1], astFile)
	if err != nil {
		return nil, err
	}

	fields, props := parseFields(matches[2]), map[string]spec.Schema{}

	for _, field := range fields {
		keyVal := strings.SplitN(field, "=", 2)
		if len(keyVal) == 2 {
			schema, err := parseObjectSchema(parser, keyVal[1], astFile)
			if err != nil {
				return nil, err
			}

			props[keyVal[0]] = *schema
		}
	}

	if len(props) == 0 {
		return schema, nil
	}

	if schema.Ref.GetURL() == nil && len(schema.Type) > 0 && schema.Type[0] == OBJECT && len(schema.Properties) == 0 && schema.AdditionalProperties == nil {
		schema.Properties = props
		return schema, nil
	}

	return spec.ComposedSchema(*schema, spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:       []string{OBJECT},
			Properties: props,
		},
	}), nil
}

func (operation *Operation) parseAPIObjectSchema(commentLine, schemaType, refType string, astFile *ast.File) (*spec.Schema, error) {
	if strings.HasSuffix(refType, ",") && strings.Contains(refType, "[") {
		// regexp may have broken generic syntax. find closing bracket and add it back
		allMatchesLenOffset := strings.Index(commentLine, refType) + len(refType)
		lostPartEndIdx := strings.Index(commentLine[allMatchesLenOffset:], "]")
		if lostPartEndIdx >= 0 {
			refType += commentLine[allMatchesLenOffset : allMatchesLenOffset+lostPartEndIdx+1]
		}
	}

	switch schemaType {
	case OBJECT:
		if !strings.HasPrefix(refType, "[]") {
			return operation.parseObjectSchema(refType, astFile)
		}

		refType = refType[2:]

		fallthrough
	case ARRAY:
		schema, err := operation.parseObjectSchema(refType, astFile)
		if err != nil {
			return nil, err
		}

		return spec.ArrayProperty(schema), nil
	default:
		return PrimitiveSchema(schemaType), nil
	}
}

// ParseResponseComment parses comment for given `response` comment string.
func (operation *Operation) ParseResponseComment(commentLine string, astFile *ast.File) error {
	matches := responsePattern.FindStringSubmatch(commentLine)
	if len(matches) != 5 {
		err := operation.ParseEmptyResponseComment(commentLine)
		if err != nil {
			return operation.ParseEmptyResponseOnly(commentLine)
		}

		return err
	}

	description := strings.Trim(matches[4], "\"")

	schema, err := operation.parseAPIObjectSchema(commentLine, strings.Trim(matches[2], "{}"), strings.TrimSpace(matches[3]), astFile)
	if err != nil {
		return err
	}

	for _, codeStr := range strings.Split(matches[1], ",") {
		if strings.EqualFold(codeStr, defaultTag) {
			operation.DefaultResponse().WithSchema(schema).WithDescription(description)

			continue
		}

		code, err := strconv.Atoi(codeStr)
		if err != nil {
			return fmt.Errorf("can not parse response comment \"%s\"", commentLine)
		}

		resp := spec.NewResponse().WithSchema(schema).WithDescription(description)
		if description == "" {
			resp.WithDescription(http.StatusText(code))
		}

		operation.AddResponse(code, resp)
	}

	return nil
}

func newHeaderSpec(schemaType, description string) spec.Header {
	return spec.Header{
		SimpleSchema: spec.SimpleSchema{
			Type: schemaType,
		},
		HeaderProps: spec.HeaderProps{
			Description: description,
		},
		VendorExtensible: spec.VendorExtensible{
			Extensions: nil,
		},
		CommonValidations: spec.CommonValidations{
			Maximum:          nil,
			ExclusiveMaximum: false,
			Minimum:          nil,
			ExclusiveMinimum: false,
			MaxLength:        nil,
			MinLength:        nil,
			Pattern:          "",
			MaxItems:         nil,
			MinItems:         nil,
			UniqueItems:      false,
			MultipleOf:       nil,
			Enum:             nil,
		},
	}
}

// ParseResponseHeaderComment parses comment for given `response header` comment string.
func (operation *Operation) ParseResponseHeaderComment(commentLine string, _ *ast.File) error {
	matches := responsePattern.FindStringSubmatch(commentLine)
	if len(matches) != 5 {
		return fmt.Errorf("can not parse response comment \"%s\"", commentLine)
	}

	header := newHeaderSpec(strings.Trim(matches[2], "{}"), strings.Trim(matches[4], "\""))

	headerKey := strings.TrimSpace(matches[3])

	if strings.EqualFold(matches[1], "all") {
		if operation.Responses.Default != nil {
			operation.Responses.Default.Headers[headerKey] = header
		}

		if operation.Responses.StatusCodeResponses != nil {
			for code, response := range operation.Responses.StatusCodeResponses {
				response.Headers[headerKey] = header
				operation.Responses.StatusCodeResponses[code] = response
			}
		}

		return nil
	}

	for _, codeStr := range strings.Split(matches[1], ",") {
		if strings.EqualFold(codeStr, defaultTag) {
			if operation.Responses.Default != nil {
				operation.Responses.Default.Headers[headerKey] = header
			}

			continue
		}

		code, err := strconv.Atoi(codeStr)
		if err != nil {
			return fmt.Errorf("can not parse response comment \"%s\"", commentLine)
		}

		if operation.Responses.StatusCodeResponses != nil {
			response, responseExist := operation.Responses.StatusCodeResponses[code]
			if responseExist {
				response.Headers[headerKey] = header

				operation.Responses.StatusCodeResponses[code] = response
			}
		}
	}

	return nil
}

var emptyResponsePattern = regexp.MustCompile(`([\w,]+)\s+"(.*)"`)

// ParseEmptyResponseComment parse only comment out status code and description,eg: @Success 200 "it's ok".
func (operation *Operation) ParseEmptyResponseComment(commentLine string) error {
	matches := emptyResponsePattern.FindStringSubmatch(commentLine)
	if len(matches) != 3 {
		return fmt.Errorf("can not parse response comment \"%s\"", commentLine)
	}

	description := strings.Trim(matches[2], "\"")

	for _, codeStr := range strings.Split(matches[1], ",") {
		if strings.EqualFold(codeStr, defaultTag) {
			operation.DefaultResponse().WithDescription(description)

			continue
		}

		code, err := strconv.Atoi(codeStr)
		if err != nil {
			return fmt.Errorf("can not parse response comment \"%s\"", commentLine)
		}

		operation.AddResponse(code, spec.NewResponse().WithDescription(description))
	}

	return nil
}

// ParseEmptyResponseOnly parse only comment out status code ,eg: @Success 200.
func (operation *Operation) ParseEmptyResponseOnly(commentLine string) error {
	for _, codeStr := range strings.Split(commentLine, ",") {
		if strings.EqualFold(codeStr, defaultTag) {
			_ = operation.DefaultResponse()

			continue
		}

		code, err := strconv.Atoi(codeStr)
		if err != nil {
			return fmt.Errorf("can not parse response comment \"%s\"", commentLine)
		}

		operation.AddResponse(code, spec.NewResponse().WithDescription(http.StatusText(code)))
	}

	return nil
}

// DefaultResponse return the default response member pointer.
func (operation *Operation) DefaultResponse() *spec.Response {
	if operation.Responses.Default == nil {
		operation.Responses.Default = &spec.Response{
			ResponseProps: spec.ResponseProps{
				Description: "",
				Headers:     make(map[string]spec.Header),
			},
		}
	}

	return operation.Responses.Default
}

// AddResponse add a response for a code.
func (operation *Operation) AddResponse(code int, response *spec.Response) {
	if response.Headers == nil {
		response.Headers = make(map[string]spec.Header)
	}

	operation.Responses.StatusCodeResponses[code] = *response
}

// createParameter returns swagger spec.Parameter for given  paramType, description, paramName, schemaType, required.
func createParameter(paramType, description, paramName, objectType, schemaType string, required bool, enums []interface{}, collectionFormat string) spec.Parameter {
	// //five possible parameter types. 	query, path, body, header, form
	result := spec.Parameter{
		ParamProps: spec.ParamProps{
			Name:        paramName,
			Description: description,
			Required:    required,
			In:          paramType,
		},
	}

	if paramType == "body" {
		return result
	}

	switch objectType {
	case ARRAY:
		result.Type = objectType
		result.CollectionFormat = collectionFormat
		result.Items = &spec.Items{
			CommonValidations: spec.CommonValidations{
				Enum: enums,
			},
			SimpleSchema: spec.SimpleSchema{
				Type: schemaType,
			},
		}
	case PRIMITIVE, OBJECT:
		result.Type = schemaType
		result.Enum = enums
	}
	return result
}

func getCodeExampleForSummary(summaryName string, dirPath string) ([]byte, error) {
	dirEntries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()

		if !strings.Contains(fileName, ".json") {
			continue
		}

		if strings.Contains(fileName, summaryName) {
			fullPath := filepath.Join(dirPath, fileName)

			commentInfo, err := os.ReadFile(fullPath)
			if err != nil {
				return nil, fmt.Errorf("Failed to read code example file %s error: %s ", fullPath, err)
			}

			return commentInfo, nil
		}
	}

	return nil, fmt.Errorf("unable to find code example file for tag %s in the given directory", summaryName)
}
