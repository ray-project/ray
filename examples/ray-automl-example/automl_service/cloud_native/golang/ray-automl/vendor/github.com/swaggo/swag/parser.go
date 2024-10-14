package swag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	goparser "go/parser"
	"go/token"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/KyleBanks/depth"
	"github.com/go-openapi/spec"
)

const (
	// CamelCase indicates using CamelCase strategy for struct field.
	CamelCase = "camelcase"

	// PascalCase indicates using PascalCase strategy for struct field.
	PascalCase = "pascalcase"

	// SnakeCase indicates using SnakeCase strategy for struct field.
	SnakeCase = "snakecase"

	idAttr                  = "@id"
	acceptAttr              = "@accept"
	produceAttr             = "@produce"
	paramAttr               = "@param"
	successAttr             = "@success"
	failureAttr             = "@failure"
	responseAttr            = "@response"
	headerAttr              = "@header"
	tagsAttr                = "@tags"
	routerAttr              = "@router"
	summaryAttr             = "@summary"
	deprecatedAttr          = "@deprecated"
	securityAttr            = "@security"
	titleAttr               = "@title"
	conNameAttr             = "@contact.name"
	conURLAttr              = "@contact.url"
	conEmailAttr            = "@contact.email"
	licNameAttr             = "@license.name"
	licURLAttr              = "@license.url"
	versionAttr             = "@version"
	descriptionAttr         = "@description"
	descriptionMarkdownAttr = "@description.markdown"
	secBasicAttr            = "@securitydefinitions.basic"
	secAPIKeyAttr           = "@securitydefinitions.apikey"
	secApplicationAttr      = "@securitydefinitions.oauth2.application"
	secImplicitAttr         = "@securitydefinitions.oauth2.implicit"
	secPasswordAttr         = "@securitydefinitions.oauth2.password"
	secAccessCodeAttr       = "@securitydefinitions.oauth2.accesscode"
	tosAttr                 = "@termsofservice"
	extDocsDescAttr         = "@externaldocs.description"
	extDocsURLAttr          = "@externaldocs.url"
	xCodeSamplesAttr        = "@x-codesamples"
	scopeAttrPrefix         = "@scope."
)

// ParseFlag determine what to parse
type ParseFlag int

const (
	// ParseNone parse nothing
	ParseNone ParseFlag = 0x00
	// ParseOperations parse operations
	ParseOperations = 0x01
	// ParseModels parse models
	ParseModels = 0x02
	// ParseAll parse operations and models
	ParseAll = ParseOperations | ParseModels
)

var (
	// ErrRecursiveParseStruct recursively parsing struct.
	ErrRecursiveParseStruct = errors.New("recursively parsing struct")

	// ErrFuncTypeField field type is func.
	ErrFuncTypeField = errors.New("field type is func")

	// ErrFailedConvertPrimitiveType Failed to convert for swag to interpretable type.
	ErrFailedConvertPrimitiveType = errors.New("swag property: failed convert primitive type")

	// ErrSkippedField .swaggo specifies field should be skipped.
	ErrSkippedField = errors.New("field is skipped by global overrides")
)

var allMethod = map[string]struct{}{
	http.MethodGet:     {},
	http.MethodPut:     {},
	http.MethodPost:    {},
	http.MethodDelete:  {},
	http.MethodOptions: {},
	http.MethodHead:    {},
	http.MethodPatch:   {},
}

// Parser implements a parser for Go source files.
type Parser struct {
	// swagger represents the root document object for the API specification
	swagger *spec.Swagger

	// packages store entities of APIs, definitions, file, package path etc.  and their relations
	packages *PackagesDefinitions

	// parsedSchemas store schemas which have been parsed from ast.TypeSpec
	parsedSchemas map[*TypeSpecDef]*Schema

	// outputSchemas store schemas which will be export to swagger
	outputSchemas map[*TypeSpecDef]*Schema

	// PropNamingStrategy naming strategy
	PropNamingStrategy string

	// ParseVendor parse vendor folder
	ParseVendor bool

	// ParseDependencies whether swag should be parse outside dependency folder
	ParseDependency bool

	// ParseInternal whether swag should parse internal packages
	ParseInternal bool

	// Strict whether swag should error or warn when it detects cases which are most likely user errors
	Strict bool

	// RequiredByDefault set validation required for all fields by default
	RequiredByDefault bool

	// structStack stores full names of the structures that were already parsed or are being parsed now
	structStack []*TypeSpecDef

	// markdownFileDir holds the path to the folder, where markdown files are stored
	markdownFileDir string

	// codeExampleFilesDir holds path to the folder, where code example files are stored
	codeExampleFilesDir string

	// collectionFormatInQuery set the default collectionFormat otherwise then 'csv' for array in query params
	collectionFormatInQuery string

	// excludes excludes dirs and files in SearchDir
	excludes map[string]struct{}

	// tells parser to include only specific extension
	parseExtension string

	// debugging output goes here
	debug Debugger

	// fieldParserFactory create FieldParser
	fieldParserFactory FieldParserFactory

	// Overrides allows global replacements of types. A blank replacement will be skipped.
	Overrides map[string]string

	// parseGoList whether swag use go list to parse dependency
	parseGoList bool

	// tags to filter the APIs after
	tags map[string]struct{}
}

// FieldParserFactory create FieldParser.
type FieldParserFactory func(ps *Parser, field *ast.Field) FieldParser

// FieldParser parse struct field.
type FieldParser interface {
	ShouldSkip() bool
	FieldName() (string, error)
	FormName() string
	CustomSchema() (*spec.Schema, error)
	ComplementSchema(schema *spec.Schema) error
	IsRequired() (bool, error)
}

// Debugger is the interface that wraps the basic Printf method.
type Debugger interface {
	Printf(format string, v ...interface{})
}

// New creates a new Parser with default properties.
func New(options ...func(*Parser)) *Parser {
	parser := &Parser{
		swagger: &spec.Swagger{
			SwaggerProps: spec.SwaggerProps{
				Info: &spec.Info{
					InfoProps: spec.InfoProps{
						Contact: &spec.ContactInfo{},
						License: nil,
					},
					VendorExtensible: spec.VendorExtensible{
						Extensions: spec.Extensions{},
					},
				},
				Paths: &spec.Paths{
					Paths: make(map[string]spec.PathItem),
					VendorExtensible: spec.VendorExtensible{
						Extensions: nil,
					},
				},
				Definitions:         make(map[string]spec.Schema),
				SecurityDefinitions: make(map[string]*spec.SecurityScheme),
			},
			VendorExtensible: spec.VendorExtensible{
				Extensions: nil,
			},
		},
		packages:           NewPackagesDefinitions(),
		debug:              log.New(os.Stdout, "", log.LstdFlags),
		parsedSchemas:      make(map[*TypeSpecDef]*Schema),
		outputSchemas:      make(map[*TypeSpecDef]*Schema),
		excludes:           make(map[string]struct{}),
		tags:               make(map[string]struct{}),
		fieldParserFactory: newTagBaseFieldParser,
		Overrides:          make(map[string]string),
	}

	for _, option := range options {
		option(parser)
	}

	parser.packages.debug = parser.debug

	return parser
}

// SetParseDependency sets whether to parse the dependent packages.
func SetParseDependency(parseDependency bool) func(*Parser) {
	return func(p *Parser) {
		p.ParseDependency = parseDependency
		if p.packages != nil {
			p.packages.parseDependency = parseDependency
		}
	}
}

// SetMarkdownFileDirectory sets the directory to search for markdown files.
func SetMarkdownFileDirectory(directoryPath string) func(*Parser) {
	return func(p *Parser) {
		p.markdownFileDir = directoryPath
	}
}

// SetCodeExamplesDirectory sets the directory to search for code example files.
func SetCodeExamplesDirectory(directoryPath string) func(*Parser) {
	return func(p *Parser) {
		p.codeExampleFilesDir = directoryPath
	}
}

// SetExcludedDirsAndFiles sets directories and files to be excluded when searching.
func SetExcludedDirsAndFiles(excludes string) func(*Parser) {
	return func(p *Parser) {
		for _, f := range strings.Split(excludes, ",") {
			f = strings.TrimSpace(f)
			if f != "" {
				f = filepath.Clean(f)
				p.excludes[f] = struct{}{}
			}
		}
	}
}

// SetTags sets the tags to be included
func SetTags(include string) func(*Parser) {
	return func(p *Parser) {
		for _, f := range strings.Split(include, ",") {
			f = strings.TrimSpace(f)
			if f != "" {
				p.tags[f] = struct{}{}
			}
		}
	}
}

// SetParseExtension parses only those operations which match given extension
func SetParseExtension(parseExtension string) func(*Parser) {
	return func(p *Parser) {
		p.parseExtension = parseExtension
	}
}

// SetStrict sets whether swag should error or warn when it detects cases which are most likely user errors.
func SetStrict(strict bool) func(*Parser) {
	return func(p *Parser) {
		p.Strict = strict
	}
}

// SetDebugger allows the use of user-defined implementations.
func SetDebugger(logger Debugger) func(parser *Parser) {
	return func(p *Parser) {
		if logger != nil {
			p.debug = logger
		}
	}
}

// SetFieldParserFactory allows the use of user-defined implementations.
func SetFieldParserFactory(factory FieldParserFactory) func(parser *Parser) {
	return func(p *Parser) {
		p.fieldParserFactory = factory
	}
}

// SetOverrides allows the use of user-defined global type overrides.
func SetOverrides(overrides map[string]string) func(parser *Parser) {
	return func(p *Parser) {
		for k, v := range overrides {
			p.Overrides[k] = v
		}
	}
}

// SetCollectionFormat set default collection format
func SetCollectionFormat(collectionFormat string) func(*Parser) {
	return func(p *Parser) {
		p.collectionFormatInQuery = collectionFormat
	}
}

// ParseUsingGoList sets whether swag use go list to parse dependency
func ParseUsingGoList(enabled bool) func(parser *Parser) {
	return func(p *Parser) {
		p.parseGoList = enabled
	}
}

// ParseAPI parses general api info for given searchDir and mainAPIFile.
func (parser *Parser) ParseAPI(searchDir string, mainAPIFile string, parseDepth int) error {
	return parser.ParseAPIMultiSearchDir([]string{searchDir}, mainAPIFile, parseDepth)
}

// ParseAPIMultiSearchDir is like ParseAPI but for multiple search dirs.
func (parser *Parser) ParseAPIMultiSearchDir(searchDirs []string, mainAPIFile string, parseDepth int) error {
	for _, searchDir := range searchDirs {
		parser.debug.Printf("Generate general API Info, search dir:%s", searchDir)

		packageDir, err := getPkgName(searchDir)
		if err != nil {
			parser.debug.Printf("warning: failed to get package name in dir: %s, error: %s", searchDir, err.Error())
		}

		err = parser.getAllGoFileInfo(packageDir, searchDir)
		if err != nil {
			return err
		}
	}

	absMainAPIFilePath, err := filepath.Abs(filepath.Join(searchDirs[0], mainAPIFile))
	if err != nil {
		return err
	}

	// Use 'go list' command instead of depth.Resolve()
	if parser.ParseDependency {
		if parser.parseGoList {
			pkgs, err := listPackages(context.Background(), filepath.Dir(absMainAPIFilePath), nil, "-deps")
			if err != nil {
				return fmt.Errorf("pkg %s cannot find all dependencies, %s", filepath.Dir(absMainAPIFilePath), err)
			}

			length := len(pkgs)
			for i := 0; i < length; i++ {
				err := parser.getAllGoFileInfoFromDepsByList(pkgs[i])
				if err != nil {
					return err
				}
			}
		} else {
			var t depth.Tree
			t.ResolveInternal = true
			t.MaxDepth = parseDepth

			pkgName, err := getPkgName(filepath.Dir(absMainAPIFilePath))
			if err != nil {
				return err
			}

			err = t.Resolve(pkgName)
			if err != nil {
				return fmt.Errorf("pkg %s cannot find all dependencies, %s", pkgName, err)
			}
			for i := 0; i < len(t.Root.Deps); i++ {
				err := parser.getAllGoFileInfoFromDeps(&t.Root.Deps[i])
				if err != nil {
					return err
				}
			}
		}
	}

	err = parser.ParseGeneralAPIInfo(absMainAPIFilePath)
	if err != nil {
		return err
	}

	parser.parsedSchemas, err = parser.packages.ParseTypes()
	if err != nil {
		return err
	}

	err = parser.packages.RangeFiles(parser.ParseRouterAPIInfo)
	if err != nil {
		return err
	}

	return parser.checkOperationIDUniqueness()
}

func getPkgName(searchDir string) (string, error) {
	cmd := exec.Command("go", "list", "-f={{.ImportPath}}")
	cmd.Dir = searchDir

	var stdout, stderr strings.Builder

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("execute go list command, %s, stdout:%s, stderr:%s", err, stdout.String(), stderr.String())
	}

	outStr, _ := stdout.String(), stderr.String()

	if outStr[0] == '_' { // will shown like _/{GOPATH}/src/{YOUR_PACKAGE} when NOT enable GO MODULE.
		outStr = strings.TrimPrefix(outStr, "_"+build.Default.GOPATH+"/src/")
	}

	f := strings.Split(outStr, "\n")

	outStr = f[0]

	return outStr, nil
}

// ParseGeneralAPIInfo parses general api info for given mainAPIFile path.
func (parser *Parser) ParseGeneralAPIInfo(mainAPIFile string) error {
	fileTree, err := goparser.ParseFile(token.NewFileSet(), mainAPIFile, nil, goparser.ParseComments)
	if err != nil {
		return fmt.Errorf("cannot parse source files %s: %s", mainAPIFile, err)
	}

	parser.swagger.Swagger = "2.0"

	for _, comment := range fileTree.Comments {
		comments := strings.Split(comment.Text(), "\n")
		if !isGeneralAPIComment(comments) {
			continue
		}

		err = parseGeneralAPIInfo(parser, comments)
		if err != nil {
			return err
		}
	}

	return nil
}

func parseGeneralAPIInfo(parser *Parser, comments []string) error {
	previousAttribute := ""

	// parsing classic meta data model
	for line := 0; line < len(comments); line++ {
		commentLine := comments[line]
		commentLine = strings.TrimSpace(commentLine)
		if len(commentLine) == 0 {
			continue
		}
		fields := FieldsByAnySpace(commentLine, 2)

		attribute := fields[0]
		var value string
		if len(fields) > 1 {
			value = fields[1]
		}

		switch attr := strings.ToLower(attribute); attr {
		case versionAttr, titleAttr, tosAttr, licNameAttr, licURLAttr, conNameAttr, conURLAttr, conEmailAttr:
			setSwaggerInfo(parser.swagger, attr, value)
		case descriptionAttr:
			if previousAttribute == attribute {
				parser.swagger.Info.Description += "\n" + value

				continue
			}

			setSwaggerInfo(parser.swagger, attr, value)
		case descriptionMarkdownAttr:
			commentInfo, err := getMarkdownForTag("api", parser.markdownFileDir)
			if err != nil {
				return err
			}

			setSwaggerInfo(parser.swagger, descriptionAttr, string(commentInfo))

		case "@host":
			parser.swagger.Host = value
		case "@basepath":
			parser.swagger.BasePath = value

		case acceptAttr:
			err := parser.ParseAcceptComment(value)
			if err != nil {
				return err
			}
		case produceAttr:
			err := parser.ParseProduceComment(value)
			if err != nil {
				return err
			}
		case "@schemes":
			parser.swagger.Schemes = strings.Split(value, " ")
		case "@tag.name":
			parser.swagger.Tags = append(parser.swagger.Tags, spec.Tag{
				TagProps: spec.TagProps{
					Name: value,
				},
			})
		case "@tag.description":
			tag := parser.swagger.Tags[len(parser.swagger.Tags)-1]
			tag.TagProps.Description = value
			replaceLastTag(parser.swagger.Tags, tag)
		case "@tag.description.markdown":
			tag := parser.swagger.Tags[len(parser.swagger.Tags)-1]

			commentInfo, err := getMarkdownForTag(tag.TagProps.Name, parser.markdownFileDir)
			if err != nil {
				return err
			}

			tag.TagProps.Description = string(commentInfo)
			replaceLastTag(parser.swagger.Tags, tag)
		case "@tag.docs.url":
			tag := parser.swagger.Tags[len(parser.swagger.Tags)-1]
			tag.TagProps.ExternalDocs = &spec.ExternalDocumentation{
				URL:         value,
				Description: "",
			}

			replaceLastTag(parser.swagger.Tags, tag)
		case "@tag.docs.description":
			tag := parser.swagger.Tags[len(parser.swagger.Tags)-1]
			if tag.TagProps.ExternalDocs == nil {
				return fmt.Errorf("%s needs to come after a @tags.docs.url", attribute)
			}

			tag.TagProps.ExternalDocs.Description = value
			replaceLastTag(parser.swagger.Tags, tag)

		case secBasicAttr, secAPIKeyAttr, secApplicationAttr, secImplicitAttr, secPasswordAttr, secAccessCodeAttr:
			scheme, err := parseSecAttributes(attribute, comments, &line)
			if err != nil {
				return err
			}

			parser.swagger.SecurityDefinitions[value] = scheme

		case "@query.collection.format":
			parser.collectionFormatInQuery = TransToValidCollectionFormat(value)

		case extDocsDescAttr, extDocsURLAttr:
			if parser.swagger.ExternalDocs == nil {
				parser.swagger.ExternalDocs = new(spec.ExternalDocumentation)
			}
			switch attr {
			case extDocsDescAttr:
				parser.swagger.ExternalDocs.Description = value
			case extDocsURLAttr:
				parser.swagger.ExternalDocs.URL = value
			}

		default:
			if strings.HasPrefix(attribute, "@x-") {
				extensionName := attribute[1:]

				extExistsInSecurityDef := false
				// for each security definition
				for _, v := range parser.swagger.SecurityDefinitions {
					// check if extension exists
					_, extExistsInSecurityDef = v.VendorExtensible.Extensions.GetString(extensionName)
					// if it exists in at least one, then we stop iterating
					if extExistsInSecurityDef {
						break
					}
				}

				// if it is present on security def, don't add it again
				if extExistsInSecurityDef {
					break
				}

				if len(value) == 0 {
					return fmt.Errorf("annotation %s need a value", attribute)
				}

				var valueJSON interface{}
				err := json.Unmarshal([]byte(value), &valueJSON)
				if err != nil {
					return fmt.Errorf("annotation %s need a valid json value", attribute)
				}

				if strings.Contains(extensionName, "logo") {
					parser.swagger.Info.Extensions.Add(extensionName, valueJSON)
				} else {
					if parser.swagger.Extensions == nil {
						parser.swagger.Extensions = make(map[string]interface{})
					}

					parser.swagger.Extensions[attribute[1:]] = valueJSON
				}
			}
		}

		previousAttribute = attribute
	}

	return nil
}

func setSwaggerInfo(swagger *spec.Swagger, attribute, value string) {
	switch attribute {
	case versionAttr:
		swagger.Info.Version = value
	case titleAttr:
		swagger.Info.Title = value
	case tosAttr:
		swagger.Info.TermsOfService = value
	case descriptionAttr:
		swagger.Info.Description = value
	case conNameAttr:
		swagger.Info.Contact.Name = value
	case conEmailAttr:
		swagger.Info.Contact.Email = value
	case conURLAttr:
		swagger.Info.Contact.URL = value
	case licNameAttr:
		swagger.Info.License = initIfEmpty(swagger.Info.License)
		swagger.Info.License.Name = value
	case licURLAttr:
		swagger.Info.License = initIfEmpty(swagger.Info.License)
		swagger.Info.License.URL = value
	}
}

func parseSecAttributes(context string, lines []string, index *int) (*spec.SecurityScheme, error) {
	const (
		in               = "@in"
		name             = "@name"
		descriptionAttr  = "@description"
		tokenURL         = "@tokenurl"
		authorizationURL = "@authorizationurl"
	)

	var search []string

	attribute := strings.ToLower(FieldsByAnySpace(lines[*index], 2)[0])
	switch attribute {
	case secBasicAttr:
		return spec.BasicAuth(), nil
	case secAPIKeyAttr:
		search = []string{in, name}
	case secApplicationAttr, secPasswordAttr:
		search = []string{tokenURL}
	case secImplicitAttr:
		search = []string{authorizationURL}
	case secAccessCodeAttr:
		search = []string{tokenURL, authorizationURL}
	}

	// For the first line we get the attributes in the context parameter, so we skip to the next one
	*index++

	attrMap, scopes := make(map[string]string), make(map[string]string)
	extensions, description := make(map[string]interface{}), ""

	for ; *index < len(lines); *index++ {
		v := strings.TrimSpace(lines[*index])
		if len(v) == 0 {
			continue
		}

		fields := FieldsByAnySpace(v, 2)
		securityAttr := strings.ToLower(fields[0])
		var value string
		if len(fields) > 1 {
			value = fields[1]
		}

		for _, findterm := range search {
			if securityAttr == findterm {
				attrMap[securityAttr] = value

				break
			}
		}

		isExists, err := isExistsScope(securityAttr)
		if err != nil {
			return nil, err
		}

		if isExists {
			scopes[securityAttr[len(scopeAttrPrefix):]] = v[len(securityAttr):]
		}

		if strings.HasPrefix(securityAttr, "@x-") {
			// Add the custom attribute without the @
			extensions[securityAttr[1:]] = value
		}

		// Not mandatory field
		if securityAttr == descriptionAttr {
			description = value
		}

		// next securityDefinitions
		if strings.Index(securityAttr, "@securitydefinitions.") == 0 {
			// Go back to the previous line and break
			*index--

			break
		}
	}

	if len(attrMap) != len(search) {
		return nil, fmt.Errorf("%s is %v required", context, search)
	}

	var scheme *spec.SecurityScheme

	switch attribute {
	case secAPIKeyAttr:
		scheme = spec.APIKeyAuth(attrMap[name], attrMap[in])
	case secApplicationAttr:
		scheme = spec.OAuth2Application(attrMap[tokenURL])
	case secImplicitAttr:
		scheme = spec.OAuth2Implicit(attrMap[authorizationURL])
	case secPasswordAttr:
		scheme = spec.OAuth2Password(attrMap[tokenURL])
	case secAccessCodeAttr:
		scheme = spec.OAuth2AccessToken(attrMap[authorizationURL], attrMap[tokenURL])
	}

	scheme.Description = description

	for extKey, extValue := range extensions {
		scheme.AddExtension(extKey, extValue)
	}

	for scope, scopeDescription := range scopes {
		scheme.AddScope(scope, scopeDescription)
	}

	return scheme, nil
}

func initIfEmpty(license *spec.License) *spec.License {
	if license == nil {
		return new(spec.License)
	}

	return license
}

// ParseAcceptComment parses comment for given `accept` comment string.
func (parser *Parser) ParseAcceptComment(commentLine string) error {
	return parseMimeTypeList(commentLine, &parser.swagger.Consumes, "%v accept type can't be accepted")
}

// ParseProduceComment parses comment for given `produce` comment string.
func (parser *Parser) ParseProduceComment(commentLine string) error {
	return parseMimeTypeList(commentLine, &parser.swagger.Produces, "%v produce type can't be accepted")
}

func isGeneralAPIComment(comments []string) bool {
	for _, commentLine := range comments {
		commentLine = strings.TrimSpace(commentLine)
		if len(commentLine) == 0 {
			continue
		}
		attribute := strings.ToLower(FieldsByAnySpace(commentLine, 2)[0])
		switch attribute {
		// The @summary, @router, @success, @failure annotation belongs to Operation
		case summaryAttr, routerAttr, successAttr, failureAttr, responseAttr:
			return false
		}
	}

	return true
}

func getMarkdownForTag(tagName string, dirPath string) ([]byte, error) {
	dirEntries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()

		if !strings.Contains(fileName, ".md") {
			continue
		}

		if strings.Contains(fileName, tagName) {
			fullPath := filepath.Join(dirPath, fileName)

			commentInfo, err := os.ReadFile(fullPath)
			if err != nil {
				return nil, fmt.Errorf("Failed to read markdown file %s error: %s ", fullPath, err)
			}

			return commentInfo, nil
		}
	}

	return nil, fmt.Errorf("Unable to find markdown file for tag %s in the given directory", tagName)
}

func isExistsScope(scope string) (bool, error) {
	s := strings.Fields(scope)
	for _, v := range s {
		if strings.Contains(v, scopeAttrPrefix) {
			if strings.Contains(v, ",") {
				return false, fmt.Errorf("@scope can't use comma(,) get=" + v)
			}
		}
	}

	return strings.Contains(scope, scopeAttrPrefix), nil
}

func getTagsFromComment(comment string) (tags []string) {
	commentLine := strings.TrimSpace(strings.TrimLeft(comment, "/"))
	if len(commentLine) == 0 {
		return nil
	}

	attribute := strings.Fields(commentLine)[0]
	lineRemainder, lowerAttribute := strings.TrimSpace(commentLine[len(attribute):]), strings.ToLower(attribute)

	if lowerAttribute == tagsAttr {
		for _, tag := range strings.Split(lineRemainder, ",") {
			tags = append(tags, strings.TrimSpace(tag))
		}
	}
	return

}

func (parser *Parser) matchTags(comments []*ast.Comment) (match bool) {
	if len(parser.tags) != 0 {
		for _, comment := range comments {
			for _, tag := range getTagsFromComment(comment.Text) {
				if _, has := parser.tags["!"+tag]; has {
					return false
				}
				if _, has := parser.tags[tag]; has {
					match = true // keep iterating as it may contain a tag that is excluded
				}
			}
		}
		return
	}
	return true
}

func matchExtension(extensionToMatch string, comments []*ast.Comment) (match bool) {
	if len(extensionToMatch) != 0 {
		for _, comment := range comments {
			commentLine := strings.TrimSpace(strings.TrimLeft(comment.Text, "/"))
			fields := FieldsByAnySpace(commentLine, 2)
			if len(fields) > 0 {
				lowerAttribute := strings.ToLower(fields[0])

				if lowerAttribute == fmt.Sprintf("@x-%s", strings.ToLower(extensionToMatch)) {
					return true
				}
			}
		}
		return false
	}
	return true
}

// ParseRouterAPIInfo parses router api info for given astFile.
func (parser *Parser) ParseRouterAPIInfo(fileInfo *AstFileInfo) error {
	for _, astDescription := range fileInfo.File.Decls {
		if (fileInfo.ParseFlag & ParseOperations) == ParseNone {
			continue
		}
		astDeclaration, ok := astDescription.(*ast.FuncDecl)
		if ok && astDeclaration.Doc != nil && astDeclaration.Doc.List != nil {
			if parser.matchTags(astDeclaration.Doc.List) &&
				matchExtension(parser.parseExtension, astDeclaration.Doc.List) {
				// for per 'function' comment, create a new 'Operation' object
				operation := NewOperation(parser, SetCodeExampleFilesDirectory(parser.codeExampleFilesDir))
				for _, comment := range astDeclaration.Doc.List {
					err := operation.ParseComment(comment.Text, fileInfo.File)
					if err != nil {
						return fmt.Errorf("ParseComment error in file %s :%+v", fileInfo.Path, err)
					}
				}
				err := processRouterOperation(parser, operation)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func refRouteMethodOp(item *spec.PathItem, method string) (op **spec.Operation) {
	switch method {
	case http.MethodGet:
		op = &item.Get
	case http.MethodPost:
		op = &item.Post
	case http.MethodDelete:
		op = &item.Delete
	case http.MethodPut:
		op = &item.Put
	case http.MethodPatch:
		op = &item.Patch
	case http.MethodHead:
		op = &item.Head
	case http.MethodOptions:
		op = &item.Options
	}

	return
}

func processRouterOperation(parser *Parser, operation *Operation) error {
	for _, routeProperties := range operation.RouterProperties {
		var (
			pathItem spec.PathItem
			ok       bool
		)

		pathItem, ok = parser.swagger.Paths.Paths[routeProperties.Path]
		if !ok {
			pathItem = spec.PathItem{}
		}

		op := refRouteMethodOp(&pathItem, routeProperties.HTTPMethod)

		// check if we already have an operation for this path and method
		if *op != nil {
			err := fmt.Errorf("route %s %s is declared multiple times", routeProperties.HTTPMethod, routeProperties.Path)
			if parser.Strict {
				return err
			}

			parser.debug.Printf("warning: %s\n", err)
		}

		*op = &operation.Operation

		parser.swagger.Paths.Paths[routeProperties.Path] = pathItem
	}

	return nil
}

func convertFromSpecificToPrimitive(typeName string) (string, error) {
	name := typeName
	if strings.ContainsRune(name, '.') {
		name = strings.Split(name, ".")[1]
	}

	switch strings.ToUpper(name) {
	case "TIME", "OBJECTID", "UUID":
		return STRING, nil
	case "DECIMAL":
		return NUMBER, nil
	}

	return typeName, ErrFailedConvertPrimitiveType
}

func (parser *Parser) getTypeSchema(typeName string, file *ast.File, ref bool) (*spec.Schema, error) {
	if override, ok := parser.Overrides[typeName]; ok {
		parser.debug.Printf("Override detected for %s: using %s instead", typeName, override)
		return parseObjectSchema(parser, override, file)
	}

	if IsInterfaceLike(typeName) {
		return &spec.Schema{}, nil
	}
	if IsGolangPrimitiveType(typeName) {
		return PrimitiveSchema(TransToValidSchemeType(typeName)), nil
	}

	schemaType, err := convertFromSpecificToPrimitive(typeName)
	if err == nil {
		return PrimitiveSchema(schemaType), nil
	}

	typeSpecDef := parser.packages.FindTypeSpec(typeName, file)
	if typeSpecDef == nil {
		return nil, fmt.Errorf("cannot find type definition: %s", typeName)
	}

	if override, ok := parser.Overrides[typeSpecDef.FullPath()]; ok {
		if override == "" {
			parser.debug.Printf("Override detected for %s: ignoring", typeSpecDef.FullPath())

			return nil, ErrSkippedField
		}

		parser.debug.Printf("Override detected for %s: using %s instead", typeSpecDef.FullPath(), override)

		separator := strings.LastIndex(override, ".")
		if separator == -1 {
			// treat as a swaggertype tag
			parts := strings.Split(override, ",")

			return BuildCustomSchema(parts)
		}

		typeSpecDef = parser.packages.findTypeSpec(override[0:separator], override[separator+1:])
	}

	schema, ok := parser.parsedSchemas[typeSpecDef]
	if !ok {
		var err error

		schema, err = parser.ParseDefinition(typeSpecDef)
		if err != nil {
			if err == ErrRecursiveParseStruct && ref {
				return parser.getRefTypeSchema(typeSpecDef, schema), nil
			}
			return nil, err
		}
	}

	if ref {
		if IsComplexSchema(schema.Schema) {
			return parser.getRefTypeSchema(typeSpecDef, schema), nil
		}
		// if it is a simple schema, just return a copy
		newSchema := *schema.Schema
		return &newSchema, nil
	}

	return schema.Schema, nil
}

func (parser *Parser) getRefTypeSchema(typeSpecDef *TypeSpecDef, schema *Schema) *spec.Schema {
	_, ok := parser.outputSchemas[typeSpecDef]
	if !ok {
		parser.swagger.Definitions[schema.Name] = spec.Schema{}

		if schema.Schema != nil {
			parser.swagger.Definitions[schema.Name] = *schema.Schema
		}

		parser.outputSchemas[typeSpecDef] = schema
	}

	refSchema := RefSchema(schema.Name)

	return refSchema
}

func (parser *Parser) isInStructStack(typeSpecDef *TypeSpecDef) bool {
	for _, specDef := range parser.structStack {
		if typeSpecDef == specDef {
			return true
		}
	}

	return false
}

// ParseDefinition parses given type spec that corresponds to the type under
// given name and package, and populates swagger schema definitions registry
// with a schema for the given type
func (parser *Parser) ParseDefinition(typeSpecDef *TypeSpecDef) (*Schema, error) {
	typeName := typeSpecDef.TypeName()
	schema, found := parser.parsedSchemas[typeSpecDef]
	if found {
		parser.debug.Printf("Skipping '%s', already parsed.", typeName)

		return schema, nil
	}

	if parser.isInStructStack(typeSpecDef) {
		parser.debug.Printf("Skipping '%s', recursion detected.", typeName)

		return &Schema{
				Name:    typeName,
				PkgPath: typeSpecDef.PkgPath,
				Schema:  PrimitiveSchema(OBJECT),
			},
			ErrRecursiveParseStruct
	}

	parser.structStack = append(parser.structStack, typeSpecDef)

	parser.debug.Printf("Generating %s", typeName)

	definition, err := parser.parseTypeExpr(typeSpecDef.File, typeSpecDef.TypeSpec.Type, false)
	if err != nil {
		parser.debug.Printf("Error parsing type definition '%s': %s", typeName, err)
		return nil, err
	}

	if definition.Description == "" {
		fillDefinitionDescription(definition, typeSpecDef.File, typeSpecDef)
	}

	if len(typeSpecDef.Enums) > 0 {
		var varnames []string
		var enumComments = make(map[string]string)
		for _, value := range typeSpecDef.Enums {
			definition.Enum = append(definition.Enum, value.Value)
			varnames = append(varnames, value.key)
			if len(value.Comment) > 0 {
				enumComments[value.key] = value.Comment
			}
		}
		if definition.Extensions == nil {
			definition.Extensions = make(spec.Extensions)
		}
		definition.Extensions[enumVarNamesExtension] = varnames
		if len(enumComments) > 0 {
			definition.Extensions[enumCommentsExtension] = enumComments
		}
	}

	sch := Schema{
		Name:    typeName,
		PkgPath: typeSpecDef.PkgPath,
		Schema:  definition,
	}
	parser.parsedSchemas[typeSpecDef] = &sch

	// update an empty schema as a result of recursion
	s2, found := parser.outputSchemas[typeSpecDef]
	if found {
		parser.swagger.Definitions[s2.Name] = *definition
	}

	return &sch, nil
}

func fullTypeName(parts ...string) string {
	return strings.Join(parts, ".")
}

// fillDefinitionDescription additionally fills fields in definition (spec.Schema)
// TODO: If .go file contains many types, it may work for a long time
func fillDefinitionDescription(definition *spec.Schema, file *ast.File, typeSpecDef *TypeSpecDef) {
	for _, astDeclaration := range file.Decls {
		generalDeclaration, ok := astDeclaration.(*ast.GenDecl)
		if !ok || generalDeclaration.Tok != token.TYPE {
			continue
		}

		for _, astSpec := range generalDeclaration.Specs {
			typeSpec, ok := astSpec.(*ast.TypeSpec)
			if !ok || typeSpec != typeSpecDef.TypeSpec {
				continue
			}

			definition.Description =
				extractDeclarationDescription(typeSpec.Doc, typeSpec.Comment, generalDeclaration.Doc)
		}
	}
}

// extractDeclarationDescription gets first description
// from attribute descriptionAttr in commentGroups (ast.CommentGroup)
func extractDeclarationDescription(commentGroups ...*ast.CommentGroup) string {
	var description string

	for _, commentGroup := range commentGroups {
		if commentGroup == nil {
			continue
		}

		isHandlingDescription := false

		for _, comment := range commentGroup.List {
			commentText := strings.TrimSpace(strings.TrimLeft(comment.Text, "/"))
			if len(commentText) == 0 {
				continue
			}
			attribute := FieldsByAnySpace(commentText, 2)[0]

			if strings.ToLower(attribute) != descriptionAttr {
				if !isHandlingDescription {
					continue
				}

				break
			}

			isHandlingDescription = true
			description += " " + strings.TrimSpace(commentText[len(attribute):])
		}
	}

	return strings.TrimLeft(description, " ")
}

// parseTypeExpr parses given type expression that corresponds to the type under
// given name and package, and returns swagger schema for it.
func (parser *Parser) parseTypeExpr(file *ast.File, typeExpr ast.Expr, ref bool) (*spec.Schema, error) {
	switch expr := typeExpr.(type) {
	// type Foo interface{}
	case *ast.InterfaceType:
		return &spec.Schema{}, nil

	// type Foo struct {...}
	case *ast.StructType:
		return parser.parseStruct(file, expr.Fields)

	// type Foo Baz
	case *ast.Ident:
		return parser.getTypeSchema(expr.Name, file, ref)

	// type Foo *Baz
	case *ast.StarExpr:
		return parser.parseTypeExpr(file, expr.X, ref)

	// type Foo pkg.Bar
	case *ast.SelectorExpr:
		if xIdent, ok := expr.X.(*ast.Ident); ok {
			return parser.getTypeSchema(fullTypeName(xIdent.Name, expr.Sel.Name), file, ref)
		}
	// type Foo []Baz
	case *ast.ArrayType:
		itemSchema, err := parser.parseTypeExpr(file, expr.Elt, true)
		if err != nil {
			return nil, err
		}

		return spec.ArrayProperty(itemSchema), nil
	// type Foo map[string]Bar
	case *ast.MapType:
		if _, ok := expr.Value.(*ast.InterfaceType); ok {
			return spec.MapProperty(nil), nil
		}
		schema, err := parser.parseTypeExpr(file, expr.Value, true)
		if err != nil {
			return nil, err
		}

		return spec.MapProperty(schema), nil

	case *ast.FuncType:
		return nil, ErrFuncTypeField
		// ...
	}

	return parser.parseGenericTypeExpr(file, typeExpr)
}

func (parser *Parser) parseStruct(file *ast.File, fields *ast.FieldList) (*spec.Schema, error) {
	required, properties := make([]string, 0), make(map[string]spec.Schema)

	for _, field := range fields.List {
		fieldProps, requiredFromAnon, err := parser.parseStructField(file, field)
		if err != nil {
			if err == ErrFuncTypeField || err == ErrSkippedField {
				continue
			}

			return nil, err
		}

		if len(fieldProps) == 0 {
			continue
		}

		required = append(required, requiredFromAnon...)

		for k, v := range fieldProps {
			properties[k] = v
		}
	}

	sort.Strings(required)

	return &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:       []string{OBJECT},
			Properties: properties,
			Required:   required,
		},
	}, nil
}

func (parser *Parser) parseStructField(file *ast.File, field *ast.Field) (map[string]spec.Schema, []string, error) {
	if field.Tag != nil {
		skip, ok := reflect.StructTag(strings.ReplaceAll(field.Tag.Value, "`", "")).Lookup("swaggerignore")
		if ok && strings.EqualFold(skip, "true") {
			return nil, nil, nil
		}
	}

	ps := parser.fieldParserFactory(parser, field)

	if ps.ShouldSkip() {
		return nil, nil, nil
	}

	fieldName, err := ps.FieldName()
	if err != nil {
		return nil, nil, err
	}

	if fieldName == "" {
		typeName, err := getFieldType(file, field.Type, nil)
		if err != nil {
			return nil, nil, err
		}

		schema, err := parser.getTypeSchema(typeName, file, false)
		if err != nil {
			return nil, nil, err
		}

		if len(schema.Type) > 0 && schema.Type[0] == OBJECT {
			if len(schema.Properties) == 0 {
				return nil, nil, nil
			}

			properties := map[string]spec.Schema{}
			for k, v := range schema.Properties {
				properties[k] = v
			}

			return properties, schema.SchemaProps.Required, nil
		}
		// for alias type of non-struct types ,such as array,map, etc. ignore field tag.
		return map[string]spec.Schema{typeName: *schema}, nil, nil

	}

	schema, err := ps.CustomSchema()
	if err != nil {
		return nil, nil, err
	}

	if schema == nil {
		typeName, err := getFieldType(file, field.Type, nil)
		if err == nil {
			// named type
			schema, err = parser.getTypeSchema(typeName, file, true)
		} else {
			// unnamed type
			schema, err = parser.parseTypeExpr(file, field.Type, false)
		}

		if err != nil {
			return nil, nil, err
		}
	}

	err = ps.ComplementSchema(schema)
	if err != nil {
		return nil, nil, err
	}

	var tagRequired []string

	required, err := ps.IsRequired()
	if err != nil {
		return nil, nil, err
	}

	if required {
		tagRequired = append(tagRequired, fieldName)
	}

	if formName := ps.FormName(); len(formName) > 0 {
		if schema.Extensions == nil {
			schema.Extensions = make(spec.Extensions)
		}
		schema.Extensions[formTag] = formName
	}

	return map[string]spec.Schema{fieldName: *schema}, tagRequired, nil
}

func getFieldType(file *ast.File, field ast.Expr, genericParamTypeDefs map[string]*genericTypeSpec) (string, error) {
	switch fieldType := field.(type) {
	case *ast.Ident:
		return fieldType.Name, nil
	case *ast.SelectorExpr:
		packageName, err := getFieldType(file, fieldType.X, genericParamTypeDefs)
		if err != nil {
			return "", err
		}

		return fullTypeName(packageName, fieldType.Sel.Name), nil
	case *ast.StarExpr:
		fullName, err := getFieldType(file, fieldType.X, genericParamTypeDefs)
		if err != nil {
			return "", err
		}

		return fullName, nil
	default:
		return getGenericFieldType(file, field, genericParamTypeDefs)
	}
}

func (parser *Parser) getUnderlyingSchema(schema *spec.Schema) *spec.Schema {
	if schema == nil {
		return nil
	}

	if url := schema.Ref.GetURL(); url != nil {
		if pos := strings.LastIndexByte(url.Fragment, '/'); pos >= 0 {
			name := url.Fragment[pos+1:]
			if schema, ok := parser.swagger.Definitions[name]; ok {
				return &schema
			}
		}
	}

	if len(schema.AllOf) > 0 {
		merged := &spec.Schema{}
		MergeSchema(merged, schema)
		for _, s := range schema.AllOf {
			MergeSchema(merged, parser.getUnderlyingSchema(&s))
		}
		return merged
	}
	return nil
}

// GetSchemaTypePath get path of schema type.
func (parser *Parser) GetSchemaTypePath(schema *spec.Schema, depth int) []string {
	if schema == nil || depth == 0 {
		return nil
	}

	if underlying := parser.getUnderlyingSchema(schema); underlying != nil {
		return parser.GetSchemaTypePath(underlying, depth)
	}

	if len(schema.Type) > 0 {
		switch schema.Type[0] {
		case ARRAY:
			depth--

			s := []string{schema.Type[0]}

			return append(s, parser.GetSchemaTypePath(schema.Items.Schema, depth)...)
		case OBJECT:
			if schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil {
				// for map
				depth--

				s := []string{schema.Type[0]}

				return append(s, parser.GetSchemaTypePath(schema.AdditionalProperties.Schema, depth)...)
			}
		}

		return []string{schema.Type[0]}
	}

	return []string{ANY}
}

func replaceLastTag(slice []spec.Tag, element spec.Tag) {
	slice = append(slice[:len(slice)-1], element)
}

// defineTypeOfExample example value define the type (object and array unsupported).
func defineTypeOfExample(schemaType, arrayType, exampleValue string) (interface{}, error) {
	switch schemaType {
	case STRING:
		return exampleValue, nil
	case NUMBER:
		v, err := strconv.ParseFloat(exampleValue, 64)
		if err != nil {
			return nil, fmt.Errorf("example value %s can't convert to %s err: %s", exampleValue, schemaType, err)
		}

		return v, nil
	case INTEGER:
		v, err := strconv.Atoi(exampleValue)
		if err != nil {
			return nil, fmt.Errorf("example value %s can't convert to %s err: %s", exampleValue, schemaType, err)
		}

		return v, nil
	case BOOLEAN:
		v, err := strconv.ParseBool(exampleValue)
		if err != nil {
			return nil, fmt.Errorf("example value %s can't convert to %s err: %s", exampleValue, schemaType, err)
		}

		return v, nil
	case ARRAY:
		values := strings.Split(exampleValue, ",")
		result := make([]interface{}, 0)
		for _, value := range values {
			v, err := defineTypeOfExample(arrayType, "", value)
			if err != nil {
				return nil, err
			}

			result = append(result, v)
		}

		return result, nil
	case OBJECT:
		if arrayType == "" {
			return nil, fmt.Errorf("%s is unsupported type in example value `%s`", schemaType, exampleValue)
		}

		values := strings.Split(exampleValue, ",")

		result := map[string]interface{}{}

		for _, value := range values {
			mapData := strings.SplitN(value, ":", 2)

			if len(mapData) == 2 {
				v, err := defineTypeOfExample(arrayType, "", mapData[1])
				if err != nil {
					return nil, err
				}

				result[mapData[0]] = v

				continue
			}

			return nil, fmt.Errorf("example value %s should format: key:value", exampleValue)
		}

		return result, nil
	}

	return nil, fmt.Errorf("%s is unsupported type in example value %s", schemaType, exampleValue)
}

// GetAllGoFileInfo gets all Go source files information for given searchDir.
func (parser *Parser) getAllGoFileInfo(packageDir, searchDir string) error {
	return filepath.Walk(searchDir, func(path string, f os.FileInfo, _ error) error {
		err := parser.Skip(path, f)
		if err != nil {
			return err
		}

		if f.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(searchDir, path)
		if err != nil {
			return err
		}

		return parser.parseFile(filepath.ToSlash(filepath.Dir(filepath.Clean(filepath.Join(packageDir, relPath)))), path, nil, ParseAll)
	})
}

func (parser *Parser) getAllGoFileInfoFromDeps(pkg *depth.Pkg) error {
	ignoreInternal := pkg.Internal && !parser.ParseInternal
	if ignoreInternal || !pkg.Resolved { // ignored internal and not resolved dependencies
		return nil
	}

	// Skip cgo
	if pkg.Raw == nil && pkg.Name == "C" {
		return nil
	}

	srcDir := pkg.Raw.Dir

	files, err := os.ReadDir(srcDir) // only parsing files in the dir(don't contain sub dir files)
	if err != nil {
		return err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		path := filepath.Join(srcDir, f.Name())
		if err := parser.parseFile(pkg.Name, path, nil, ParseModels); err != nil {
			return err
		}
	}

	for i := 0; i < len(pkg.Deps); i++ {
		if err := parser.getAllGoFileInfoFromDeps(&pkg.Deps[i]); err != nil {
			return err
		}
	}

	return nil
}

func (parser *Parser) parseFile(packageDir, path string, src interface{}, flag ParseFlag) error {
	if strings.HasSuffix(strings.ToLower(path), "_test.go") || filepath.Ext(path) != ".go" {
		return nil
	}

	return parser.packages.ParseFile(packageDir, path, src, flag)
}

func (parser *Parser) checkOperationIDUniqueness() error {
	// operationsIds contains all operationId annotations to check it's unique
	operationsIds := make(map[string]string)

	for path, item := range parser.swagger.Paths.Paths {
		var method, id string

		for method = range allMethod {
			op := refRouteMethodOp(&item, method)
			if *op != nil {
				id = (**op).ID

				break
			}
		}

		if id == "" {
			continue
		}

		current := fmt.Sprintf("%s %s", method, path)

		previous, ok := operationsIds[id]
		if ok {
			return fmt.Errorf(
				"duplicated @id annotation '%s' found in '%s', previously declared in: '%s'",
				id, current, previous)
		}

		operationsIds[id] = current
	}

	return nil
}

// Skip returns filepath.SkipDir error if match vendor and hidden folder.
func (parser *Parser) Skip(path string, f os.FileInfo) error {
	return walkWith(parser.excludes, parser.ParseVendor)(path, f)
}

func walkWith(excludes map[string]struct{}, parseVendor bool) func(path string, fileInfo os.FileInfo) error {
	return func(path string, f os.FileInfo) error {
		if f.IsDir() {
			if !parseVendor && f.Name() == "vendor" || // ignore "vendor"
				f.Name() == "docs" || // exclude docs
				len(f.Name()) > 1 && f.Name()[0] == '.' && f.Name() != ".." { // exclude all hidden folder
				return filepath.SkipDir
			}

			if excludes != nil {
				if _, ok := excludes[path]; ok {
					return filepath.SkipDir
				}
			}
		}

		return nil
	}
}

// GetSwagger returns *spec.Swagger which is the root document object for the API specification.
func (parser *Parser) GetSwagger() *spec.Swagger {
	return parser.swagger
}

// addTestType just for tests.
func (parser *Parser) addTestType(typename string) {
	typeDef := &TypeSpecDef{}
	parser.packages.uniqueDefinitions[typename] = typeDef
	parser.parsedSchemas[typeDef] = &Schema{
		PkgPath: "",
		Name:    typename,
		Schema:  PrimitiveSchema(OBJECT),
	}
}
