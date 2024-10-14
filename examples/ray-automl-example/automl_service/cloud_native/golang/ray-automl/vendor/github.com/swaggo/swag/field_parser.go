package swag

import (
	"fmt"
	"go/ast"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/go-openapi/spec"
)

var _ FieldParser = &tagBaseFieldParser{p: nil, field: nil, tag: ""}

const (
	requiredLabel    = "required"
	optionalLabel    = "optional"
	swaggerTypeTag   = "swaggertype"
	swaggerIgnoreTag = "swaggerignore"
)

type tagBaseFieldParser struct {
	p     *Parser
	field *ast.Field
	tag   reflect.StructTag
}

func newTagBaseFieldParser(p *Parser, field *ast.Field) FieldParser {
	fieldParser := tagBaseFieldParser{
		p:     p,
		field: field,
		tag:   "",
	}
	if fieldParser.field.Tag != nil {
		fieldParser.tag = reflect.StructTag(strings.ReplaceAll(field.Tag.Value, "`", ""))
	}

	return &fieldParser
}

func (ps *tagBaseFieldParser) ShouldSkip() bool {
	// Skip non-exported fields.
	if ps.field.Names != nil && !ast.IsExported(ps.field.Names[0].Name) {
		return true
	}

	if ps.field.Tag == nil {
		return false
	}

	ignoreTag := ps.tag.Get(swaggerIgnoreTag)
	if strings.EqualFold(ignoreTag, "true") {
		return true
	}

	// json:"tag,hoge"
	name := strings.TrimSpace(strings.Split(ps.tag.Get(jsonTag), ",")[0])
	if name == "-" {
		return true
	}

	return false
}

func (ps *tagBaseFieldParser) FieldName() (string, error) {
	var name string

	if ps.field.Tag != nil {
		// json:"tag,hoge"
		name = strings.TrimSpace(strings.Split(ps.tag.Get(jsonTag), ",")[0])
		if name != "" {
			return name, nil
		}

		// use "form" tag over json tag
		name = ps.FormName()
		if name != "" {
			return name, nil
		}
	}

	if ps.field.Names == nil {
		return "", nil
	}

	switch ps.p.PropNamingStrategy {
	case SnakeCase:
		return toSnakeCase(ps.field.Names[0].Name), nil
	case PascalCase:
		return ps.field.Names[0].Name, nil
	default:
		return toLowerCamelCase(ps.field.Names[0].Name), nil
	}
}

func (ps *tagBaseFieldParser) FormName() string {
	if ps.field.Tag != nil {
		return strings.TrimSpace(strings.Split(ps.tag.Get(formTag), ",")[0])
	}
	return ""
}

func toSnakeCase(in string) string {
	var (
		runes  = []rune(in)
		length = len(runes)
		out    []rune
	)

	for idx := 0; idx < length; idx++ {
		if idx > 0 && unicode.IsUpper(runes[idx]) &&
			((idx+1 < length && unicode.IsLower(runes[idx+1])) || unicode.IsLower(runes[idx-1])) {
			out = append(out, '_')
		}

		out = append(out, unicode.ToLower(runes[idx]))
	}

	return string(out)
}

func toLowerCamelCase(in string) string {
	var flag bool

	out := make([]rune, len(in))

	runes := []rune(in)
	for i, curr := range runes {
		if (i == 0 && unicode.IsUpper(curr)) || (flag && unicode.IsUpper(curr)) {
			out[i] = unicode.ToLower(curr)
			flag = true

			continue
		}

		out[i] = curr
		flag = false
	}

	return string(out)
}

func (ps *tagBaseFieldParser) CustomSchema() (*spec.Schema, error) {
	if ps.field.Tag == nil {
		return nil, nil
	}

	typeTag := ps.tag.Get(swaggerTypeTag)
	if typeTag != "" {
		return BuildCustomSchema(strings.Split(typeTag, ","))
	}

	return nil, nil
}

type structField struct {
	schemaType   string
	arrayType    string
	formatType   string
	maximum      *float64
	minimum      *float64
	multipleOf   *float64
	maxLength    *int64
	minLength    *int64
	maxItems     *int64
	minItems     *int64
	exampleValue interface{}
	enums        []interface{}
	enumVarNames []interface{}
	unique       bool
}

// splitNotWrapped slices s into all substrings separated by sep if sep is not
// wrapped by brackets and returns a slice of the substrings between those separators.
func splitNotWrapped(s string, sep rune) []string {
	openCloseMap := map[rune]rune{
		'(': ')',
		'[': ']',
		'{': '}',
	}

	var (
		result    = make([]string, 0)
		current   = strings.Builder{}
		openCount = 0
		openChar  rune
	)

	for _, char := range s {
		switch {
		case openChar == 0 && openCloseMap[char] != 0:
			openChar = char

			openCount++

			current.WriteRune(char)
		case char == openChar:
			openCount++

			current.WriteRune(char)
		case openCount > 0 && char == openCloseMap[openChar]:
			openCount--

			current.WriteRune(char)
		case openCount == 0 && char == sep:
			result = append(result, current.String())

			openChar = 0

			current = strings.Builder{}
		default:
			current.WriteRune(char)
		}
	}

	if current.String() != "" {
		result = append(result, current.String())
	}

	return result
}

// ComplementSchema complement schema with field properties
func (ps *tagBaseFieldParser) ComplementSchema(schema *spec.Schema) error {
	types := ps.p.GetSchemaTypePath(schema, 2)
	if len(types) == 0 {
		return fmt.Errorf("invalid type for field: %s", ps.field.Names[0])
	}

	if IsRefSchema(schema) {
		var newSchema = spec.Schema{}
		err := ps.complementSchema(&newSchema, types)
		if err != nil {
			return err
		}
		if !reflect.ValueOf(newSchema).IsZero() {
			*schema = *(newSchema.WithAllOf(*schema))
		}
		return nil
	}

	return ps.complementSchema(schema, types)
}

// complementSchema complement schema with field properties
func (ps *tagBaseFieldParser) complementSchema(schema *spec.Schema, types []string) error {
	if ps.field.Tag == nil {
		if ps.field.Doc != nil {
			schema.Description = strings.TrimSpace(ps.field.Doc.Text())
		}

		if schema.Description == "" && ps.field.Comment != nil {
			schema.Description = strings.TrimSpace(ps.field.Comment.Text())
		}

		return nil
	}

	field := &structField{
		schemaType: types[0],
		formatType: ps.tag.Get(formatTag),
	}

	if len(types) > 1 && (types[0] == ARRAY || types[0] == OBJECT) {
		field.arrayType = types[1]
	}

	jsonTagValue := ps.tag.Get(jsonTag)

	bindingTagValue := ps.tag.Get(bindingTag)
	if bindingTagValue != "" {
		parseValidTags(bindingTagValue, field)
	}

	validateTagValue := ps.tag.Get(validateTag)
	if validateTagValue != "" {
		parseValidTags(validateTagValue, field)
	}

	enumsTagValue := ps.tag.Get(enumsTag)
	if enumsTagValue != "" {
		err := parseEnumTags(enumsTagValue, field)
		if err != nil {
			return err
		}
	}

	if IsNumericType(field.schemaType) || IsNumericType(field.arrayType) {
		maximum, err := getFloatTag(ps.tag, maximumTag)
		if err != nil {
			return err
		}

		if maximum != nil {
			field.maximum = maximum
		}

		minimum, err := getFloatTag(ps.tag, minimumTag)
		if err != nil {
			return err
		}

		if minimum != nil {
			field.minimum = minimum
		}

		multipleOf, err := getFloatTag(ps.tag, multipleOfTag)
		if err != nil {
			return err
		}

		if multipleOf != nil {
			field.multipleOf = multipleOf
		}
	}

	if field.schemaType == STRING || field.arrayType == STRING {
		maxLength, err := getIntTag(ps.tag, maxLengthTag)
		if err != nil {
			return err
		}

		if maxLength != nil {
			field.maxLength = maxLength
		}

		minLength, err := getIntTag(ps.tag, minLengthTag)
		if err != nil {
			return err
		}

		if minLength != nil {
			field.minLength = minLength
		}
	}

	// json:"name,string" or json:",string"
	exampleTagValue, ok := ps.tag.Lookup(exampleTag)
	if ok {
		field.exampleValue = exampleTagValue

		if !strings.Contains(jsonTagValue, ",string") {
			example, err := defineTypeOfExample(field.schemaType, field.arrayType, exampleTagValue)
			if err != nil {
				return err
			}

			field.exampleValue = example
		}
	}

	// perform this after setting everything else (min, max, etc...)
	if strings.Contains(jsonTagValue, ",string") {
		// @encoding/json: "It applies only to fields of string, floating point, integer, or boolean types."
		defaultValues := map[string]string{
			// Zero Values as string
			STRING:  "",
			INTEGER: "0",
			BOOLEAN: "false",
			NUMBER:  "0",
		}

		defaultValue, ok := defaultValues[field.schemaType]
		if ok {
			field.schemaType = STRING
			*schema = *PrimitiveSchema(field.schemaType)

			if field.exampleValue == nil {
				// if exampleValue is not defined by the user,
				// we will force an example with a correct value
				// (eg: int->"0", bool:"false")
				field.exampleValue = defaultValue
			}
		}
	}

	if ps.field.Doc != nil {
		schema.Description = strings.TrimSpace(ps.field.Doc.Text())
	}

	if schema.Description == "" && ps.field.Comment != nil {
		schema.Description = strings.TrimSpace(ps.field.Comment.Text())
	}

	schema.ReadOnly = ps.tag.Get(readOnlyTag) == "true"

	defaultTagValue := ps.tag.Get(defaultTag)
	if defaultTagValue != "" {
		value, err := defineType(field.schemaType, defaultTagValue)
		if err != nil {
			return err
		}

		schema.Default = value
	}

	schema.Example = field.exampleValue

	if field.schemaType != ARRAY {
		schema.Format = field.formatType
	}

	extensionsTagValue := ps.tag.Get(extensionsTag)
	if extensionsTagValue != "" {
		schema.Extensions = setExtensionParam(extensionsTagValue)
	}

	varNamesTag := ps.tag.Get("x-enum-varnames")
	if varNamesTag != "" {
		varNames := strings.Split(varNamesTag, ",")
		if len(varNames) != len(field.enums) {
			return fmt.Errorf("invalid count of x-enum-varnames. expected %d, got %d", len(field.enums), len(varNames))
		}

		field.enumVarNames = nil

		for _, v := range varNames {
			field.enumVarNames = append(field.enumVarNames, v)
		}

		if field.schemaType == ARRAY {
			// Add the var names in the items schema
			if schema.Items.Schema.Extensions == nil {
				schema.Items.Schema.Extensions = map[string]interface{}{}
			}
			schema.Items.Schema.Extensions[enumVarNamesExtension] = field.enumVarNames
		} else {
			// Add to top level schema
			if schema.Extensions == nil {
				schema.Extensions = map[string]interface{}{}
			}
			schema.Extensions[enumVarNamesExtension] = field.enumVarNames
		}
	}

	eleSchema := schema

	if field.schemaType == ARRAY {
		// For Array only
		schema.MaxItems = field.maxItems
		schema.MinItems = field.minItems
		schema.UniqueItems = field.unique

		eleSchema = schema.Items.Schema
		eleSchema.Format = field.formatType
	}

	eleSchema.Maximum = field.maximum
	eleSchema.Minimum = field.minimum
	eleSchema.MultipleOf = field.multipleOf
	eleSchema.MaxLength = field.maxLength
	eleSchema.MinLength = field.minLength
	eleSchema.Enum = field.enums

	return nil
}

func getFloatTag(structTag reflect.StructTag, tagName string) (*float64, error) {
	strValue := structTag.Get(tagName)
	if strValue == "" {
		return nil, nil
	}

	value, err := strconv.ParseFloat(strValue, 64)
	if err != nil {
		return nil, fmt.Errorf("can't parse numeric value of %q tag: %v", tagName, err)
	}

	return &value, nil
}

func getIntTag(structTag reflect.StructTag, tagName string) (*int64, error) {
	strValue := structTag.Get(tagName)
	if strValue == "" {
		return nil, nil
	}

	value, err := strconv.ParseInt(strValue, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("can't parse numeric value of %q tag: %v", tagName, err)
	}

	return &value, nil
}

func (ps *tagBaseFieldParser) IsRequired() (bool, error) {
	if ps.field.Tag == nil {
		return false, nil
	}

	bindingTag := ps.tag.Get(bindingTag)
	if bindingTag != "" {
		for _, val := range strings.Split(bindingTag, ",") {
			switch val {
			case requiredLabel:
				return true, nil
			case optionalLabel:
				return false, nil
			}
		}
	}

	validateTag := ps.tag.Get(validateTag)
	if validateTag != "" {
		for _, val := range strings.Split(validateTag, ",") {
			switch val {
			case requiredLabel:
				return true, nil
			case optionalLabel:
				return false, nil
			}
		}
	}

	return ps.p.RequiredByDefault, nil
}

func parseValidTags(validTag string, sf *structField) {
	// `validate:"required,max=10,min=1"`
	// ps. required checked by IsRequired().
	for _, val := range strings.Split(validTag, ",") {
		var (
			valValue string
			keyVal   = strings.Split(val, "=")
		)

		switch len(keyVal) {
		case 1:
		case 2:
			valValue = strings.ReplaceAll(strings.ReplaceAll(keyVal[1], utf8HexComma, ","), utf8Pipe, "|")
		default:
			continue
		}

		switch keyVal[0] {
		case "max", "lte":
			sf.setMax(valValue)
		case "min", "gte":
			sf.setMin(valValue)
		case "oneof":
			sf.setOneOf(valValue)
		case "unique":
			if sf.schemaType == ARRAY {
				sf.unique = true
			}
		case "dive":
			// ignore dive
			return
		default:
			continue
		}
	}
}

func parseEnumTags(enumTag string, field *structField) error {
	enumType := field.schemaType
	if field.schemaType == ARRAY {
		enumType = field.arrayType
	}

	field.enums = nil

	for _, e := range strings.Split(enumTag, ",") {
		value, err := defineType(enumType, e)
		if err != nil {
			return err
		}

		field.enums = append(field.enums, value)
	}

	return nil
}

func (sf *structField) setOneOf(valValue string) {
	if len(sf.enums) != 0 {
		return
	}

	enumType := sf.schemaType
	if sf.schemaType == ARRAY {
		enumType = sf.arrayType
	}

	valValues := parseOneOfParam2(valValue)
	for i := range valValues {
		value, err := defineType(enumType, valValues[i])
		if err != nil {
			continue
		}

		sf.enums = append(sf.enums, value)
	}
}

func (sf *structField) setMin(valValue string) {
	value, err := strconv.ParseFloat(valValue, 64)
	if err != nil {
		return
	}

	switch sf.schemaType {
	case INTEGER, NUMBER:
		sf.minimum = &value
	case STRING:
		intValue := int64(value)
		sf.minLength = &intValue
	case ARRAY:
		intValue := int64(value)
		sf.minItems = &intValue
	}
}

func (sf *structField) setMax(valValue string) {
	value, err := strconv.ParseFloat(valValue, 64)
	if err != nil {
		return
	}

	switch sf.schemaType {
	case INTEGER, NUMBER:
		sf.maximum = &value
	case STRING:
		intValue := int64(value)
		sf.maxLength = &intValue
	case ARRAY:
		intValue := int64(value)
		sf.maxItems = &intValue
	}
}

const (
	utf8HexComma = "0x2C"
	utf8Pipe     = "0x7C"
)

// These code copy from
// https://github.com/go-playground/validator/blob/d4271985b44b735c6f76abc7a06532ee997f9476/baked_in.go#L207
// ---.
var oneofValsCache = map[string][]string{}
var oneofValsCacheRWLock = sync.RWMutex{}
var splitParamsRegex = regexp.MustCompile(`'[^']*'|\S+`)

func parseOneOfParam2(param string) []string {
	oneofValsCacheRWLock.RLock()
	values, ok := oneofValsCache[param]
	oneofValsCacheRWLock.RUnlock()

	if !ok {
		oneofValsCacheRWLock.Lock()
		values = splitParamsRegex.FindAllString(param, -1)

		for i := 0; i < len(values); i++ {
			values[i] = strings.ReplaceAll(values[i], "'", "")
		}

		oneofValsCache[param] = values

		oneofValsCacheRWLock.Unlock()
	}

	return values
}

// ---.
