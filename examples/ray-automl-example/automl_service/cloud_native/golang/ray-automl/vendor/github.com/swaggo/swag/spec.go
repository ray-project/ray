package swag

import (
	"bytes"
	"encoding/json"
	"strings"
	"text/template"
)

// Spec holds exported Swagger Info so clients can modify it.
type Spec struct {
	Version          string
	Host             string
	BasePath         string
	Schemes          []string
	Title            string
	Description      string
	InfoInstanceName string
	SwaggerTemplate  string
}

// ReadDoc parses SwaggerTemplate into swagger document.
func (i *Spec) ReadDoc() string {
	i.Description = strings.ReplaceAll(i.Description, "\n", "\\n")

	tpl, err := template.New("swagger_info").Funcs(template.FuncMap{
		"marshal": func(v interface{}) string {
			a, _ := json.Marshal(v)

			return string(a)
		},
		"escape": func(v interface{}) string {
			// escape tabs
			var str = strings.ReplaceAll(v.(string), "\t", "\\t")
			// replace " with \", and if that results in \\", replace that with \\\"
			str = strings.ReplaceAll(str, "\"", "\\\"")

			return strings.ReplaceAll(str, "\\\\\"", "\\\\\\\"")
		},
	}).Parse(i.SwaggerTemplate)
	if err != nil {
		return i.SwaggerTemplate
	}

	var doc bytes.Buffer
	if err = tpl.Execute(&doc, i); err != nil {
		return i.SwaggerTemplate
	}

	return doc.String()
}

// InstanceName returns Spec instance name.
func (i *Spec) InstanceName() string {
	return i.InfoInstanceName
}
