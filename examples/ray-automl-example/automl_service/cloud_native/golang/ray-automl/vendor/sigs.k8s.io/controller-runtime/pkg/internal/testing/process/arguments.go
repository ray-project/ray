/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package process

import (
	"bytes"
	"html/template"
	"sort"
	"strings"
)

// RenderTemplates returns an []string to render the templates
//
// Deprecated: will be removed in favor of Arguments.
func RenderTemplates(argTemplates []string, data interface{}) (args []string, err error) {
	var t *template.Template

	for _, arg := range argTemplates {
		t, err = template.New(arg).Parse(arg)
		if err != nil {
			args = nil
			return
		}

		buf := &bytes.Buffer{}
		err = t.Execute(buf, data)
		if err != nil {
			args = nil
			return
		}
		args = append(args, buf.String())
	}

	return
}

// SliceToArguments converts a slice of arguments to structured arguments,
// appending each argument that starts with `--` and contains an `=` to the
// argument set (ignoring defaults), returning the rest.
//
// Deprecated: will be removed when RenderTemplates is removed.
func SliceToArguments(sliceArgs []string, args *Arguments) []string {
	var rest []string
	for i, arg := range sliceArgs {
		if arg == "--" {
			rest = append(rest, sliceArgs[i:]...)
			return rest
		}
		// skip non-flag arguments, skip arguments w/o equals because we
		// can't tell if the next argument should take a value
		if !strings.HasPrefix(arg, "--") || !strings.Contains(arg, "=") {
			rest = append(rest, arg)
			continue
		}

		parts := strings.SplitN(arg[2:], "=", 2)
		name := parts[0]
		val := parts[1]

		args.AppendNoDefaults(name, val)
	}

	return rest
}

// TemplateDefaults specifies defaults to be used for joining structured arguments with templates.
//
// Deprecated: will be removed when RenderTemplates is removed.
type TemplateDefaults struct {
	// Data will be used to render the template.
	Data interface{}
	// Defaults will be used to default structured arguments if no template is passed.
	Defaults map[string][]string
	// MinimalDefaults will be used to default structured arguments if a template is passed.
	// Use this for flags which *must* be present.
	MinimalDefaults map[string][]string // for api server service-cluster-ip-range
}

// TemplateAndArguments joins structured arguments and non-structured arguments, preserving existing
// behavior.  Namely:
//
//  1. if templ has len > 0, it will be rendered against data
//  2. the rendered template values that look like `--foo=bar` will be split
//     and appended to args, the rest will be kept around
//  3. the given args will be rendered as string form.  If a template is given,
//     no defaults will be used, otherwise defaults will be used
//  4. a result of [args..., rest...] will be returned
//
// It returns the resulting rendered arguments, plus the arguments that were
// not transferred to `args` during rendering.
//
// Deprecated: will be removed when RenderTemplates is removed.
func TemplateAndArguments(templ []string, args *Arguments, data TemplateDefaults) (allArgs []string, nonFlagishArgs []string, err error) {
	if len(templ) == 0 { // 3 & 4 (no template case)
		return args.AsStrings(data.Defaults), nil, nil
	}

	// 1: render the template
	rendered, err := RenderTemplates(templ, data.Data)
	if err != nil {
		return nil, nil, err
	}

	// 2: filter out structured args and add them to args
	rest := SliceToArguments(rendered, args)

	// 3 (template case): render structured args, no defaults (matching the
	// legacy case where if Args was specified, no defaults were used)
	res := args.AsStrings(data.MinimalDefaults)

	// 4: return the rendered structured args + all non-structured args
	return append(res, rest...), rest, nil
}

// EmptyArguments constructs an empty set of flags with no defaults.
func EmptyArguments() *Arguments {
	return &Arguments{
		values: make(map[string]Arg),
	}
}

// Arguments are structured, overridable arguments.
// Each Arguments object contains some set of default arguments, which may
// be appended to, or overridden.
//
// When ready, you can serialize them to pass to exec.Command and friends using
// AsStrings.
//
// All flag-setting methods return the *same* instance of Arguments so that you
// can chain calls.
type Arguments struct {
	// values contains the user-set values for the arguments.
	// `values[key] = dontPass` means "don't pass this flag"
	// `values[key] = passAsName` means "pass this flag without args like --key`
	// `values[key] = []string{a, b, c}` means "--key=a --key=b --key=c`
	// any values not explicitly set here will be copied from defaults on final rendering.
	values map[string]Arg
}

// Arg is an argument that has one or more values,
// and optionally falls back to default values.
type Arg interface {
	// Append adds new values to this argument, returning
	// a new instance contain the new value.  The intermediate
	// argument should generally be assumed to be consumed.
	Append(vals ...string) Arg
	// Get returns the full set of values, optionally including
	// the passed in defaults.  If it returns nil, this will be
	// skipped.  If it returns a non-nil empty slice, it'll be
	// assumed that the argument should be passed as name-only.
	Get(defaults []string) []string
}

type userArg []string

func (a userArg) Append(vals ...string) Arg {
	return userArg(append(a, vals...)) //nolint:unconvert
}
func (a userArg) Get(_ []string) []string {
	return []string(a)
}

type defaultedArg []string

func (a defaultedArg) Append(vals ...string) Arg {
	return defaultedArg(append(a, vals...)) //nolint:unconvert
}
func (a defaultedArg) Get(defaults []string) []string {
	res := append([]string(nil), defaults...)
	return append(res, a...)
}

type dontPassArg struct{}

func (a dontPassArg) Append(vals ...string) Arg {
	return userArg(vals)
}
func (dontPassArg) Get(_ []string) []string {
	return nil
}

type passAsNameArg struct{}

func (a passAsNameArg) Append(_ ...string) Arg {
	return passAsNameArg{}
}
func (passAsNameArg) Get(_ []string) []string {
	return []string{}
}

var (
	// DontPass indicates that the given argument will not actually be
	// rendered.
	DontPass Arg = dontPassArg{}
	// PassAsName indicates that the given flag will be passed as `--key`
	// without any value.
	PassAsName Arg = passAsNameArg{}
)

// AsStrings serializes this set of arguments to a slice of strings appropriate
// for passing to exec.Command and friends, making use of the given defaults
// as indicated for each particular argument.
//
//   - Any flag in defaults that's not in Arguments will be present in the output
//   - Any flag that's present in Arguments will be passed the corresponding
//     defaults to do with as it will (ignore, append-to, suppress, etc).
func (a *Arguments) AsStrings(defaults map[string][]string) []string {
	// sort for deterministic ordering
	keysInOrder := make([]string, 0, len(defaults)+len(a.values))
	for key := range defaults {
		if _, userSet := a.values[key]; userSet {
			continue
		}
		keysInOrder = append(keysInOrder, key)
	}
	for key := range a.values {
		keysInOrder = append(keysInOrder, key)
	}
	sort.Strings(keysInOrder)

	var res []string
	for _, key := range keysInOrder {
		vals := a.Get(key).Get(defaults[key])
		switch {
		case vals == nil: // don't pass
			continue
		case len(vals) == 0: // pass as name
			res = append(res, "--"+key)
		default:
			for _, val := range vals {
				res = append(res, "--"+key+"="+val)
			}
		}
	}

	return res
}

// Get returns the value of the given flag.  If nil,
// it will not be passed in AsString, otherwise:
//
// len == 0 --> `--key`, len > 0 --> `--key=val1 --key=val2 ...`.
func (a *Arguments) Get(key string) Arg {
	if vals, ok := a.values[key]; ok {
		return vals
	}
	return defaultedArg(nil)
}

// Enable configures the given key to be passed as a "name-only" flag,
// like, `--key`.
func (a *Arguments) Enable(key string) *Arguments {
	a.values[key] = PassAsName
	return a
}

// Disable prevents this flag from be passed.
func (a *Arguments) Disable(key string) *Arguments {
	a.values[key] = DontPass
	return a
}

// Append adds additional values to this flag.  If this flag has
// yet to be set, initial values will include defaults.  If you want
// to intentionally ignore defaults/start from scratch, call AppendNoDefaults.
//
// Multiple values will look like `--key=value1 --key=value2 ...`.
func (a *Arguments) Append(key string, values ...string) *Arguments {
	vals, present := a.values[key]
	if !present {
		vals = defaultedArg{}
	}
	a.values[key] = vals.Append(values...)
	return a
}

// AppendNoDefaults adds additional values to this flag.  However,
// unlike Append, it will *not* copy values from defaults.
func (a *Arguments) AppendNoDefaults(key string, values ...string) *Arguments {
	vals, present := a.values[key]
	if !present {
		vals = userArg{}
	}
	a.values[key] = vals.Append(values...)
	return a
}

// Set resets the given flag to the specified values, ignoring any existing
// values or defaults.
func (a *Arguments) Set(key string, values ...string) *Arguments {
	a.values[key] = userArg(values)
	return a
}

// SetRaw sets the given flag to the given Arg value directly.  Use this if
// you need to do some complicated deferred logic or something.
//
// Otherwise behaves like Set.
func (a *Arguments) SetRaw(key string, val Arg) *Arguments {
	a.values[key] = val
	return a
}

// FuncArg is a basic implementation of Arg that can be used for custom argument logic,
// like pulling values out of APIServer, or dynamically calculating values just before
// launch.
//
// The given function will be mapped directly to Arg#Get, and will generally be
// used in conjunction with SetRaw.  For example, to set `--some-flag` to the
// API server's CertDir, you could do:
//
//	server.Configure().SetRaw("--some-flag", FuncArg(func(defaults []string) []string {
//	    return []string{server.CertDir}
//	}))
//
// FuncArg ignores Appends; if you need to support appending values too, consider implementing
// Arg directly.
type FuncArg func([]string) []string

// Append is a no-op for FuncArg, and just returns itself.
func (a FuncArg) Append(vals ...string) Arg { return a }

// Get delegates functionality to the FuncArg function itself.
func (a FuncArg) Get(defaults []string) []string {
	return a(defaults)
}
