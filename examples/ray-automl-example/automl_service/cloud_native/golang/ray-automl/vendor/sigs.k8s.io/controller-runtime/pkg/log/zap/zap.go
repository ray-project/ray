/*
Copyright 2019 The Kubernetes Authors.

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

// Package zap contains helpers for setting up a new logr.Logger instance
// using the Zap logging framework.
package zap

import (
	"flag"
	"io"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// EncoderConfigOption is a function that can modify a `zapcore.EncoderConfig`.
type EncoderConfigOption func(*zapcore.EncoderConfig)

// NewEncoderFunc is a function that creates an Encoder using the provided EncoderConfigOptions.
type NewEncoderFunc func(...EncoderConfigOption) zapcore.Encoder

// New returns a brand new Logger configured with Opts. It
// uses KubeAwareEncoder which adds Type information and
// Namespace/Name to the log.
func New(opts ...Opts) logr.Logger {
	return zapr.NewLogger(NewRaw(opts...))
}

// Opts allows to manipulate Options.
type Opts func(*Options)

// UseDevMode sets the logger to use (or not use) development mode (more
// human-readable output, extra stack traces and logging information, etc).
// See Options.Development.
func UseDevMode(enabled bool) Opts {
	return func(o *Options) {
		o.Development = enabled
	}
}

// WriteTo configures the logger to write to the given io.Writer, instead of standard error.
// See Options.DestWriter.
func WriteTo(out io.Writer) Opts {
	return func(o *Options) {
		o.DestWriter = out
	}
}

// Encoder configures how the logger will encode the output e.g JSON or console.
// See Options.Encoder.
func Encoder(encoder zapcore.Encoder) func(o *Options) {
	return func(o *Options) {
		o.Encoder = encoder
	}
}

// JSONEncoder configures the logger to use a JSON Encoder.
func JSONEncoder(opts ...EncoderConfigOption) func(o *Options) {
	return func(o *Options) {
		o.Encoder = newJSONEncoder(opts...)
	}
}

func newJSONEncoder(opts ...EncoderConfigOption) zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	for _, opt := range opts {
		opt(&encoderConfig)
	}
	return zapcore.NewJSONEncoder(encoderConfig)
}

// ConsoleEncoder configures the logger to use a Console encoder.
func ConsoleEncoder(opts ...EncoderConfigOption) func(o *Options) {
	return func(o *Options) {
		o.Encoder = newConsoleEncoder(opts...)
	}
}

func newConsoleEncoder(opts ...EncoderConfigOption) zapcore.Encoder {
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	for _, opt := range opts {
		opt(&encoderConfig)
	}
	return zapcore.NewConsoleEncoder(encoderConfig)
}

// Level sets Options.Level, which configures the the minimum enabled logging level e.g Debug, Info.
// A zap log level should be multiplied by -1 to get the logr verbosity.
// For example, to get logr verbosity of 3, pass zapcore.Level(-3) to this Opts.
// See https://pkg.go.dev/github.com/go-logr/zapr for how zap level relates to logr verbosity.
func Level(level zapcore.LevelEnabler) func(o *Options) {
	return func(o *Options) {
		o.Level = level
	}
}

// StacktraceLevel sets Options.StacktraceLevel, which configures the logger to record a stack trace
// for all messages at or above a given level.
// See the Level Opts for the relationship of zap log level to logr verbosity.
func StacktraceLevel(stacktraceLevel zapcore.LevelEnabler) func(o *Options) {
	return func(o *Options) {
		o.StacktraceLevel = stacktraceLevel
	}
}

// RawZapOpts allows appending arbitrary zap.Options to configure the underlying zap logger.
// See Options.ZapOpts.
func RawZapOpts(zapOpts ...zap.Option) func(o *Options) {
	return func(o *Options) {
		o.ZapOpts = append(o.ZapOpts, zapOpts...)
	}
}

// Options contains all possible settings.
type Options struct {
	// Development configures the logger to use a Zap development config
	// (stacktraces on warnings, no sampling), otherwise a Zap production
	// config will be used (stacktraces on errors, sampling).
	Development bool
	// Encoder configures how Zap will encode the output.  Defaults to
	// console when Development is true and JSON otherwise
	Encoder zapcore.Encoder
	// EncoderConfigOptions can modify the EncoderConfig needed to initialize an Encoder.
	// See https://pkg.go.dev/go.uber.org/zap/zapcore#EncoderConfig for the list of options
	// that can be configured.
	// Note that the EncoderConfigOptions are not applied when the Encoder option is already set.
	EncoderConfigOptions []EncoderConfigOption
	// NewEncoder configures Encoder using the provided EncoderConfigOptions.
	// Note that the NewEncoder function is not used when the Encoder option is already set.
	NewEncoder NewEncoderFunc
	// DestWriter controls the destination of the log output.  Defaults to
	// os.Stderr.
	DestWriter io.Writer
	// DestWritter controls the destination of the log output.  Defaults to
	// os.Stderr.
	//
	// Deprecated: Use DestWriter instead
	DestWritter io.Writer
	// Level configures the verbosity of the logging.
	// Defaults to Debug when Development is true and Info otherwise.
	// A zap log level should be multiplied by -1 to get the logr verbosity.
	// For example, to get logr verbosity of 3, set this field to zapcore.Level(-3).
	// See https://pkg.go.dev/github.com/go-logr/zapr for how zap level relates to logr verbosity.
	Level zapcore.LevelEnabler
	// StacktraceLevel is the level at and above which stacktraces will
	// be recorded for all messages. Defaults to Warn when Development
	// is true and Error otherwise.
	// See Level for the relationship of zap log level to logr verbosity.
	StacktraceLevel zapcore.LevelEnabler
	// ZapOpts allows passing arbitrary zap.Options to configure on the
	// underlying Zap logger.
	ZapOpts []zap.Option
	// TimeEncoder specifies the encoder for the timestamps in log messages.
	// Defaults to EpochTimeEncoder as this is the default in Zap currently.
	TimeEncoder zapcore.TimeEncoder
}

// addDefaults adds defaults to the Options.
func (o *Options) addDefaults() {
	if o.DestWriter == nil && o.DestWritter == nil {
		o.DestWriter = os.Stderr
	} else if o.DestWriter == nil && o.DestWritter != nil {
		// while misspelled DestWritter is deprecated but still not removed
		o.DestWriter = o.DestWritter
	}

	if o.Development {
		if o.NewEncoder == nil {
			o.NewEncoder = newConsoleEncoder
		}
		if o.Level == nil {
			lvl := zap.NewAtomicLevelAt(zap.DebugLevel)
			o.Level = &lvl
		}
		if o.StacktraceLevel == nil {
			lvl := zap.NewAtomicLevelAt(zap.WarnLevel)
			o.StacktraceLevel = &lvl
		}
		o.ZapOpts = append(o.ZapOpts, zap.Development())
	} else {
		if o.NewEncoder == nil {
			o.NewEncoder = newJSONEncoder
		}
		if o.Level == nil {
			lvl := zap.NewAtomicLevelAt(zap.InfoLevel)
			o.Level = &lvl
		}
		if o.StacktraceLevel == nil {
			lvl := zap.NewAtomicLevelAt(zap.ErrorLevel)
			o.StacktraceLevel = &lvl
		}
		// Disable sampling for increased Debug levels. Otherwise, this will
		// cause index out of bounds errors in the sampling code.
		if !o.Level.Enabled(zapcore.Level(-2)) {
			o.ZapOpts = append(o.ZapOpts,
				zap.WrapCore(func(core zapcore.Core) zapcore.Core {
					return zapcore.NewSamplerWithOptions(core, time.Second, 100, 100)
				}))
		}
	}

	if o.TimeEncoder == nil {
		o.TimeEncoder = zapcore.EpochTimeEncoder
	}
	f := func(ecfg *zapcore.EncoderConfig) {
		ecfg.EncodeTime = o.TimeEncoder
	}
	// prepend instead of append it in case someone adds a time encoder option in it
	o.EncoderConfigOptions = append([]EncoderConfigOption{f}, o.EncoderConfigOptions...)

	if o.Encoder == nil {
		o.Encoder = o.NewEncoder(o.EncoderConfigOptions...)
	}
	o.ZapOpts = append(o.ZapOpts, zap.AddStacktrace(o.StacktraceLevel))
}

// NewRaw returns a new zap.Logger configured with the passed Opts
// or their defaults. It uses KubeAwareEncoder which adds Type
// information and Namespace/Name to the log.
func NewRaw(opts ...Opts) *zap.Logger {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}
	o.addDefaults()

	// this basically mimics New<type>Config, but with a custom sink
	sink := zapcore.AddSync(o.DestWriter)

	o.ZapOpts = append(o.ZapOpts, zap.ErrorOutput(sink))
	log := zap.New(zapcore.NewCore(&KubeAwareEncoder{Encoder: o.Encoder, Verbose: o.Development}, sink, o.Level))
	log = log.WithOptions(o.ZapOpts...)
	return log
}

// BindFlags will parse the given flagset for zap option flags and set the log options accordingly:
//   - zap-devel:
//     Development Mode defaults(encoder=consoleEncoder,logLevel=Debug,stackTraceLevel=Warn)
//     Production Mode defaults(encoder=jsonEncoder,logLevel=Info,stackTraceLevel=Error)
//   - zap-encoder: Zap log encoding (one of 'json' or 'console')
//   - zap-log-level: Zap Level to configure the verbosity of logging. Can be one of 'debug', 'info', 'error',
//     or any integer value > 0 which corresponds to custom debug levels of increasing verbosity").
//   - zap-stacktrace-level: Zap Level at and above which stacktraces are captured (one of 'info', 'error' or 'panic')
//   - zap-time-encoding: Zap time encoding (one of 'epoch', 'millis', 'nano', 'iso8601', 'rfc3339' or 'rfc3339nano'),
//     Defaults to 'epoch'.
func (o *Options) BindFlags(fs *flag.FlagSet) {
	// Set Development mode value
	fs.BoolVar(&o.Development, "zap-devel", o.Development,
		"Development Mode defaults(encoder=consoleEncoder,logLevel=Debug,stackTraceLevel=Warn). "+
			"Production Mode defaults(encoder=jsonEncoder,logLevel=Info,stackTraceLevel=Error)")

	// Set Encoder value
	var encVal encoderFlag
	encVal.setFunc = func(fromFlag NewEncoderFunc) {
		o.NewEncoder = fromFlag
	}
	fs.Var(&encVal, "zap-encoder", "Zap log encoding (one of 'json' or 'console')")

	// Set the Log Level
	var levelVal levelFlag
	levelVal.setFunc = func(fromFlag zapcore.LevelEnabler) {
		o.Level = fromFlag
	}
	fs.Var(&levelVal, "zap-log-level",
		"Zap Level to configure the verbosity of logging. Can be one of 'debug', 'info', 'error', "+
			"or any integer value > 0 which corresponds to custom debug levels of increasing verbosity")

	// Set the StrackTrace Level
	var stackVal stackTraceFlag
	stackVal.setFunc = func(fromFlag zapcore.LevelEnabler) {
		o.StacktraceLevel = fromFlag
	}
	fs.Var(&stackVal, "zap-stacktrace-level",
		"Zap Level at and above which stacktraces are captured (one of 'info', 'error', 'panic').")

	// Set the time encoding
	var timeEncoderVal timeEncodingFlag
	timeEncoderVal.setFunc = func(fromFlag zapcore.TimeEncoder) {
		o.TimeEncoder = fromFlag
	}
	fs.Var(&timeEncoderVal, "zap-time-encoding", "Zap time encoding (one of 'epoch', 'millis', 'nano', 'iso8601', 'rfc3339' or 'rfc3339nano'). Defaults to 'epoch'.")
}

// UseFlagOptions configures the logger to use the Options set by parsing zap option flags from the CLI.
//
//	opts := zap.Options{}
//	opts.BindFlags(flag.CommandLine)
//	flag.Parse()
//	log := zap.New(zap.UseFlagOptions(&opts))
func UseFlagOptions(in *Options) Opts {
	return func(o *Options) {
		*o = *in
	}
}
