// TODO(hjiang): This is an example file which demonstrates opentelemetry dependency is
// correct, should be replaced with real metrics exporter implementation.

#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/version/version.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/scope.h"
#include "opentelemetry/trace/tracer.h"
#include "opentelemetry/trace/tracer_provider.h"

namespace trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;

namespace {
nostd::shared_ptr<trace::Tracer> get_tracer() {
  auto provider = trace::Provider::GetTracerProvider();
  return provider->GetTracer("foo_library", OPENTELEMETRY_SDK_VERSION);
}

void f1() { auto scoped_span = trace::Scope(get_tracer()->StartSpan("f1")); }

void f2() {
  auto scoped_span = trace::Scope(get_tracer()->StartSpan("f2"));

  f1();
  f1();
}
}  // namespace

void foo_library() {
  auto scoped_span = trace::Scope(get_tracer()->StartSpan("library"));

  f2();
}
