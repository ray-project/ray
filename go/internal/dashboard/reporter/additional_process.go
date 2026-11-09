package reporter

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// additionalProcessComponents fixes the component iteration order. Python
// iterates the _ADDITIONAL_PROCESS_MATCHERS dict in insertion order, so a
// process matching two component definitions is grouped under the first one; the
// slice keeps that order explicit and deterministic.
var additionalProcessComponents = []string{"log_monitor", "runtime_env_agent"}

// additionalProcessMatchers maps a component name to its argv predicates,
// mirroring Python _ADDITIONAL_PROCESS_MATCHERS. A process belongs to the
// component when any of its predicates returns true.
var additionalProcessMatchers = map[string][]func([]string) bool{
	"log_monitor": {
		// Python log_monitor: `python -u .../log_monitor.py --session-dir=...`
		func(argv []string) bool {
			return argvIsScriptWithFlag(argv, "log_monitor.py", "--session-dir=")
		},
		// C++ log_monitor: `.../ray/log_monitor/log_monitor --session-dir=...`
		func(argv []string) bool {
			return argvIsScriptWithFlag(argv, "ray/log_monitor/log_monitor", "--session-dir=")
		},
	},
	"runtime_env_agent": {
		// `python .../runtime_env/agent/main.py --runtime-env-agent-port=...`
		func(argv []string) bool {
			return argvIsScriptWithFlag(argv, "runtime_env/agent/main.py", "--runtime-env-agent-port=")
		},
	},
}

// argvIsScriptWithFlag mirrors Python _argv_is_script_with_flag: it locates the
// first token containing scriptSubstr and then reports whether any token from
// that position on starts with flagPrefix.
//
// Matching whole argv tokens rather than a joined cmdline is required: the raylet
// embeds child commands (--python_worker_command, --runtime_env_agent_command,
// ...) as quoted values inside a single one of its own argv elements, so a
// substring match on the joined cmdline would misclassify the raylet itself as
// log_monitor / runtime_env_agent and fold its pid and memory into those
// components.
func argvIsScriptWithFlag(argv []string, scriptSubstr, flagPrefix string) bool {
	for i, token := range argv {
		if !strings.Contains(token, scriptSubstr) {
			continue
		}
		for _, later := range argv[i:] {
			if strings.HasPrefix(later, flagPrefix) {
				return true
			}
		}
		return false
	}
	return false
}

// additional scans /proc for the auxiliary Ray processes (log_monitor,
// runtime_env_agent) and returns their stats grouped by component name,
// mirroring Python _get_additional_processes. A component with no live process
// is absent from the result rather than present with an empty list.
func (s *systemSource) additional() (map[string]interface{}, error) {
	grouped := map[string][]map[string]interface{}{}

	for _, pid := range procPIDs() {
		cmd, err := readProcFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
		if err != nil {
			// The process exited between the /proc walk and this read.
			continue
		}
		argv := splitCmdline(cmd)
		if len(argv) == 0 {
			continue
		}

		for _, component := range additionalProcessComponents {
			if matchesAnyArgv(additionalProcessMatchers[component], argv) {
				grouped[component] = append(grouped[component], s.procAttrs(pid))
				break
			}
		}
	}

	result := make(map[string]interface{}, len(grouped))
	for component, stats := range grouped {
		result[component] = stats
	}
	return result, nil
}

// procPIDs returns the numeric process ids under /proc in ascending order. The
// order matches psutil.process_iter closely enough to keep the per-component
// process list stable across cycles.
func procPIDs() []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return []int{}
	}
	pids := make([]int, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	return pids
}

// matchesAnyArgv reports whether any predicate accepts the argv.
func matchesAnyArgv(matchers []func([]string) bool, argv []string) bool {
	for _, match := range matchers {
		if match(argv) {
			return true
		}
	}
	return false
}
