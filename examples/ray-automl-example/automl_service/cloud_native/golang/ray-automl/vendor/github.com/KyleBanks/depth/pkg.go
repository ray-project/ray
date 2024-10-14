package depth

import (
	"bytes"
	"go/build"
	"path"
	"sort"
	"strings"
)

// Pkg represents a Go source package, and its dependencies.
type Pkg struct {
	Name   string `json:"name"`
	SrcDir string `json:"-"`

	Internal bool `json:"internal"`
	Resolved bool `json:"resolved"`
	Test     bool `json:"-"`

	Tree   *Tree `json:"-"`
	Parent *Pkg  `json:"-"`
	Deps   []Pkg `json:"deps"`

	Raw *build.Package `json:"-"`
}

// Resolve recursively finds all dependencies for the Pkg and the packages it depends on.
func (p *Pkg) Resolve(i Importer) {
	// Resolved is always true, regardless of if we skip the import,
	// it is only false if there is an error while importing.
	p.Resolved = true

	name := p.cleanName()
	if name == "" {
		return
	}

	// Stop resolving imports if we've reached max depth or found a duplicate.
	var importMode build.ImportMode
	if p.Tree.hasSeenImport(name) || p.Tree.isAtMaxDepth(p) {
		importMode = build.FindOnly
	}

	pkg, err := i.Import(name, p.SrcDir, importMode)
	if err != nil {
		// TODO: Check the error type?
		p.Resolved = false
		return
	}
	p.Raw = pkg

	// Update the name with the fully qualified import path.
	p.Name = pkg.ImportPath

	// If this is an internal dependency, we may need to skip it.
	if pkg.Goroot {
		p.Internal = true
		if !p.Tree.shouldResolveInternal(p) {
			return
		}
	}

	//first we set the regular dependencies, then we add the test dependencies
	//sharing the same set. This allows us to mark all test-only deps linearly
	unique := make(map[string]struct{})
	p.setDeps(i, pkg.Imports, pkg.Dir, unique, false)
	if p.Tree.ResolveTest {
		p.setDeps(i, append(pkg.TestImports, pkg.XTestImports...), pkg.Dir, unique, true)
	}
}

// setDeps takes a slice of import paths and the source directory they are relative to,
// and creates the Deps of the Pkg. Each dependency is also further resolved prior to being added
// to the Pkg.
func (p *Pkg) setDeps(i Importer, imports []string, srcDir string, unique map[string]struct{}, isTest bool) {
	for _, imp := range imports {
		// Mostly for testing files where cyclic imports are allowed.
		if imp == p.Name {
			continue
		}

		// Skip duplicates.
		if _, ok := unique[imp]; ok {
			continue
		}
		unique[imp] = struct{}{}

		p.addDep(i, imp, srcDir, isTest)
	}

	sort.Sort(byInternalAndName(p.Deps))
}

// addDep creates a Pkg and it's dependencies from an imported package name.
func (p *Pkg) addDep(i Importer, name string, srcDir string, isTest bool) {
	dep := Pkg{
		Name:   name,
		SrcDir: srcDir,
		Tree:   p.Tree,
		Parent: p,
		Test:   isTest,
	}
	dep.Resolve(i)

	p.Deps = append(p.Deps, dep)
}

// isParent goes recursively up the chain of Pkgs to determine if the name provided is ever a
// parent of the current Pkg.
func (p *Pkg) isParent(name string) bool {
	if p.Parent == nil {
		return false
	}

	if p.Parent.Name == name {
		return true
	}

	return p.Parent.isParent(name)
}

// depth returns the depth of the Pkg within the Tree.
func (p *Pkg) depth() int {
	if p.Parent == nil {
		return 0
	}

	return p.Parent.depth() + 1
}

// cleanName returns a cleaned version of the Pkg name used for resolving dependencies.
//
// If an empty string is returned, dependencies should not be resolved.
func (p *Pkg) cleanName() string {
	name := p.Name

	// C 'package' cannot be resolved.
	if name == "C" {
		return ""
	}

	// Internal golang_org/* packages must be prefixed with vendor/
	//
	// Thanks to @davecheney for this:
	// https://github.com/davecheney/graphpkg/blob/master/main.go#L46
	if strings.HasPrefix(name, "golang_org") {
		name = path.Join("vendor", name)
	}

	return name
}

// String returns a string representation of the Pkg containing the Pkg name and status.
func (p *Pkg) String() string {
	b := bytes.NewBufferString(p.Name)

	if !p.Resolved {
		b.Write([]byte(" (unresolved)"))
	}

	return b.String()
}

// byInternalAndName ensures a slice of Pkgs are sorted such that the internal stdlib
// packages are always above external packages (ie. github.com/whatever).
type byInternalAndName []Pkg

func (b byInternalAndName) Len() int {
	return len(b)
}

func (b byInternalAndName) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byInternalAndName) Less(i, j int) bool {
	if b[i].Internal && !b[j].Internal {
		return true
	} else if !b[i].Internal && b[j].Internal {
		return false
	}

	return b[i].Name < b[j].Name
}
