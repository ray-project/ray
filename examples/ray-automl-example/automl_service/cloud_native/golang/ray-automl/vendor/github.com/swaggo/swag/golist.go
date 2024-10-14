package swag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/build"
	"os/exec"
	"path/filepath"
)

func listPackages(ctx context.Context, dir string, env []string, args ...string) (pkgs []*build.Package, finalErr error) {
	cmd := exec.CommandContext(ctx, "go", append([]string{"list", "-json", "-e"}, args...)...)
	cmd.Env = env
	cmd.Dir = dir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	defer func() {
		if stderrBuf.Len() > 0 {
			finalErr = fmt.Errorf("%v\n%s", finalErr, stderrBuf.Bytes())
		}
	}()

	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(stdout)
	for dec.More() {
		var pkg build.Package
		err = dec.Decode(&pkg)
		if err != nil {
			return nil, err
		}
		pkgs = append(pkgs, &pkg)
	}
	err = cmd.Wait()
	if err != nil {
		return nil, err
	}
	return pkgs, nil
}

func (parser *Parser) getAllGoFileInfoFromDepsByList(pkg *build.Package) error {
	ignoreInternal := pkg.Goroot && !parser.ParseInternal
	if ignoreInternal { // ignored internal
		return nil
	}

	srcDir := pkg.Dir
	var err error
	for i := range pkg.GoFiles {
		err = parser.parseFile(pkg.ImportPath, filepath.Join(srcDir, pkg.GoFiles[i]), nil, ParseModels)
		if err != nil {
			return err
		}
	}

	// parse .go source files that import "C"
	for i := range pkg.CgoFiles {
		err = parser.parseFile(pkg.ImportPath, filepath.Join(srcDir, pkg.CgoFiles[i]), nil, ParseModels)
		if err != nil {
			return err
		}
	}

	return nil
}
