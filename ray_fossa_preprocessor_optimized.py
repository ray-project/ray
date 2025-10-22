#!/usr/bin/env python3
"""
Ray C/C++ Dependency Analyzer - Optimized Version

This script analyzes Ray's C/C++ dependencies and generates package-level dependency reports.
It groups individual Bazel targets by package and creates isolated folders for Fossa analysis.

Key optimizations:
- Package-level grouping instead of individual targets
- Resolves external aliases to actual package names
- Deduplication with smart merging rules
- Single consolidated output file
- Fossa-ready folder structure

Usage:
    python ray_fossa_preprocessor_optimized.py --ray-root /path/to/ray --fossa-folder /path/to/fossa_analysis

Author: AI Assistant
Date: 2024
"""

import argparse
import json
import subprocess
import sys
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
import re

class OptimizedRayFossaPreprocessor:
    """Optimized Ray C/C++ dependency analyzer with package-level grouping"""
    
    # Deliverable binaries that need compliance analysis
    DELIVERABLE_TARGETS = [
        "//:gen_ray_pkg",
    ]
    
    # Build tools that can be optionally included
    BUILD_TOOLS = [
        "protoc", "cmake", "ninja", "bazel", "python", "cython",
        "rules_foreign_cc", "rules_proto", "rules_cc"
    ]
    
    def __init__(self, ray_root: str, 
                 fossa_folder: str,
                 transitive_depth: int = -1,
                 include_build_tools: bool = False,
                 include_test_deps: bool = False):
        self.ray_root = Path(ray_root)
        self.fossa_folder = Path(fossa_folder)
        self.transitive_depth = transitive_depth
        self.include_build_tools = include_build_tools
        self.include_test_deps = include_test_deps
        
        # Data storage - package-based
        self.packages = {}  # package_id -> package_info
        self.failed_resolutions = []
        
        # Create fossa folder if it doesn't exist
        self.fossa_folder.mkdir(parents=True, exist_ok=True)
        
    def run_bazel_query(self, target: str, depth: Optional[int] = None) -> List[str]:
        """Run bazel query with correct syntax"""
        if depth is None or depth == -1:
            cmd = ["bazel", "query", f"deps({target})", "--output=label"]
            depth_str = "all transitive"
        elif depth == 0:
            cmd = ["bazel", "query", f"deps({target}, 1)", "--output=label"]
            depth_str = "direct"
        else:
            cmd = ["bazel", "query", f"deps({target}, {depth})", "--output=label"]
            depth_str = f"depth {depth}"
        
        print(f"    Running bazel query for {target} ({depth_str} dependencies)...")
        
        try:
            result = subprocess.run(cmd, cwd=self.ray_root, 
                                  capture_output=True, text=True, check=True)
            deps = result.stdout.strip().split('\n')
            print(f"    Found {len(deps)} total dependencies")
            return deps
        except subprocess.CalledProcessError as e:
            print(f"Error running bazel query for {target}: {e}")
            return []
    
    def resolve_external_alias(self, target: str) -> Optional[str]:
        """Resolve external alias to actual package name using bazel query --output=build"""
        if not target.startswith('//external:'):
            return None
            
        try:
            cmd = ["bazel", "query", "--output=build", target]
            result = subprocess.run(cmd, cwd=self.ray_root, 
                                  capture_output=True, text=True, check=True)
            
            # Parse the build output to find the 'actual' key
            for line in result.stdout.split('\n'):
                if 'actual =' in line:
                    # Extract the actual package name
                    match = re.search(r'actual\s*=\s*["\']([^"\']+)["\']', line)
                    if match:
                        actual_package = match.group(1)
                        print(f"      Resolved {target} -> {actual_package}")
                        return actual_package
                    
            # If no 'actual' key found, try to extract from name
            for line in result.stdout.split('\n'):
                if 'name =' in line:
                    match = re.search(r'name\s*=\s*["\']([^"\']+)["\']', line)
                    if match:
                        actual_package = match.group(1)
                        print(f"      Resolved {target} -> {actual_package} (from name)")
                        return actual_package
                        
        except subprocess.CalledProcessError as e:
            print(f"      Failed to resolve {target}: {e}")
            
        return None
    
    def extract_package_id(self, target: str) -> str:
        """Extract package identifier from Bazel target"""
        if target.startswith('//external:'):
            # Resolve external alias
            resolved = self.resolve_external_alias(target)
            if resolved:
                # Extract package name from resolved target like @com_github_cares_cares//:ares
                if resolved.startswith('@'):
                    match = re.match(r'@([^/]+)//', resolved)
                    if match:
                        return match.group(1)
                return resolved
            else:
                # If resolution failed, use the alias name
                return target.replace('//external:', '')
        elif target.startswith('@'):
            # Extract repository name from @repo//path:target
            match = re.match(r'@([^/]+)//', target)
            if match:
                return match.group(1)
        elif target.startswith('//'):
            # Internal target, use the path but skip empty ones
            path_part = target.replace('//', '').split(':')[0]
            if path_part:
                return path_part.replace('/', '_')
            else:
                # For //:target format, use the target name
                return target.split(':')[-1]
        
        # Fallback to target name
        return target.split(':')[-1]
    
    def is_c_cpp_target(self, target: str) -> bool:
        """Determine if a target is C/C++ related using multiple heuristics"""
        # Skip Python files explicitly
        if target.endswith('.py') or 'python' in target.lower():
            return False
            
        # Skip other non-C/C++ patterns
        non_cpp_patterns = [
            'java', 'scala', 'kotlin', 'go_', 'rust_', 'cargo',
            'node', 'npm', 'yarn', 'webpack', 'babel', 'typescript',
            'pytest', 'wheel', 'setuptools', 'pip_'
        ]
        
        target_lower = target.lower()
        if any(pattern in target_lower for pattern in non_cpp_patterns):
            return False
        
        # Check for C/C++ file extensions in the target name
        c_cpp_extensions = ['.c', '.cc', '.cpp', '.cxx', '.c++', '.h', '.hpp', '.hxx']
        if any(target.endswith(ext) for ext in c_cpp_extensions):
            return True
        
        # Check for common C/C++ library indicators in target name
        c_cpp_indicators = [
            'lib', 'static', 'shared', 'archive', 'object',
            'protobuf', 'grpc', 'absl', 'boost', 'gtest', 'gflags',
            'zlib', 'openssl', 'boringssl', 'cares', 're2',
            'cc_', 'objc_', 'fdo_', 'propeller_'
        ]
        
        if any(indicator in target_lower for indicator in c_cpp_indicators):
            return True
        
        # For external targets, assume C/C++ unless proven otherwise
        if target.startswith('//external:') or target.startswith('@'):
            return True
        
        # For internal targets, be more selective
        if target.startswith('//'):
            # Skip obvious non-C/C++ internal targets
            skip_patterns = ['bazel', 'gen_', 'gen_ray_pkg.py']
            if any(pattern in target_lower for pattern in skip_patterns):
                return False
            return True
        
        return False
    
    def is_excluded_target(self, target: str) -> bool:
        """Check if target should be explicitly excluded (non-C/C++ patterns)"""
        # Patterns to explicitly exclude (Python, JS, etc.)
        exclude_patterns = [
            'python', 'py_', 'pip_', 'wheel', 'setuptools', 'pytest',
            'node', 'npm', 'yarn', 'webpack', 'babel', 'typescript',
            'java', 'maven', 'gradle', 'scala', 'kotlin',
            'go_', 'rust_', 'cargo', 'crate'
        ]
        
        # Add test-related patterns only if not including test dependencies
        if not self.include_test_deps:
            exclude_patterns.extend(['test_', '_test', 'testing', 'mock', 'fixture'])
        
        target_lower = target.lower()
        return any(pattern in target_lower for pattern in exclude_patterns)
    
    def classify_package_type(self, package_id: str, targets: List[str]) -> str:
        """Classify package as runtime, build, or test"""
        # Check if it's a build tool
        if any(tool in package_id.lower() for tool in self.BUILD_TOOLS):
            return 'build' if self.include_build_tools else 'excluded'
        
        # Check if it's a test dependency
        if any(pattern in package_id.lower() for pattern in ['test_', '_test', 'testing', 'mock', 'fixture', 'gtest', 'googletest']):
            return 'test' if self.include_test_deps else 'excluded'
        
        return 'runtime'
    
    def process_dependencies(self) -> Dict[str, Any]:
        """Process all dependencies and group by package"""
        all_dependencies = {}
        
        for target in self.DELIVERABLE_TARGETS:
            print(f"\nAnalyzing dependencies for {target}")
            
            # Get all dependencies
            deps = self.run_bazel_query(target, self.transitive_depth)
            
            # Filter for C/C++ dependencies
            c_cpp_deps = []
            excluded_deps = []
            
            print(f"  - Analyzing {len(deps)} dependencies for C/C++ content...")
            
            for i, dep in enumerate(deps, 1):
                if i % 100 == 0 or i == len(deps):
                    progress = (i / len(deps)) * 100
                    print(f"    Progress: {i}/{len(deps)} ({progress:.1f}%)", end='\r')
                
                # Skip Bazel tools and internal targets
                if dep.startswith('@bazel_tools//') or dep.startswith('@local_config_'):
                    excluded_deps.append(dep)
                    continue
                
                # Skip Ray's own internal code
                if dep.startswith('//src/ray/'):
                    excluded_deps.append(dep)
                    continue
                
                # Skip other internal Ray targets (except our deliverable targets)
                if dep.startswith('//:') and not dep.startswith('//:gen_ray_pkg'):
                    excluded_deps.append(dep)
                    continue
                
                # Check if explicitly excluded
                if self.is_excluded_target(dep):
                    excluded_deps.append(dep)
                    continue
                
                # Check if it's a C/C++ target
                if self.is_c_cpp_target(dep):
                    c_cpp_deps.append(dep)
                else:
                    excluded_deps.append(dep)
            
            print()  # New line after progress
            print(f"  - Found {len(c_cpp_deps)} C/C++ dependencies")
            print(f"  - Excluded {len(excluded_deps)} dependencies")
            
            all_dependencies[target] = c_cpp_deps
        
        return all_dependencies
    
    def group_by_package(self, all_dependencies: Dict[str, List[str]]) -> None:
        """Group dependencies by package and apply deduplication rules"""
        print("\nGrouping dependencies by package...")
        
        for target, deps in all_dependencies.items():
            print(f"  Processing {len(deps)} dependencies for {target}")
            
            for dep in deps:
                package_id = self.extract_package_id(dep)
                
                if package_id not in self.packages:
                    self.packages[package_id] = {
                        'targets': [],
                        'type': 'unknown',
                        'target_count': 0,
                        'compliance_required': True
                    }
                
                # Add target to package
                if dep not in self.packages[package_id]['targets']:
                    self.packages[package_id]['targets'].append(dep)
                    self.packages[package_id]['target_count'] += 1
        
        # Classify package types
        print("  Classifying package types...")
        for package_id, package_info in self.packages.items():
            package_type = self.classify_package_type(package_id, package_info['targets'])
            package_info['type'] = package_type
            package_info['compliance_required'] = (package_type == 'runtime')
        
        # Apply deduplication rules
        self.apply_deduplication_rules()
        
        print(f"  Found {len(self.packages)} unique packages")
    
    def apply_deduplication_rules(self) -> None:
        """Apply deduplication rules based on our strategy"""
        print("  Applying deduplication rules...")
        
        # Group external aliases that resolve to the same package
        external_groups = {}
        for package_id, package_info in self.packages.items():
            if any(target.startswith('//external:') for target in package_info['targets']):
                # This is an external package, group by resolved name
                resolved_name = None
                for target in package_info['targets']:
                    if target.startswith('//external:'):
                        resolved = self.resolve_external_alias(target)
                        if resolved:
                            resolved_name = resolved
                            break
                
                if resolved_name and resolved_name != package_id:
                    if resolved_name not in external_groups:
                        external_groups[resolved_name] = []
                    external_groups[resolved_name].append(package_id)
        
        # Merge external aliases pointing to same package
        for resolved_name, package_ids in external_groups.items():
            if len(package_ids) > 1:
                # Merge all packages into the resolved name
                merged_targets = []
                merged_type = 'runtime'
                
                for package_id in package_ids:
                    if package_id in self.packages:
                        merged_targets.extend(self.packages[package_id]['targets'])
                        if self.packages[package_id]['type'] != 'excluded':
                            merged_type = self.packages[package_id]['type']
                        del self.packages[package_id]
                
                # Create merged package
                self.packages[resolved_name] = {
                    'targets': list(set(merged_targets)),  # Remove duplicates
                    'type': merged_type,
                    'target_count': len(set(merged_targets)),
                    'compliance_required': (merged_type == 'runtime')
                }
    
    def create_fossa_folders(self) -> None:
        """Create Fossa-ready folder structure for each package"""
        print(f"\nCreating Fossa folder structure in {self.fossa_folder}")
        
        for package_id, package_info in self.packages.items():
            if package_info['type'] == 'excluded':
                continue
                
            # Sanitize package name for filesystem
            safe_package_id = re.sub(r'[^\w\-_.]', '_', package_id)
            package_folder = self.fossa_folder / safe_package_id
            package_folder.mkdir(exist_ok=True)
            
            print(f"  Creating folder for package: {package_id} -> {safe_package_id}")
            
            # Copy entire package directory using the first target
            self.copy_package_directory(package_id, package_info['targets'], package_folder)
    
    def copy_package_directory(self, package_id: str, targets: List[str], package_folder: Path) -> None:
        """Copy entire package directory to Fossa folder using the first target"""
        print(f"    Copying package directory for {package_id}...")
        
        if not targets:
            print(f"      No targets found for {package_id}")
            return
        
        # Use the first target to find the package root directory
        first_target = targets[0]
        package_root = self.find_package_root(first_target)
        
        if not package_root or not package_root.exists():
            print(f"      Could not find package root for {package_id}")
            return
        
        try:
            # Copy the entire package directory
            if package_root.is_dir():
                # Remove existing folder if it exists
                if package_folder.exists():
                    shutil.rmtree(package_folder)
                
                # Copy entire directory
                shutil.copytree(package_root, package_folder)
                print(f"    Copied entire package directory: {package_root} -> {package_folder}")
            else:
                print(f"      Package root is not a directory: {package_root}")
                
        except (PermissionError, OSError) as e:
            print(f"      Warning: Could not copy package directory {package_root}: {e}")
            # Try to create a symlink instead
            try:
                if package_folder.exists():
                    shutil.rmtree(package_folder)
                package_folder.symlink_to(package_root.absolute())
                print(f"    Created symlink: {package_folder} -> {package_root}")
            except OSError as e2:
                print(f"      Warning: Could not create symlink for {package_root}: {e2}")
    
    def find_package_root(self, target: str) -> Optional[Path]:
        """Find the package root directory from a Bazel target"""
        try:
            # Get file location for this target
            cmd = ["bazel", "query", "--output=location", target]
            result = subprocess.run(cmd, cwd=self.ray_root, 
                                  capture_output=True, text=True, check=True)
            print(result)
            exit()
            for line in result.stdout.strip().split('\n'):
                if ':' in line:
                    file_path = line.split(':')[0]
                    if file_path and Path(file_path).exists():
                        file_path = Path(file_path)
                        
                        # For external packages, find the package root
                        if target.startswith('@') or target.startswith('//external:'):
                            # Look for the package root by going up the directory tree
                            current = file_path.parent
                            while current and current != Path('/'):
                                # Check if this looks like a package root
                                if (current.name in ['src', 'include', 'lib', 'include'] or 
                                    any((current / f).exists() for f in ['BUILD', 'BUILD.bazel', 'CMakeLists.txt', 'Makefile', 'LICENSE', 'README.md'])):
                                    return current
                                current = current.parent
                            
                            # If we can't find a clear package root, use the file's directory
                            return file_path.parent
                        else:
                            # For internal targets, use the file's directory
                            return file_path.parent
                            
        except subprocess.CalledProcessError as e:
            print(f"      Failed to get location for {target}: {e}")
            
        return None
    
    def generate_outputs(self) -> None:
        """Generate consolidated output files"""
        print("\nGenerating output files...")
        
        # Create packages summary
        packages_summary = {
            'packages': self.packages,
            'summary': {
                'total_packages': len(self.packages),
                'runtime_packages': len([p for p in self.packages.values() if p['type'] == 'runtime']),
                'build_packages': len([p for p in self.packages.values() if p['type'] == 'build']),
                'test_packages': len([p for p in self.packages.values() if p['type'] == 'test']),
                'excluded_packages': len([p for p in self.packages.values() if p['type'] == 'excluded']),
                'compliance_required': len([p for p in self.packages.values() if p['compliance_required']])
            }
        }
        
        # Write packages.json
        with open(self.ray_root / 'packages.json', 'w') as f:
            json.dump(packages_summary, f, indent=2)
        
        # Write failed resolutions if any
        if self.failed_resolutions:
            with open(self.ray_root / 'failed_resolutions.json', 'w') as f:
                json.dump(self.failed_resolutions, f, indent=2)
        
        print("Generated files:")
        print("  - packages.json")
        if self.failed_resolutions:
            print("  - failed_resolutions.json")
        print(f"  - Fossa folder: {self.fossa_folder}")
    
    def run_analysis(self) -> None:
        """Run the complete optimized analysis"""
        print("Starting Optimized Ray C/C++ Dependency Analysis")
        print("=" * 60)
        
        # Step 1: Process dependencies
        print("\n1. Processing dependencies...")
        all_dependencies = self.process_dependencies()
        
        # Step 2: Group by package
        print("\n2. Grouping by package...")
        self.group_by_package(all_dependencies)
        
        # Step 3: Create Fossa folders
        print("\n3. Creating Fossa folder structure...")
        self.create_fossa_folders()
        
        # Step 4: Generate outputs
        print("\n4. Generating output files...")
        self.generate_outputs()
        
        print("\nAnalysis complete!")
        print(f"Found {len(self.packages)} unique packages")
        print(f"Fossa analysis folder: {self.fossa_folder}")

def main():
    parser = argparse.ArgumentParser(description='Optimized Ray C/C++ Dependency Analyzer')
    parser.add_argument('--ray-root', required=True, help='Path to Ray repository')
    parser.add_argument('--fossa-folder', required=True, help='Path to Fossa analysis folder')
    parser.add_argument('--transitive-depth', type=int, default=-1, 
                       help='Transitive dependency depth (-1=all, 0=direct only)')
    parser.add_argument('--include-build-tools', action='store_true', default=False,
                       help='Include build tools in compliance analysis')
    parser.add_argument('--include-test-deps', action='store_true', default=False,
                       help='Include test dependencies in compliance analysis')
    
    args = parser.parse_args()
    
    # Validate ray root
    ray_root = Path(args.ray_root)
    if not ray_root.exists():
        print(f"Error: Ray root directory does not exist: {ray_root}")
        sys.exit(1)
    
    if not (ray_root / "WORKSPACE").exists():
        print(f"Error: Not a valid Ray repository (no WORKSPACE file): {ray_root}")
        sys.exit(1)
    
    # Create preprocessor and run analysis
    preprocessor = OptimizedRayFossaPreprocessor(
        ray_root=str(ray_root),
        fossa_folder=args.fossa_folder,
        transitive_depth=args.transitive_depth,
        include_build_tools=args.include_build_tools,
        include_test_deps=args.include_test_deps
    )
    
    try:
        preprocessor.run_analysis()
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error during analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()