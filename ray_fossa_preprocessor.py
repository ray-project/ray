#!/usr/bin/env python3
"""
Ray C/C++ Dependency Analyzer

This script analyzes Ray's C/C++ dependencies and generates dependency reports.
It handles Bazel's complex dependency structure, including external dependencies
and custom C code.

Usage:
    python ray_fossa_preprocessor.py --ray-root /path/to/ray [options]

Author: AI Assistant
Date: 2024
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

class RayFossaPreprocessor:
    """Main class for Ray C/C++ dependency analysis"""
    
    # Deliverable binaries that need compliance analysis
    DELIVERABLE_TARGETS = [
        "//:gen_ray_pkg",
        # "//src/ray/raylet", 
        # "//src/ray/gcs:gcs_server",
        # "//src/ray/raylet_ipc_client"
    ]
    
    # Build tools that can be optionally included
    BUILD_TOOLS = [
        "protoc", "cmake", "ninja", "bazel", "python", "cython",
        "rules_foreign_cc", "rules_proto", "rules_cc"
    ]
    
    
    def __init__(self, ray_root: str, 
                 transitive_depth: int = -1,
                 include_build_tools: bool = False,
                 include_test_deps: bool = False):
        self.ray_root = Path(ray_root)
        self.transitive_depth = transitive_depth
        self.include_build_tools = include_build_tools
        self.include_test_deps = include_test_deps
        
        # Data storage
        self.transitive_deps = {}
        self.build_deps = {}
        self.runtime_deps = {}
        self.test_deps = {}
        self.custom_c_code = {}
        
    def run_bazel_query(self, target: str, depth: Optional[int] = None) -> List[str]:
        """Run bazel query with correct syntax"""
        if depth is None:
            # All transitive dependencies
            cmd = ["bazel", "query", f"deps({target})", "--output=label"]
        elif depth == 0:
            # Direct dependencies only
            cmd = ["bazel", "query", f"deps({target}, 1)", "--output=label"]
        else:
            # Specific depth
            cmd = ["bazel", "query", f"deps({target}, {depth})", "--output=label"]
        
        try:
            result = subprocess.run(cmd, cwd=self.ray_root, 
                                  capture_output=True, text=True, check=True)
            return result.stdout.strip().split('\n')
        except subprocess.CalledProcessError as e:
            print(f"Error running bazel query for {target}: {e}")
            return []
    
    def filter_c_cpp_dependencies(self, all_deps: List[str]) -> List[str]:
        """Filter for C/C++ related dependencies - handles both //external/ and @repo// patterns"""
        c_cpp_deps = []
        excluded_deps = []
        
        # External targets (//external/...) - though none found in Ray
        external_deps = [dep for dep in all_deps if dep.startswith('//external/')]
        
        # Repository targets (@repo//...) - this is where most external deps are
        repo_deps = [dep for dep in all_deps if dep.startswith('@') and not dep.startswith('@bazel_tools//')]
        
        # Known C/C++ library patterns for external targets
        external_c_cpp_patterns = [
            'protobuf', 'grpc', 'zlib', 'openssl', 'boringssl', 'cares', 're2', 
            'absl', 'boost', 'gtest', 'gflags', 'spdlog', 'hiredis', 'redis',
            'msgpack', 'flatbuffers', 'rapidjson', 'nlohmann', 'jemalloc',
            'opencensus', 'opentelemetry', 'prometheus', 'arrow', 'thrift',
            'snappy', 'lz4', 'zstd', 'brotli', 'curl', 'libevent', 'libuv'
        ]
        
        # Known C/C++ repository patterns
        repo_c_cpp_patterns = [
            'com_github_', 'com_google_', 'io_opencensus', 'io_opentelemetry',
            'boringssl', 'openssl', 'zlib', 'protobuf', 'grpc', 'absl',
            'boost', 'gtest', 'gflags', 'spdlog', 'hiredis', 'redis',
            'msgpack', 'flatbuffers', 'rapidjson', 'nlohmann', 'jemalloc',
            'prometheus', 'opencensus', 'opentelemetry', 'arrow', 'thrift',
            'snappy', 'lz4', 'zstd', 'brotli', 'curl', 'libevent', 'libuv'
        ]
        
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
        
        # Check external targets
        for dep in external_deps:
            if any(pattern in dep for pattern in external_c_cpp_patterns):
                c_cpp_deps.append(dep)
            elif any(pattern in dep for pattern in exclude_patterns):
                excluded_deps.append(f"{dep} (excluded: non-C/C++)")
            else:
                excluded_deps.append(f"{dep} (excluded: unknown)")
        
        # Check repository targets
        for dep in repo_deps:
            if any(pattern in dep for pattern in repo_c_cpp_patterns):
                c_cpp_deps.append(dep)
            elif any(pattern in dep for pattern in exclude_patterns):
                excluded_deps.append(f"{dep} (excluded: non-C/C++)")
            else:
                excluded_deps.append(f"{dep} (excluded: unknown)")
        
        # Print debugging information
        print(f"  - Total dependencies analyzed: {len(all_deps)}")
        print(f"  - C/C++ dependencies found: {len(c_cpp_deps)}")
        print(f"  - Excluded dependencies: {len(excluded_deps)}")
        
        if excluded_deps and len(excluded_deps) <= 10:  # Show first 10 excluded deps
            print(f"  - Sample excluded deps: {excluded_deps[:5]}")
        
        return c_cpp_deps
    
    def get_timestamp(self) -> str:
        """Get current timestamp in YYYYMMDD_HHMMSS format"""
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    
    def classify_dependency_types(self, deps: List[str]) -> Dict[str, Any]:
        """Classify dependencies as runtime, build-only, or test-only"""
        classified = {
            'runtime': [],
            'build_only': [],
            'test_only': []
        }
        
        for dep in deps:
            dep_info = {
                'name': dep,
                'type': 'runtime',
                'compliance_required': True
            }
            
            # Check if it's a build tool
            if any(tool in dep.lower() for tool in self.BUILD_TOOLS):
                if self.include_build_tools:
                    dep_info['type'] = 'build_tool'
                    classified['build_only'].append(dep_info)
                else:
                    # Skip build tools if not including them
                    continue
            # Check if it's a test dependency
            elif any(pattern in dep.lower() for pattern in ['test_', '_test', 'testing', 'mock', 'fixture', 'gtest', 'googletest']):
                if self.include_test_deps:
                    dep_info['type'] = 'test'
                    dep_info['compliance_required'] = False  # Test deps typically don't need compliance
                    classified['test_only'].append(dep_info)
                else:
                    # Skip test dependencies if not including them
                    continue
            else:
                # All other C/C++ deps are runtime
                classified['runtime'].append(dep_info)
        
        return classified
    
    def analyze_transitive_dependencies(self) -> Dict[str, List[str]]:
        """Analyze transitive deps with configurable depth"""
        all_dependencies = {}
        
        for target in self.DELIVERABLE_TARGETS:
            print(f"\nAnalyzing dependencies for {target}")
            
            if self.transitive_depth == -1:
                # Get ALL transitive dependencies
                deps = self.run_bazel_query(target)
            elif self.transitive_depth == 0:
                # Direct dependencies only
                deps = self.run_bazel_query(target, 1)
            else:
                # Specific depth
                deps = self.run_bazel_query(target, self.transitive_depth)
            
            # Filter for C/C++ dependencies
            c_cpp_deps = self.filter_c_cpp_dependencies(deps)
            all_dependencies[target] = c_cpp_deps
            
            print(f"Found {len(c_cpp_deps)} C/C++ dependencies for {target}")
            
            # Show breakdown by pattern
            external_count = len([d for d in c_cpp_deps if d.startswith('//external/')])
            repo_count = len([d for d in c_cpp_deps if d.startswith('@')])
            print(f"  - External targets (//external/): {external_count}")
            print(f"  - Repository targets (@repo//): {repo_count}")
        
        return all_dependencies
    
    
    def scan_custom_c_code(self) -> Dict[str, Any]:
        """Scan src/ray/thirdparty/ for embedded C libraries"""
        thirdparty_dir = self.ray_root / "src" / "ray" / "thirdparty"
        custom_code = {}
        
        if not thirdparty_dir.exists():
            print("No thirdparty directory found")
            return custom_code
        
        for c_file in thirdparty_dir.rglob("*.c"):
            file_info = {
                'file': str(c_file),
                'type': 'embedded_library',
                'original_source': 'unknown',
                'license': 'unknown',
                'description': 'Custom C implementation'
            }
            
            # Try to determine license from file content
            try:
                with open(c_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(1000)  # Read first 1000 chars
                    
                if 'MIT' in content or 'MIT License' in content:
                    file_info['license'] = 'MIT'
                elif 'BSD' in content:
                    file_info['license'] = 'BSD'
                elif 'GPL' in content:
                    file_info['license'] = 'GPL'
                elif 'public domain' in content.lower():
                    file_info['license'] = 'public_domain'
                else:
                    file_info['license'] = 'unknown'
                    
            except Exception as e:
                print(f"Error reading {c_file}: {e}")
            
            custom_code[c_file.name] = file_info
        
        print(f"Found {len(custom_code)} custom C files")
        return custom_code
    
    
    
    def create_compliance_report(self) -> Dict[str, Any]:
        """Create comprehensive compliance report"""
        report = {
            'summary': {
                'total_dependencies': 0,
                'runtime_dependencies': 0,
                'build_only_dependencies': 0,
                'test_dependencies': 0,
                'compliance_required': 0
            },
            'custom_c_code': self.custom_c_code
        }
        
        # Calculate summary statistics
        all_runtime_dep_names = set()
        for target_deps in self.runtime_deps.values():
            for dep in target_deps:
                if isinstance(dep, dict):
                    all_runtime_dep_names.add(dep.get('name', str(dep)))
                else:
                    all_runtime_dep_names.add(str(dep))
        
        all_test_dep_names = set()
        for target_deps in self.test_deps.values():
            for dep in target_deps:
                if isinstance(dep, dict):
                    all_test_dep_names.add(dep.get('name', str(dep)))
                else:
                    all_test_dep_names.add(str(dep))
        
        report['summary']['runtime_dependencies'] = len(all_runtime_dep_names)
        report['summary']['test_dependencies'] = len(all_test_dep_names)
        report['summary']['total_dependencies'] = len(all_runtime_dep_names) + len(all_test_dep_names)
        report['summary']['compliance_required'] = len(all_runtime_dep_names)  # Only runtime deps need compliance
        
        return report
    
    def run_full_analysis(self):
        """Run the complete analysis pipeline"""
        print("Starting Ray C/C++ Dependency Analysis")
        print("=" * 60)
        
        # Step 1: Analyze transitive dependencies
        print("\n1. Analyzing transitive dependencies...")
        self.transitive_deps = self.analyze_transitive_dependencies()
        
        # Step 2: Classify dependency types
        print("\n2. Classifying dependency types...")
        for target, deps in self.transitive_deps.items():
            classified = self.classify_dependency_types(deps)
            # Store just the dependency names for different dep types, not the full objects
            self.runtime_deps[target] = [dep['name'] for dep in classified['runtime']]
            self.build_deps[target] = [dep['name'] for dep in classified['build_only']]
            self.test_deps[target] = [dep['name'] for dep in classified['test_only']]
        
        # Step 3: Scan custom C code
        print("\n3. Scanning custom C code...")
        self.custom_c_code = self.scan_custom_c_code()
        
        # Step 4: Generate outputs
        print("\n4. Generating output files...")
        self.generate_outputs()
        
        print("\nAnalysis complete!")
    
    def generate_outputs(self):
        """Generate all output files"""
        # Generate dependency manifests
        with open(self.ray_root / 'dependencies.json', 'w') as f:
            json.dump(self.transitive_deps, f, indent=2)
        
        with open(self.ray_root / 'runtime_dependencies.json', 'w') as f:
            json.dump(self.runtime_deps, f, indent=2)
        
        with open(self.ray_root / 'build_dependencies.json', 'w') as f:
            json.dump(self.build_deps, f, indent=2)
        
        with open(self.ray_root / 'test_dependencies.json', 'w') as f:
            json.dump(self.test_deps, f, indent=2)
        
        with open(self.ray_root / 'custom_c_analysis.json', 'w') as f:
            json.dump(self.custom_c_code, f, indent=2)
        
        # Generate compliance report
        compliance_report = self.create_compliance_report()
        with open(self.ray_root / 'compliance_report.json', 'w') as f:
            json.dump(compliance_report, f, indent=2)
        
        # Generate human-readable report
        self.generate_markdown_report(compliance_report)
        
        print("Generated files:")
        print("  - dependencies.json")
        print("  - runtime_dependencies.json")
        print("  - build_dependencies.json")
        if self.include_test_deps:
            print("  - test_dependencies.json")
        print("  - custom_c_analysis.json")
        print("  - compliance_report.json")
        print("  - compliance_report.md")
    
    def generate_markdown_report(self, report: Dict[str, Any]):
        """Generate human-readable markdown report"""
        md_content = f"""# Ray C/C++ Dependencies Compliance Report

## Summary

- **Total Dependencies**: {report['summary']['total_dependencies']}
- **Runtime Dependencies**: {report['summary']['runtime_dependencies']}
- **Test Dependencies**: {report['summary']['test_dependencies']}
- **Compliance Required**: {report['summary']['compliance_required']}

## Analysis Configuration

- **Transitive Depth**: {'All' if self.transitive_depth == -1 else self.transitive_depth}
- **Include Build Tools**: {self.include_build_tools}
- **Include Test Dependencies**: {self.include_test_deps}

## Dependencies by Target

"""
        
        for target, deps in self.transitive_deps.items():
            md_content += f"### {target}\n"
            md_content += f"- **Total C/C++ Dependencies**: {len(deps)}\n"
            md_content += f"- **External Targets**: {len([d for d in deps if d.startswith('//external/')])}\n"
            md_content += f"- **Repository Targets**: {len([d for d in deps if d.startswith('@')])}\n\n"
        
        md_content += """## Next Steps

1. Review the generated dependency analysis files
2. Use the dependency data for compliance analysis with your preferred tools
3. Review any flagged dependencies for compliance issues

## Files Generated

- `dependencies.json` - Complete dependency data
- `runtime_dependencies.json` - Runtime dependencies only
- `custom_c_analysis.json` - Custom C code analysis
- `compliance_report.json` - Machine-readable report
- `compliance_report.md` - Human-readable report
"""
        
        with open(self.ray_root / 'compliance_report.md', 'w') as f:
            f.write(md_content)

def main():
    parser = argparse.ArgumentParser(description='Ray C/C++ Dependency Analyzer')
    parser.add_argument('--ray-root', required=True, help='Path to Ray repository')
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
    preprocessor = RayFossaPreprocessor(
        ray_root=str(ray_root),
        transitive_depth=args.transitive_depth,
        include_build_tools=args.include_build_tools,
        include_test_deps=args.include_test_deps
    )
    
    try:
        preprocessor.run_full_analysis()
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error during analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()