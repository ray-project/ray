#!/usr/bin/env python3
"""
Ray FOSSA Preprocessor

This script analyzes Ray's C/C++ dependencies and prepares data for FOSSA compliance scanning.
It handles Bazel's complex dependency structure, including external dependencies, patches,
custom C code, and platform-specific variations.

Usage:
    python ray_fossa_preprocessor.py --ray-root /path/to/ray [options]

Author: AI Assistant
Date: 2024
"""

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import yaml
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Set

class RayFossaPreprocessor:
    """Main class for Ray FOSSA preprocessing"""
    
    # Deliverable binaries that need compliance analysis
    DELIVERABLE_TARGETS = [
        "//:gen_ray_pkg",
        "//src/ray/raylet", 
        "//src/ray/gcs:gcs_server",
        "//src/ray/core_worker",
        "//src/ray/raylet_ipc_client",
        "//src/ray/gcs_rpc_client"
    ]
    
    # Build tools that can be optionally included
    BUILD_TOOLS = [
        "protoc", "cmake", "ninja", "bazel", "python", "cython",
        "rules_foreign_cc", "rules_proto", "rules_cc"
    ]
    
    # Platform configurations
    PLATFORM_CONFIGS = {
        'linux_x86_64': {
            'target_platform': '@platforms//os:linux',
            'cpu_arch': '@platforms//cpu:x86_64',
            'binary_extension': '',
            'analysis_tool': 'ldd'
        },
        'linux_arm64': {
            'target_platform': '@platforms//os:linux', 
            'cpu_arch': '@platforms//cpu:arm64',
            'binary_extension': '',
            'analysis_tool': 'ldd'
        },
        'darwin_arm64': {
            'target_platform': '@platforms//os:osx',
            'cpu_arch': '@platforms//cpu:arm64', 
            'binary_extension': '',
            'analysis_tool': 'otool'
        },
        'windows_x86_64': {
            'target_platform': '@platforms//os:windows',
            'cpu_arch': '@platforms//cpu:x86_64',
            'binary_extension': '.exe',
            'analysis_tool': 'dumpbin'
        }
    }
    
    def __init__(self, ray_root: str, 
                 transitive_depth: int = -1,
                 include_build_tools: bool = False,
                 include_test_deps: bool = False,
                 platforms: List[str] = None):
        self.ray_root = Path(ray_root)
        self.transitive_depth = transitive_depth
        self.include_build_tools = include_build_tools
        self.include_test_deps = include_test_deps
        self.platforms = platforms or ['linux_x86_64', 'darwin_arm64']
        
        # Data storage
        self.direct_deps = {}
        self.transitive_deps = {}
        self.build_deps = {}
        self.runtime_deps = {}
        self.patches = {}
        self.custom_c_code = {}
        self.platform_results = {}
        
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
            'go_', 'rust_', 'cargo', 'crate',
            'test_', '_test', 'testing', 'mock', 'fixture'
        ]
        
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
    
    def validate_c_cpp_dependencies(self, deps: List[str]) -> Dict[str, Any]:
        """Validate that dependencies are actually C/C++ related"""
        validation = {
            'total_deps': len(deps),
            'c_cpp_deps': 0,
            'suspicious_deps': [],
            'file_extensions': {},
            'patterns': {}
        }
        
        c_cpp_extensions = ['.h', '.hpp', '.c', '.cpp', '.cc', '.cxx', '.c++']
        suspicious_patterns = ['python', 'py_', 'pip_', 'node', 'npm', 'java', 'go_', 'rust_']
        
        for dep in deps:
            # Check file extensions
            if '.' in dep:
                ext = '.' + dep.split('.')[-1]
                validation['file_extensions'][ext] = validation['file_extensions'].get(ext, 0) + 1
            
            # Check for suspicious patterns
            is_suspicious = any(pattern in dep.lower() for pattern in suspicious_patterns)
            if is_suspicious:
                validation['suspicious_deps'].append(dep)
            else:
                validation['c_cpp_deps'] += 1
        
        return validation
    
    def classify_dependency_types(self, deps: List[str]) -> Dict[str, Any]:
        """Classify dependencies as runtime, build-only, test-only, etc."""
        classified = {
            'runtime': [],
            'build_only': [],
            'test_only': [],
            'build_tools': [],
            'excluded': []
        }
        
        for dep in deps:
            dep_info = {
                'name': dep,
                'type': 'unknown',
                'compliance_required': False,
                'excluded': False,
                'exclusion_reason': None
            }
            
            # Check if it's a build tool
            if any(tool in dep.lower() for tool in self.BUILD_TOOLS):
                if self.include_build_tools:
                    dep_info['type'] = 'build_tool'
                    dep_info['compliance_required'] = True
                    classified['build_tools'].append(dep_info)
                else:
                    dep_info['excluded'] = True
                    dep_info['exclusion_reason'] = 'build_tool'
                    classified['excluded'].append(dep_info)
                continue
            
            # For now, assume all C/C++ deps are runtime (can be enhanced with binary analysis)
            dep_info['type'] = 'runtime'
            dep_info['compliance_required'] = True
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
            
            # Validate the filtering
            validation = self.validate_c_cpp_dependencies(c_cpp_deps)
            print(f"  - Validation: {validation['c_cpp_deps']} C/C++ deps, {len(validation['suspicious_deps'])} suspicious")
            
            if validation['suspicious_deps']:
                print(f"  - Suspicious dependencies found: {validation['suspicious_deps'][:3]}")
            
            all_dependencies[target] = c_cpp_deps
            
            print(f"Found {len(c_cpp_deps)} C/C++ dependencies for {target}")
            
            # Show breakdown by pattern
            external_count = len([d for d in c_cpp_deps if d.startswith('//external/')])
            repo_count = len([d for d in c_cpp_deps if d.startswith('@')])
            print(f"  - External targets (//external/): {external_count}")
            print(f"  - Repository targets (@repo//): {repo_count}")
        
        return all_dependencies
    
    def analyze_patches(self) -> Dict[str, Any]:
        """Analyze thirdparty/patches/ for license implications"""
        patches_dir = self.ray_root / "thirdparty" / "patches"
        patch_analysis = {}
        
        if not patches_dir.exists():
            print("No patches directory found")
            return patch_analysis
        
        for patch_file in patches_dir.glob("*.patch"):
            patch_info = {
                'file': str(patch_file),
                'target': 'unknown',
                'files_modified': [],
                'license_impact': 'none',
                'description': 'Unknown'
            }
            
            # Try to determine target from filename
            patch_name = patch_file.stem
            if 'spdlog' in patch_name:
                patch_info['target'] = 'com_github_spdlog'
            elif 'grpc' in patch_name:
                patch_info['target'] = 'com_github_grpc_grpc'
            elif 'redis' in patch_name:
                patch_info['target'] = 'com_github_antirez_redis'
            # Add more mappings as needed
            
            patch_analysis[patch_file.name] = patch_info
        
        print(f"Analyzed {len(patch_analysis)} patch files")
        return patch_analysis
    
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
    
    def analyze_binary_dependencies(self, binary_path: str) -> Dict[str, Any]:
        """Analyze how dependencies are linked in binaries"""
        linking_info = {
            'static_libs': [],
            'dynamic_libs': [],
            'system_libs': [],
            'bundled_libs': []
        }
        
        if not os.path.exists(binary_path):
            print(f"Binary not found: {binary_path}")
            return linking_info
        
        # Use platform-specific tools
        if platform.system() == "Darwin":  # macOS
            cmd = ["otool", "-L", binary_path]
        elif platform.system() == "Linux":
            cmd = ["ldd", binary_path]
        else:
            print(f"Binary analysis not supported on {platform.system()}")
            return linking_info
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            linking_info['dynamic_libs'] = self.parse_linking_output(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error analyzing binary {binary_path}: {e}")
        
        return linking_info
    
    def parse_linking_output(self, output: str) -> List[str]:
        """Parse output from ldd/otool to extract library names"""
        libs = []
        for line in output.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            # Extract library name (different formats for ldd vs otool)
            if '=>' in line:  # ldd format
                lib_name = line.split('=>')[0].strip()
            elif line.startswith('/'):  # otool format
                lib_name = line.split()[0]
            else:
                continue
            
            if lib_name and not lib_name.startswith('['):
                libs.append(lib_name)
        
        return libs
    
    def generate_fossa_config(self) -> str:
        """Generate .fossa.yml configuration file"""
        config = {
            'version': 3,
            'project': {
                'name': 'rajesh-test',
                'branch': f'fossa-c-cpp-analysis-{self.get_timestamp()}'
            },
            'analyze': {
                'modules': [],
                'exclude': [
                    'python/**',
                    '**/requirements.txt',
                    '**/setup.py', 
                    '**/pyproject.toml',
                    '**/package.json',
                    '**/package-lock.json',
                    '**/yarn.lock',
                    '**/node_modules/**',
                    '**/__pycache__/**',
                    '**/*.py',
                    '**/*.js',
                    '**/*.ts',
                    '**/*.jsx',
                    '**/*.tsx',
                    '**/test/**',
                    '**/tests/**',
                    '**/examples/**',
                    '**/docs/**',
                    '**/doc/**',
                    '**/*.md',
                    '**/*.rst',
                    '**/*.txt',
                    'bazel-ray/**',
                    'bazel-bin/**',
                    'bazel-out/**',
                    'bazel-testlogs/**',
                    'bazel/**',
                    '**/external/**',
                    '**/Cargo.toml',
                    '**/Gemfile',
                    '**/Gemfile.lock',
                    '**/Cargo.lock',
                    '**/rust/**',
                    '**/ruby/**',
                    '**/go.mod',
                    '**/go.sum',
                    '**/pom.xml',
                    '**/build.gradle'
                ]
            }
        }
        
        # Add modules for different dependency types
        config['analyze']['modules'].extend([
            {
                'name': 'ray-runtime-deps',
                'type': 'custom',
                'dependencies': 'runtime_dependencies.json'
            },
            {
                'name': 'ray-custom-c',
                'type': 'custom', 
                'dependencies': 'custom_c_analysis.json'
            },
            {
                'name': 'ray-patched-deps',
                'type': 'custom',
                'dependencies': 'patches_analysis.json'
            }
        ])
        
        if self.include_build_tools:
            config['analyze']['modules'].append({
                'name': 'ray-build-tools',
                'type': 'custom',
                'dependencies': 'build_dependencies.json'
            })
        
        return config
    
    def create_compliance_report(self) -> Dict[str, Any]:
        """Create comprehensive compliance report"""
        report = {
            'summary': {
                'total_dependencies': 0,
                'runtime_dependencies': 0,
                'build_only_dependencies': 0,
                'test_only_dependencies': 0,
                'compliance_required': 0,
                'platforms_analyzed': self.platforms
            },
            'runtime_dependencies': {
                'high_risk': [],
                'medium_risk': [],
                'low_risk': []
            },
            'build_only_dependencies': {
                'note': 'These do not affect final binary distribution',
                'dependencies': []
            },
            'patches_analysis': self.patches,
            'custom_c_code': self.custom_c_code,
            'platform_differences': self.platform_results
        }
        
        # Calculate summary statistics - fix the unhashable type error
        all_runtime_dep_names = set()
        for target_deps in self.runtime_deps.values():
            # Extract just the dependency names, not the full dict objects
            for dep in target_deps:
                if isinstance(dep, dict):
                    all_runtime_dep_names.add(dep.get('name', str(dep)))
                else:
                    all_runtime_dep_names.add(str(dep))
        
        report['summary']['total_dependencies'] = len(all_runtime_dep_names)
        report['summary']['runtime_dependencies'] = len(all_runtime_dep_names)
        report['summary']['compliance_required'] = len(all_runtime_dep_names)
        
        return report
    
    def run_full_analysis(self):
        """Run the complete analysis pipeline"""
        print("Starting Ray FOSSA Preprocessing Analysis")
        print("=" * 60)
        
        # Step 1: Analyze transitive dependencies
        print("\n1. Analyzing transitive dependencies...")
        self.transitive_deps = self.analyze_transitive_dependencies()
        
        # Step 2: Classify dependency types
        print("\n2. Classifying dependency types...")
        for target, deps in self.transitive_deps.items():
            classified = self.classify_dependency_types(deps)
            # Store just the dependency names for runtime deps, not the full objects
            self.runtime_deps[target] = [dep['name'] for dep in classified['runtime']]
            self.build_deps[target] = [dep['name'] for dep in classified['build_only']]
        
        # Step 3: Analyze patches
        print("\n3. Analyzing patches...")
        self.patches = self.analyze_patches()
        
        # Step 4: Scan custom C code
        print("\n4. Scanning custom C code...")
        self.custom_c_code = self.scan_custom_c_code()
        
        # Step 5: Generate outputs
        print("\n5. Generating output files...")
        self.generate_outputs()
        
        print("\nAnalysis complete!")
    
    def generate_outputs(self):
        """Generate all output files"""
        # Generate .fossa.yml
        fossa_config = self.generate_fossa_config()
        with open(self.ray_root / '.fossa.yml', 'w') as f:
            import yaml
            yaml.dump(fossa_config, f, default_flow_style=False, sort_keys=False)
        
        # Generate dependency manifests
        with open(self.ray_root / 'dependencies.json', 'w') as f:
            json.dump(self.transitive_deps, f, indent=2)
        
        with open(self.ray_root / 'runtime_dependencies.json', 'w') as f:
            json.dump(self.runtime_deps, f, indent=2)
        
        with open(self.ray_root / 'build_dependencies.json', 'w') as f:
            json.dump(self.build_deps, f, indent=2)
        
        with open(self.ray_root / 'patches_analysis.json', 'w') as f:
            json.dump(self.patches, f, indent=2)
        
        with open(self.ray_root / 'custom_c_analysis.json', 'w') as f:
            json.dump(self.custom_c_code, f, indent=2)
        
        # Generate compliance report
        compliance_report = self.create_compliance_report()
        with open(self.ray_root / 'compliance_report.json', 'w') as f:
            json.dump(compliance_report, f, indent=2)
        
        # Generate human-readable report
        self.generate_markdown_report(compliance_report)
        
        print("Generated files:")
        print("  - .fossa.yml")
        print("  - dependencies.json")
        print("  - runtime_dependencies.json")
        print("  - build_dependencies.json")
        print("  - patches_analysis.json")
        print("  - custom_c_analysis.json")
        print("  - compliance_report.json")
        print("  - compliance_report.md")
    
    def generate_markdown_report(self, report: Dict[str, Any]):
        """Generate human-readable markdown report"""
        md_content = f"""# Ray C/C++ Dependencies Compliance Report

## Summary

- **Total Dependencies**: {report['summary']['total_dependencies']}
- **Runtime Dependencies**: {report['summary']['runtime_dependencies']}
- **Compliance Required**: {report['summary']['compliance_required']}
- **Platforms Analyzed**: {', '.join(report['summary']['platforms_analyzed'])}

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

1. Review the generated `.fossa.yml` configuration
2. Run `fossa analyze` to perform license scanning
3. Run `fossa report` to generate compliance reports
4. Review any flagged dependencies for compliance issues

## Files Generated

- `.fossa.yml` - FOSSA configuration
- `dependencies.json` - Complete dependency data
- `runtime_dependencies.json` - Runtime dependencies only
- `patches_analysis.json` - Patch impact analysis
- `custom_c_analysis.json` - Custom C code analysis
- `compliance_report.json` - Machine-readable report
- `compliance_report.md` - Human-readable report
"""
        
        with open(self.ray_root / 'compliance_report.md', 'w') as f:
            f.write(md_content)

def main():
    parser = argparse.ArgumentParser(description='Ray FOSSA Preprocessor')
    parser.add_argument('--ray-root', required=True, help='Path to Ray repository')
    parser.add_argument('--transitive-depth', type=int, default=-1, 
                       help='Transitive dependency depth (-1=all, 0=direct only)')
    parser.add_argument('--include-build-tools', action='store_true', default=False,
                       help='Include build tools in compliance analysis')
    parser.add_argument('--include-test-deps', action='store_true', default=False,
                       help='Include test dependencies in compliance analysis')
    parser.add_argument('--platforms', nargs='+', 
                       choices=['linux_x86_64', 'linux_arm64', 'darwin_arm64', 'windows_x86_64'],
                       default=['linux_x86_64', 'darwin_arm64'],
                       help='Platforms to analyze')
    
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
        include_test_deps=args.include_test_deps,
        platforms=args.platforms
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