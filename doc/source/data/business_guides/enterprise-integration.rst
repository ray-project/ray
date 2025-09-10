.. _enterprise-integration:

Enterprise Integration Guide
============================

Ray Data provides enterprise-grade capabilities for integrating with existing enterprise systems, legacy infrastructure, and modern cloud platforms. This guide covers integration patterns and deployment strategies for large-scale enterprise environments.

**What you'll learn:**

* Integration patterns with enterprise systems and legacy infrastructure
* Data connectivity and format transformation strategies
* Multi-system integration and resource optimization
* Enterprise deployment and operational patterns

Enterprise Architecture Patterns
---------------------------------

**Hub and Spoke Architecture**

Implement centralized data processing with distributed source systems:

.. code-block:: python

    import ray
    from typing import Dict, List
    import logging
    from datetime import datetime

    class EnterpriseDataHub:
        """Centralized data processing hub for enterprise systems."""
        
        def __init__(self, hub_config: Dict):
            self.hub_config = hub_config
            self.logger = logging.getLogger(__name__)
            self.connectors = {}
            self.security_manager = EnterpriseSecurityManager()
            
        def register_enterprise_system(self, system_name: str, connector_config: Dict):
            """Register an enterprise system connector."""
            
            connector_type = connector_config['type']
            
            if connector_type == 'sap':
                self.connectors[system_name] = SAPConnector(connector_config)
            elif connector_type == 'oracle':
                self.connectors[system_name] = OracleConnector(connector_config)
            elif connector_type == 'mainframe':
                self.connectors[system_name] = MainframeConnector(connector_config)
            elif connector_type == 'api':
                self.connectors[system_name] = EnterpriseAPIConnector(connector_config)
            elif connector_type == 'file_share':
                self.connectors[system_name] = FileShareConnector(connector_config)
            
            self.logger.info(f"Registered enterprise system: {system_name}")
        
        def orchestrate_enterprise_pipeline(self, pipeline_config: Dict):
            """Orchestrate data pipeline across enterprise systems."""
            
            pipeline_id = f"enterprise_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            try:
                # Authentication and authorization
                self.security_manager.authenticate_pipeline(pipeline_config)
                
                # Data extraction from enterprise systems
                extracted_data = {}
                for source_system in pipeline_config['source_systems']:
                    system_name = source_system['name']
                    
                    if system_name not in self.connectors:
                        raise ValueError(f"System {system_name} not registered")
                    
                    self.logger.info(f"Extracting data from {system_name}")
                    
                    # Apply security policies
                    security_context = self.security_manager.get_security_context(system_name)
                    
                    # Extract data with security context
                    data = self.connectors[system_name].extract_data(
                        source_system['extraction_config'],
                        security_context
                    )
                    
                    extracted_data[system_name] = data
                
                # Data integration and transformation
                integrated_data = self.integrate_enterprise_data(extracted_data, pipeline_config)
                
                # Data governance and quality checks
                validated_data = self.apply_enterprise_governance(integrated_data, pipeline_config)
                
                # Load to target systems
                self.load_to_target_systems(validated_data, pipeline_config['target_systems'])
                
                # Audit logging
                self.security_manager.log_pipeline_execution(pipeline_id, 'SUCCESS')
                
                return {
                    'pipeline_id': pipeline_id,
                    'status': 'SUCCESS',
                    'records_processed': validated_data.count()
                }
                
            except Exception as e:
                self.security_manager.log_pipeline_execution(pipeline_id, 'FAILED', str(e))
                raise
        
        def integrate_enterprise_data(self, extracted_data: Dict, config: Dict):
            """Integrate data from multiple enterprise systems."""
            
            integration_rules = config.get('integration_rules', [])
            
            # Start with primary dataset
            primary_system = config['primary_system']
            integrated_dataset = extracted_data[primary_system]
            
            # Apply integration rules
            for rule in integration_rules:
                if rule['type'] == 'join':
                    secondary_system = rule['secondary_system']
                    join_key = rule['join_key']
                    join_type = rule.get('join_type', 'left')
                    
                    integrated_dataset = integrated_dataset.join(
                        extracted_data[secondary_system],
                        on=join_key,
                        how=join_type
                    )
                
                elif rule['type'] == 'union':
                    secondary_system = rule['secondary_system']
                    integrated_dataset = integrated_dataset.union(
                        extracted_data[secondary_system]
                    )
                
                elif rule['type'] == 'enrichment':
                    # Apply custom enrichment logic
                    enrichment_func = self._get_enrichment_function(rule['function'])
                    integrated_dataset = integrated_dataset.map_batches(enrichment_func)
            
            return integrated_dataset

**Legacy System Integration**

Connect with mainframe and legacy systems:

.. code-block:: python

    class MainframeConnector:
        """Connector for mainframe systems."""
        
        def __init__(self, config: Dict):
            self.config = config
            self.connection_pool = self._create_connection_pool()
        
        def extract_data(self, extraction_config: Dict, security_context: Dict):
            """Extract data from mainframe systems."""
            
            # Connect to mainframe via appropriate protocol
            if self.config['protocol'] == 'db2':
                return self._extract_from_db2(extraction_config, security_context)
            elif self.config['protocol'] == 'ims':
                return self._extract_from_ims(extraction_config, security_context)
            elif self.config['protocol'] == 'vsam':
                return self._extract_from_vsam(extraction_config, security_context)
            else:
                raise ValueError(f"Unsupported mainframe protocol: {self.config['protocol']}")
        
        def _extract_from_db2(self, config: Dict, security_context: Dict):
            """Extract data from DB2 on mainframe."""
            
            # Use Ray Data's SQL connector with mainframe-specific optimizations
            connection_string = self._build_db2_connection_string(security_context)
            
            dataset = ray.data.read_sql(
                connection_string,
                config['query'],
                parallelism=config.get('parallelism', 1)  # Limited for mainframe
            )
            
            # Apply mainframe-specific data transformations
            def transform_mainframe_data(batch):
                # Handle EBCDIC encoding if needed
                if config.get('encoding') == 'ebcdic':
                    batch = self._convert_from_ebcdic(batch)
                
                # Handle packed decimal fields
                if 'packed_decimal_fields' in config:
                    batch = self._convert_packed_decimals(batch, config['packed_decimal_fields'])
                
                # Add extraction metadata
                batch['_extracted_from'] = 'mainframe_db2'
                batch['_extracted_at'] = datetime.now()
                
                return batch
            
            return dataset.map_batches(transform_mainframe_data)

**API Integration Patterns**

Integrate with enterprise APIs and web services:

.. code-block:: python

    class EnterpriseAPIConnector:
        """Connector for enterprise APIs and web services."""
        
        def __init__(self, config: Dict):
            self.config = config
            self.auth_manager = APIAuthenticationManager(config.get('auth', {}))
        
        def extract_data(self, extraction_config: Dict, security_context: Dict):
            """Extract data from enterprise APIs."""
            
            api_type = extraction_config['api_type']
            
            if api_type == 'rest':
                return self._extract_from_rest_api(extraction_config, security_context)
            elif api_type == 'soap':
                return self._extract_from_soap_api(extraction_config, security_context)
            elif api_type == 'graphql':
                return self._extract_from_graphql_api(extraction_config, security_context)
            else:
                raise ValueError(f"Unsupported API type: {api_type}")
        
        def _extract_from_rest_api(self, config: Dict, security_context: Dict):
            """Extract data from REST APIs with pagination and rate limiting."""
            
            def fetch_api_data(batch_params):
                """Fetch data from API with proper authentication and error handling."""
                
                import requests
                import time
                
                results = []
                
                for params in batch_params.to_pylist():
                    try:
                        # Get authentication token
                        auth_token = self.auth_manager.get_token(security_context)
                        
                        # Make API request
                        headers = {
                            'Authorization': f'Bearer {auth_token}',
                            'Content-Type': 'application/json'
                        }
                        
                        response = requests.get(
                            config['endpoint'],
                            headers=headers,
                            params=params,
                            timeout=config.get('timeout', 30)
                        )
                        
                        response.raise_for_status()
                        
                        # Parse response
                        data = response.json()
                        if isinstance(data, list):
                            results.extend(data)
                        else:
                            results.append(data)
                        
                        # Rate limiting
                        if 'rate_limit_delay' in config:
                            time.sleep(config['rate_limit_delay'])
                            
                    except requests.exceptions.RequestException as e:
                        self.logger.error(f"API request failed: {e}")
                        # Implement retry logic or error handling
                        continue
                
                return pd.DataFrame(results)
            
            # Create parameter batches for API calls
            api_parameters = self._generate_api_parameters(config)
            parameter_dataset = ray.data.from_pandas(pd.DataFrame(api_parameters))
            
            # Execute API calls in parallel
            api_data = parameter_dataset.map_batches(fetch_api_data)
            
            return api_data

Security and Governance
-----------------------

**Enterprise Security Framework**

.. code-block:: python

    class EnterpriseSecurityManager:
        """Comprehensive security management for enterprise data processing."""
        
        def __init__(self):
            self.audit_logger = AuditLogger()
            self.encryption_manager = EncryptionManager()
            self.access_control = AccessControlManager()
        
        def authenticate_pipeline(self, pipeline_config: Dict):
            """Authenticate and authorize pipeline execution."""
            
            # Multi-factor authentication
            user_context = self._authenticate_user(pipeline_config['user_credentials'])
            
            # Role-based access control
            required_permissions = pipeline_config.get('required_permissions', [])
            self._check_permissions(user_context, required_permissions)
            
            # Data classification access
            data_classifications = pipeline_config.get('data_classifications', [])
            self._check_data_access_rights(user_context, data_classifications)
            
            # Audit logging
            self.audit_logger.log_authentication(user_context, pipeline_config)
        
        def apply_data_protection(self, dataset, protection_policies: List[Dict]):
            """Apply data protection policies."""
            
            def apply_protection_policies(batch):
                """Apply encryption, masking, and tokenization."""
                
                protected_batch = batch.copy()
                
                for policy in protection_policies:
                    if policy['type'] == 'encryption':
                        columns = policy['columns']
                        for col in columns:
                            if col in protected_batch.columns:
                                protected_batch[col] = self.encryption_manager.encrypt_column(
                                    protected_batch[col], 
                                    policy['encryption_key_id']
                                )
                    
                    elif policy['type'] == 'masking':
                        columns = policy['columns']
                        mask_type = policy['mask_type']
                        for col in columns:
                            if col in protected_batch.columns:
                                protected_batch[col] = self._apply_data_masking(
                                    protected_batch[col], 
                                    mask_type
                                )
                    
                    elif policy['type'] == 'tokenization':
                        columns = policy['columns']
                        for col in columns:
                            if col in protected_batch.columns:
                                protected_batch[col] = self._tokenize_column(
                                    protected_batch[col], 
                                    policy['token_vault_id']
                                )
                
                return protected_batch
            
            return dataset.map_batches(apply_protection_policies)
        
        def _apply_data_masking(self, column_data, mask_type: str):
            """Apply different types of data masking."""
            
            if mask_type == 'full_mask':
                return column_data.apply(lambda x: '*' * len(str(x)) if pd.notna(x) else x)
            
            elif mask_type == 'partial_mask':
                def partial_mask(value):
                    if pd.isna(value):
                        return value
                    str_val = str(value)
                    if len(str_val) <= 4:
                        return '*' * len(str_val)
                    return str_val[:2] + '*' * (len(str_val) - 4) + str_val[-2:]
                
                return column_data.apply(partial_mask)
            
            elif mask_type == 'format_preserving':
                # Implement format-preserving encryption/masking
                return column_data.apply(self._format_preserving_mask)
            
            else:
                raise ValueError(f"Unknown mask type: {mask_type}")

**Compliance and Audit**

.. code-block:: python

    class ComplianceManager:
        """Manage compliance with enterprise regulations."""
        
        def __init__(self):
            self.regulations = {
                'gdpr': GDPRComplianceHandler(),
                'hipaa': HIPAAComplianceHandler(),
                'sox': SOXComplianceHandler(),
                'pci_dss': PCIDSSComplianceHandler()
            }
        
        def ensure_compliance(self, dataset, compliance_requirements: List[str]):
            """Ensure data processing meets compliance requirements."""
            
            def apply_compliance_rules(batch):
                """Apply compliance-specific data handling rules."""
                
                compliant_batch = batch.copy()
                
                for requirement in compliance_requirements:
                    if requirement in self.regulations:
                        handler = self.regulations[requirement]
                        compliant_batch = handler.process_data(compliant_batch)
                
                # Add compliance metadata
                compliant_batch['_compliance_applied'] = compliance_requirements
                compliant_batch['_compliance_timestamp'] = datetime.now()
                
                return compliant_batch
            
            return dataset.map_batches(apply_compliance_rules)
        
        def generate_compliance_report(self, pipeline_execution_log: Dict):
            """Generate compliance report for audit purposes."""
            
            report = {
                'pipeline_id': pipeline_execution_log['pipeline_id'],
                'execution_timestamp': pipeline_execution_log['timestamp'],
                'compliance_checks': [],
                'data_lineage': pipeline_execution_log.get('lineage', []),
                'security_controls': pipeline_execution_log.get('security_controls', [])
            }
            
            # Check each compliance requirement
            for requirement in pipeline_execution_log.get('compliance_requirements', []):
                if requirement in self.regulations:
                    handler = self.regulations[requirement]
                    check_result = handler.validate_compliance(pipeline_execution_log)
                    report['compliance_checks'].append({
                        'regulation': requirement,
                        'status': check_result['status'],
                        'details': check_result['details']
                    })
            
            return report

Multi-Tenancy and Resource Isolation
------------------------------------

**Enterprise Multi-Tenancy**

.. code-block:: python

    class EnterpriseTenantManager:
        """Manage multi-tenant data processing with resource isolation."""
        
        def __init__(self, cluster_config: Dict):
            self.cluster_config = cluster_config
            self.tenant_configs = {}
            self.resource_allocator = ResourceAllocator()
        
        def register_tenant(self, tenant_id: str, tenant_config: Dict):
            """Register a new tenant with specific resource allocations."""
            
            self.tenant_configs[tenant_id] = {
                'resource_quota': tenant_config['resource_quota'],
                'security_policies': tenant_config['security_policies'],
                'data_isolation_level': tenant_config.get('data_isolation_level', 'strict'),
                'allowed_data_sources': tenant_config.get('allowed_data_sources', []),
                'compliance_requirements': tenant_config.get('compliance_requirements', [])
            }
        
        def execute_tenant_pipeline(self, tenant_id: str, pipeline_config: Dict):
            """Execute pipeline with tenant-specific resource isolation."""
            
            if tenant_id not in self.tenant_configs:
                raise ValueError(f"Tenant {tenant_id} not registered")
            
            tenant_config = self.tenant_configs[tenant_id]
            
            # Allocate tenant-specific resources
            resource_context = self.resource_allocator.allocate_resources(
                tenant_id, 
                tenant_config['resource_quota']
            )
            
            try:
                # Create isolated Ray runtime
                with self._create_tenant_runtime(tenant_id, resource_context):
                    
                    # Apply tenant security policies
                    secured_pipeline = self._apply_tenant_security(
                        pipeline_config, 
                        tenant_config['security_policies']
                    )
                    
                    # Execute pipeline with isolation
                    result = self._execute_isolated_pipeline(
                        secured_pipeline, 
                        tenant_config
                    )
                    
                    return result
                    
            finally:
                # Clean up tenant resources
                self.resource_allocator.deallocate_resources(tenant_id, resource_context)
        
        def _create_tenant_runtime(self, tenant_id: str, resource_context: Dict):
            """Create isolated Ray runtime for tenant."""
            
            runtime_config = {
                'namespace': f'tenant_{tenant_id}',
                'resources': resource_context['allocated_resources'],
                'object_store_memory': resource_context['object_store_quota'],
                'redis_password': resource_context['isolation_key']
            }
            
            return ray.init(**runtime_config)

**Resource Management**

.. code-block:: python

    class ResourceAllocator:
        """Allocate and manage resources for enterprise workloads."""
        
        def __init__(self):
            self.resource_pools = {
                'cpu_intensive': {'cpu_cores': 100, 'memory_gb': 400},
                'memory_intensive': {'cpu_cores': 50, 'memory_gb': 800},
                'gpu_enabled': {'cpu_cores': 32, 'memory_gb': 200, 'gpus': 8},
                'general_purpose': {'cpu_cores': 64, 'memory_gb': 256}
            }
            self.active_allocations = {}
        
        def allocate_resources(self, tenant_id: str, resource_requirements: Dict):
            """Allocate resources based on workload requirements."""
            
            workload_type = resource_requirements.get('workload_type', 'general_purpose')
            scale_factor = resource_requirements.get('scale_factor', 1.0)
            
            if workload_type not in self.resource_pools:
                raise ValueError(f"Unknown workload type: {workload_type}")
            
            base_resources = self.resource_pools[workload_type]
            allocated_resources = {
                key: int(value * scale_factor) 
                for key, value in base_resources.items()
            }
            
            # Check resource availability
            if not self._check_resource_availability(allocated_resources):
                raise ResourceExhaustedError("Insufficient resources available")
            
            # Reserve resources
            allocation_id = f"{tenant_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.active_allocations[allocation_id] = {
                'tenant_id': tenant_id,
                'allocated_resources': allocated_resources,
                'allocation_time': datetime.now()
            }
            
            return {
                'allocation_id': allocation_id,
                'allocated_resources': allocated_resources,
                'isolation_key': self._generate_isolation_key(tenant_id)
            }

Deployment Patterns
------------------

**Enterprise Deployment Architecture**

.. code-block:: python

    class EnterpriseDeploymentManager:
        """Manage enterprise deployment patterns."""
        
        def __init__(self):
            self.deployment_strategies = {
                'on_premises': OnPremisesDeployment(),
                'hybrid_cloud': HybridCloudDeployment(),
                'multi_cloud': MultiCloudDeployment(),
                'edge_computing': EdgeComputingDeployment()
            }
        
        def deploy_ray_data_cluster(self, deployment_config: Dict):
            """Deploy Ray Data cluster based on enterprise requirements."""
            
            deployment_type = deployment_config['type']
            
            if deployment_type not in self.deployment_strategies:
                raise ValueError(f"Unknown deployment type: {deployment_type}")
            
            strategy = self.deployment_strategies[deployment_type]
            
            # Prepare deployment
            deployment_plan = strategy.create_deployment_plan(deployment_config)
            
            # Apply security configurations
            security_config = self._apply_enterprise_security(deployment_config)
            
            # Deploy infrastructure
            infrastructure = strategy.deploy_infrastructure(deployment_plan, security_config)
            
            # Configure monitoring and observability
            monitoring_config = self._setup_enterprise_monitoring(deployment_config)
            
            # Deploy Ray Data cluster
            cluster = strategy.deploy_ray_cluster(infrastructure, monitoring_config)
            
            return {
                'cluster_id': cluster['cluster_id'],
                'endpoints': cluster['endpoints'],
                'monitoring_urls': monitoring_config['urls'],
                'deployment_status': 'SUCCESS'
            }

Best Practices for Enterprise Integration
----------------------------------------

**1. Security First**

* Implement comprehensive authentication and authorization
* Use encryption for data in transit and at rest
* Apply data masking and tokenization for sensitive data
* Maintain detailed audit logs for compliance

**2. Scalable Architecture**

* Design for multi-tenancy with proper resource isolation
* Implement horizontal scaling patterns
* Use enterprise-grade monitoring and alerting
* Plan for disaster recovery and business continuity

**3. Governance and Compliance**

* Implement data governance frameworks
* Ensure compliance with relevant regulations
* Maintain comprehensive data lineage
* Provide self-service capabilities with proper controls

**4. Integration Patterns**

* Use standard APIs and protocols where possible
* Implement proper error handling and retry logic
* Design for eventual consistency in distributed systems
* Plan for legacy system modernization

**5. Operational Excellence**

* Implement comprehensive monitoring and observability
* Use infrastructure as code for deployments
* Automate testing and validation processes
* Plan for capacity management and cost optimization

Next Steps
----------

* **Advanced Analytics**: Implement advanced analytical capabilities → :ref:`advanced-analytics`
* **Performance Optimization**: Optimize for enterprise workloads → :ref:`performance-optimization`
* **Production Deployment**: Deploy with enterprise patterns → :ref:`production-deployment`
* **Cloud Integration**: Leverage cloud-native capabilities → :ref:`cloud-platforms`
