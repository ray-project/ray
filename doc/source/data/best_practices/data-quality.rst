.. _data-quality:

Data Quality Management
=======================

Implementing comprehensive data quality management is essential for production Ray Data deployments. This guide covers validation strategies, monitoring approaches, and quality assurance best practices.

**What you'll learn:**

* Data validation patterns and implementation strategies
* Data quality monitoring and alerting systems
* Quality metrics and performance tracking
* Production data quality best practices

Data Quality Framework
----------------------

**Multi-Layer Validation Strategy**

Implement validation at multiple layers of your data pipeline:

.. code-block:: python

    import ray
    import pandas as pd
    from typing import Dict, List, Any
    import logging

    # Initialize validation framework
    logger = logging.getLogger(__name__)
    validation_rules = {}
    quality_metrics = {}

    def validate_schema(batch: pd.DataFrame, expected_schema: Dict[str, Any]) -> Dict:
        """Validate data against expected schema."""
        validation_results = {
            'schema_valid': True,
            'missing_columns': [],
            'type_mismatches': [],
            'validation_timestamp': pd.Timestamp.now()
        }
        
        # Check for missing columns
        expected_columns = set(expected_schema.keys())
        actual_columns = set(batch.columns)
        missing_columns = expected_columns - actual_columns
        
        if missing_columns:
            validation_results['schema_valid'] = False
            validation_results['missing_columns'] = list(missing_columns)
        
        # Check data types
        for column, expected_type in expected_schema.items():
            if column in batch.columns:
                actual_type = batch[column].dtype
                if not is_compatible_type(actual_type, expected_type):
                    validation_results['schema_valid'] = False
                    validation_results['type_mismatches'].append({
                        'column': column,
                        'expected': str(expected_type),
                        'actual': str(actual_type)
                    })
        
        return validation_results

    def validate_business_rules(batch: pd.DataFrame) -> Dict:
        """Apply business rule validations."""
        validation_results = {
            'rules_passed': 0,
            'rules_failed': 0,
            'failed_rules': [],
            'validation_details': []
        }
        
        for rule_name, rule_func in validation_rules.items():
            try:
                rule_result = rule_func(batch)
                if rule_result['passed']:
                    validation_results['rules_passed'] += 1
                else:
                    validation_results['rules_failed'] += 1
                    validation_results['failed_rules'].append(rule_name)
                    validation_results['validation_details'].append({
                        'rule': rule_name,
                        'details': rule_result.get('details', 'Rule failed')
                    })
            except Exception as e:
                logger.error(f"Validation rule {rule_name} failed with error: {e}")
                validation_results['rules_failed'] += 1
                validation_results['failed_rules'].append(rule_name)
        
        return validation_results

    def calculate_quality_metrics(batch: pd.DataFrame) -> Dict:
        """Calculate comprehensive data quality metrics."""
        total_rows = len(batch)
        total_cells = total_rows * len(batch.columns)
        
        metrics = {
            'total_rows': total_rows,
            'total_columns': len(batch.columns),
            'completeness_rate': 0.0,
            'uniqueness_rate': 0.0,
            'validity_rate': 0.0,
            'consistency_rate': 0.0,
            'column_metrics': {}
        }
        
        if total_rows > 0:
                # Completeness: percentage of non-null values
                null_count = batch.isnull().sum().sum()
                metrics['completeness_rate'] = (total_cells - null_count) / total_cells
                
                # Uniqueness: percentage of unique rows
                unique_rows = len(batch.drop_duplicates())
                metrics['uniqueness_rate'] = unique_rows / total_rows
                
                # Column-level metrics
                for column in batch.columns:
                    col_data = batch[column]
                    metrics['column_metrics'][column] = {
                        'completeness': col_data.notna().mean(),
                        'uniqueness': col_data.nunique() / len(col_data) if len(col_data) > 0 else 0,
                        'data_type': str(col_data.dtype)
                    }
            
            return metrics
        
        def _is_compatible_type(self, actual_type, expected_type) -> bool:
            """Check if actual data type is compatible with expected type."""
            # Implement type compatibility logic
            type_compatibility = {
                'int64': ['int32', 'int64', 'float64'],
                'float64': ['int32', 'int64', 'float32', 'float64'],
                'object': ['object', 'string'],
                'datetime64[ns]': ['datetime64[ns]', 'object']
            }
            
            expected_str = str(expected_type)
            actual_str = str(actual_type)
            
            return actual_str in type_compatibility.get(expected_str, [expected_str])

**Business Rule Validation**

Define and implement business-specific validation rules:

.. code-block:: python

    def create_business_validation_rules():
        """Create business-specific validation rules."""
        
        quality_framework = DataQualityFramework()
        
        # Rule 1: Customer age validation
        def validate_customer_age(batch):
            invalid_ages = batch[(batch['age'] < 0) | (batch['age'] > 150)]
            return {
                'passed': len(invalid_ages) == 0,
                'details': f"Found {len(invalid_ages)} records with invalid ages"
            }
        
        # Rule 2: Email format validation
        def validate_email_format(batch):
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            if 'email' in batch.columns:
                invalid_emails = batch[~batch['email'].str.match(email_pattern, na=False)]
                return {
                    'passed': len(invalid_emails) == 0,
                    'details': f"Found {len(invalid_emails)} records with invalid email formats"
                }
            return {'passed': True, 'details': 'Email column not found'}
        
        # Rule 3: Revenue consistency validation
        def validate_revenue_consistency(batch):
            if all(col in batch.columns for col in ['quantity', 'unit_price', 'total_revenue']):
                calculated_revenue = batch['quantity'] * batch['unit_price']
                inconsistent = batch[abs(batch['total_revenue'] - calculated_revenue) > 0.01]
                return {
                    'passed': len(inconsistent) == 0,
                    'details': f"Found {len(inconsistent)} records with revenue calculation inconsistencies"
                }
            return {'passed': True, 'details': 'Required revenue columns not found'}
        
        # Add rules to framework
        quality_framework.add_validation_rule('customer_age', validate_customer_age)
        quality_framework.add_validation_rule('email_format', validate_email_format)
        quality_framework.add_validation_rule('revenue_consistency', validate_revenue_consistency)
        
        return quality_framework

**Advanced Business Rule Validation**

Implement sophisticated business rule validation with domain knowledge encoding:

.. code-block:: python

    # Define advanced business validation rules
    def customer_age_validation(batch: pd.DataFrame) -> Dict:
        """Validate customer age business rules."""
        validation_result = {
            'passed': True,
            'details': [],
            'violations': []
        }
        
        # Business rule: Customers must be 18+ for financial products
        if 'age' in batch.columns:
            underage_customers = batch[batch['age'] < 18]
            if len(underage_customers) > 0:
                validation_result['passed'] = False
                validation_result['violations'].append({
                    'rule': 'minimum_age_18',
                    'count': len(underage_customers),
                    'details': 'Customers under 18 found'
                })
        
        # Business rule: Age must be reasonable (0-120)
        if 'age' in batch.columns:
            invalid_ages = batch[(batch['age'] < 0) | (batch['age'] > 120)]
            if len(invalid_ages) > 0:
                validation_result['passed'] = False
                validation_result['violations'].append({
                    'rule': 'reasonable_age_range',
                    'count': len(invalid_ages),
                    'details': 'Ages outside reasonable range (0-120)'
                })
        
        return validation_result

    def financial_amount_validation(batch: pd.DataFrame) -> Dict:
        """Validate financial amount business rules."""
        validation_result = {
            'passed': True,
            'details': [],
            'violations': []
        }
        
        # Business rule: Transaction amounts must be positive
        if 'amount' in batch.columns:
            negative_amounts = batch[batch['amount'] < 0]
            if len(negative_amounts) > 0:
                validation_result['passed'] = False
                validation_result['violations'].append({
                    'rule': 'positive_amounts',
                    'count': len(negative_amounts),
                    'details': 'Negative transaction amounts found'
                })
        
        # Business rule: Amounts must be within reasonable limits
        if 'amount' in batch.columns:
            # Flag amounts over $1M for review
            large_amounts = batch[batch['amount'] > 1000000]
            if len(large_amounts) > 0:
                validation_result['details'].append({
                    'rule': 'large_amount_review',
                    'count': len(large_amounts),
                    'details': 'Amounts over $1M require manual review'
                })
        
        return validation_result

    # Register advanced validation rules
    quality_framework.add_validation_rule('customer_age_advanced', customer_age_validation)
    quality_framework.add_validation_rule('financial_amount_advanced', financial_amount_validation)

**Domain Knowledge Encoding Patterns**

Implement comprehensive domain knowledge encoding for different business domains:

.. code-block:: python

    # Financial Services Domain Rules
    def financial_compliance_validation(batch: pd.DataFrame) -> Dict:
        """Validate financial compliance rules."""
        validation_result = {
            'passed': True,
            'details': [],
            'violations': []
        }
        
        # Anti-money laundering (AML) rules
        if 'transaction_amount' in batch.columns and 'customer_risk_level' in batch.columns:
            # High-risk customers have lower transaction limits
            high_risk_large_txns = batch[
                (batch['customer_risk_level'] == 'high') & 
                (batch['transaction_amount'] > 10000)
            ]
            if len(high_risk_large_txns) > 0:
                validation_result['violations'].append({
                    'rule': 'aml_high_risk_limit',
                    'count': len(high_risk_large_txns),
                    'details': 'High-risk customers exceeding transaction limits'
                })
        
        # Know Your Customer (KYC) validation
        if 'customer_id' in batch.columns and 'kyc_status' in batch.columns:
            incomplete_kyc = batch[batch['kyc_status'] != 'complete']
            if len(incomplete_kyc) > 0:
                validation_result['details'].append({
                    'rule': 'kyc_completion',
                    'count': len(incomplete_kyc),
                    'details': 'Customers with incomplete KYC'
                })
        
        return validation_result

    # Healthcare Domain Rules
    def healthcare_compliance_validation(batch: pd.DataFrame) -> Dict:
        """Validate healthcare compliance rules."""
        validation_result = {
            'passed': True,
            'details': [],
            'violations': []
        }
        
        # HIPAA compliance checks
        if 'patient_id' in batch.columns and 'data_sensitivity' in batch.columns:
            # Sensitive data must have proper access controls
            sensitive_data = batch[batch['data_sensitivity'] == 'high']
            if len(sensitive_data) > 0:
                validation_result['details'].append({
                    'rule': 'hipaa_sensitive_data',
                    'count': len(sensitive_data),
                    'details': 'High-sensitivity patient data requires special handling'
                })
        
        # Medical record completeness
        if 'diagnosis_code' in batch.columns and 'treatment_plan' in batch.columns:
            incomplete_records = batch[
                batch['diagnosis_code'].isna() | 
                batch['treatment_plan'].isna()
            ]
            if len(incomplete_records) > 0:
                validation_result['violations'].append({
                    'rule': 'medical_record_completeness',
                    'count': len(incomplete_records),
                    'details': 'Incomplete medical records found'
                })
        
        return validation_result

    # E-commerce Domain Rules
    def ecommerce_business_validation(batch: pd.DataFrame) -> Dict:
        """Validate e-commerce business rules."""
        validation_result = {
            'passed': True,
            'details': [],
            'violations': []
        }
        
        # Inventory consistency
        if 'product_id' in batch.columns and 'available_quantity' in batch.columns:
            negative_inventory = batch[batch['available_quantity'] < 0]
            if len(negative_inventory) > 0:
                validation_result['passed'] = False
                validation_result['violations'].append({
                    'rule': 'inventory_consistency',
                    'count': len(negative_inventory),
                    'details': 'Negative inventory quantities found'
                })
        
        # Pricing validation
        if 'unit_price' in batch.columns and 'discount_percentage' in batch.columns:
            # Discounts cannot exceed 100%
            invalid_discounts = batch[batch['discount_percentage'] > 100]
            if len(invalid_discounts) > 0:
                validation_result['passed'] = False
                validation_result['violations'].append({
                    'rule': 'discount_validation',
                    'count': len(invalid_discounts),
                    'details': 'Discount percentages exceed 100%'
                })
        
        return validation_result

    # Register domain-specific rules
    quality_framework.add_validation_rule('financial_compliance', financial_compliance_validation)
    quality_framework.add_validation_rule('healthcare_compliance', healthcare_compliance_validation)
    quality_framework.add_validation_rule('ecommerce_business', ecommerce_business_validation)

Data Quality Monitoring
-----------------------

**Continuous Quality Monitoring**

Implement continuous monitoring of data quality metrics:

.. code-block:: python

    import ray
    from ray.data.aggregate import Count, Mean
    import json
    from datetime import datetime

    # Initialize monitoring system
    alert_thresholds = {
        'completeness': 0.95,
        'uniqueness': 0.90,
        'business_rules': 0.05
    }
    quality_history = []

    def quality_check_and_monitor(batch):
        """Apply quality checks and collect metrics."""
        
        # Initialize quality framework
        quality_framework = create_business_validation_rules()
        
        # Run validations
        schema_validation = quality_framework.validate_schema(batch, get_expected_schema())
        business_validation = quality_framework.validate_business_rules(batch)
        quality_metrics = quality_framework.calculate_quality_metrics(batch)
        
        # Combine results
        quality_report = {
            'pipeline_name': 'data_pipeline',
            'timestamp': datetime.now().isoformat(),
            'batch_size': len(batch),
            'schema_validation': schema_validation,
            'business_validation': business_validation,
            'quality_metrics': quality_metrics
        }
        
        # Check for alerts
        check_quality_alerts(quality_report)
        
        # Store quality history
        quality_history.append(quality_report)
        
        return batch

    def check_quality_alerts(quality_report: Dict):
        """Check if quality metrics exceed alert thresholds."""
        
        metrics = quality_report['quality_metrics']
        alerts = []
        
        # Check completeness threshold
        if metrics['completeness_rate'] < alert_thresholds.get('completeness', 0.95):
            alerts.append({
                'type': 'completeness',
                'severity': 'high',
                'message': f"Data completeness {metrics['completeness_rate']:.2%} below threshold"
            })
        
        # Check uniqueness threshold
        if metrics['uniqueness_rate'] < alert_thresholds.get('uniqueness', 0.90):
            alerts.append({
                'type': 'uniqueness',
                'severity': 'medium',
                'message': f"Data uniqueness {metrics['uniqueness_rate']:.2%} below threshold"
            })
        
        # Check business rule failures
        business_validation = quality_report['business_validation']
        total_rules = business_validation['rules_passed'] + business_validation['rules_failed']
        if total_rules > 0:
            failure_rate = business_validation['rules_failed'] / total_rules
            if failure_rate > alert_thresholds.get('business_rules', 0.05):
                alerts.append({
                    'type': 'business_rules',
                    'severity': 'high',
                    'message': f"Business rule failure rate {failure_rate:.2%} exceeds threshold"
                })
        
        # Send alerts if any
        if alerts:
            send_alerts(alerts)

    def send_alerts(alerts: List[Dict]):
        """Send quality alerts to monitoring systems."""
        for alert in alerts:
            # Log alert
            print(f"QUALITY ALERT: {alert['message']}")
            
            # Send to external monitoring system
                self._send_to_monitoring_system(alert)
        
        def _send_to_monitoring_system(self, alert: Dict):
            """Send alert to external monitoring system (implement based on your system)."""
            # Example: Send to DataDog, PagerDuty, Slack, etc.
            pass
        
        def _get_expected_schema(self) -> Dict[str, Any]:
            """Get expected schema for validation."""
            return {
                'customer_id': 'int64',
                'email': 'object',
                'age': 'int64',
                'total_revenue': 'float64',
                'created_date': 'datetime64[ns]'
            }

**Comprehensive Monitoring and Alerting System**

Implement enterprise-grade monitoring with intelligent alerting:

.. code-block:: python

    import time
    from datetime import datetime, timedelta
    import json

    class DataQualityMonitoringSystem:
        """Enterprise data quality monitoring system."""
        
        def __init__(self):
            self.alert_history = []
            self.quality_trends = {}
            self.escalation_policies = {}
            self.integration_configs = {}
            
        def setup_monitoring_integrations(self):
            """Configure monitoring system integrations."""
            
            # DataDog integration
            self.integration_configs['datadog'] = {
                'api_key': 'your_datadog_api_key',
                'app_key': 'your_datadog_app_key',
                'endpoint': 'https://api.datadoghq.com/api/v1'
            }
            
            # Slack integration for alerts
            self.integration_configs['slack'] = {
                'webhook_url': 'your_slack_webhook_url',
                'channel': '#data-quality-alerts',
                'username': 'Data Quality Bot'
            }
            
            # PagerDuty integration for critical alerts
            self.integration_configs['pagerduty'] = {
                'api_key': 'your_pagerduty_api_key',
                'service_id': 'your_service_id'
            }
            
            # Email integration
            self.integration_configs['email'] = {
                'smtp_server': 'smtp.company.com',
                'from_address': 'data-quality@company.com',
                'to_addresses': ['data-team@company.com', 'oncall@company.com']
            }
        
        def define_escalation_policies(self):
            """Define alert escalation policies."""
            
            self.escalation_policies = {
                'low': {
                    'channels': ['slack'],
                    'delay_minutes': 60,
                    'escalation_channels': ['email']
                },
                'medium': {
                    'channels': ['slack', 'email'],
                    'delay_minutes': 30,
                    'escalation_channels': ['pagerduty']
                },
                'high': {
                    'channels': ['slack', 'email', 'pagerduty'],
                    'delay_minutes': 15,
                    'escalation_channels': ['phone']
                },
                'critical': {
                    'channels': ['slack', 'email', 'pagerduty', 'phone'],
                    'delay_minutes': 5,
                    'escalation_channels': ['immediate_escalation']
                }
            }
        
        def send_intelligent_alerts(self, alerts: List[Dict]):
            """Send alerts with intelligent routing and escalation."""
            
            for alert in alerts:
                # Determine alert severity
                severity = self._calculate_alert_severity(alert)
                
                # Add to alert history
                alert_record = {
                    'timestamp': datetime.now().isoformat(),
                    'alert': alert,
                    'severity': severity,
                    'status': 'sent'
                }
                self.alert_history.append(alert_record)
                
                # Send to appropriate channels
                self._send_to_channels(alert, severity)
                
                # Schedule escalation if needed
                self._schedule_escalation(alert, severity)
        
        def _calculate_alert_severity(self, alert: Dict) -> str:
            """Calculate alert severity based on multiple factors."""
            
            severity_score = 0
            
            # Factor 1: Data volume affected
            if 'batch_size' in alert:
                if alert['batch_size'] > 1000000:  # >1M records
                    severity_score += 3
                elif alert['batch_size'] > 100000:  # >100K records
                    severity_score += 2
                elif alert['batch_size'] > 10000:  # >10K records
                    severity_score += 1
            
            # Factor 2: Quality metric degradation
            if 'quality_metrics' in alert:
                metrics = alert['quality_metrics']
                if metrics.get('completeness_rate', 1.0) < 0.8:
                    severity_score += 2
                if metrics.get('validity_rate', 1.0) < 0.9:
                    severity_score += 2
            
            # Factor 3: Business impact
            if 'business_validation' in alert:
                business_val = alert['business_validation']
                if business_val.get('rules_failed', 0) > 0:
                    severity_score += 2
            
            # Factor 4: Time sensitivity
            if 'pipeline_name' in alert:
                if 'real_time' in alert['pipeline_name'].lower():
                    severity_score += 1
                if 'financial' in alert['pipeline_name'].lower():
                    severity_score += 1
            
            # Determine severity level
            if severity_score >= 6:
                return 'critical'
            elif severity_score >= 4:
                return 'high'
            elif severity_score >= 2:
                return 'medium'
            else:
                return 'low'
        
        def _send_to_channels(self, alert: Dict, severity: str):
            """Send alert to configured channels."""
            
            policy = self.escalation_policies.get(severity, {})
            channels = policy.get('channels', [])
            
            for channel in channels:
                try:
                    if channel == 'slack':
                        self._send_to_slack(alert, severity)
                    elif channel == 'email':
                        self._send_to_email(alert, severity)
                    elif channel == 'pagerduty':
                        self._send_to_pagerduty(alert, severity)
                except Exception as e:
                    print(f"Failed to send alert to {channel}: {e}")
        
        def _schedule_escalation(self, alert: Dict, severity: str):
            """Schedule alert escalation if needed."""
            
            policy = self.escalation_policies.get(severity, {})
            delay_minutes = policy.get('delay_minutes', 60)
            escalation_channels = policy.get('escalation_channels', [])
            
            if escalation_channels:
                # Schedule escalation after delay
                escalation_time = datetime.now() + timedelta(minutes=delay_minutes)
                # In production, use a proper task scheduler
                print(f"Escalation scheduled for {escalation_time} via {escalation_channels}")
        
        def generate_quality_trends(self, time_window_hours: int = 24):
            """Generate quality trends for monitoring."""
            
            cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
            recent_alerts = [
                alert for alert in self.alert_history
                if datetime.fromisoformat(alert['timestamp']) > cutoff_time
            ]
            
            # Calculate trends
            severity_counts = {}
            alert_types = {}
            
            for alert in recent_alerts:
                severity = alert['severity']
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
                
                alert_type = alert['alert'].get('type', 'unknown')
                alert_types[alert_type] = alert_types.get(alert_type, 0) + 1
            
            # Store trends
            self.quality_trends = {
                'time_window_hours': time_window_hours,
                'total_alerts': len(recent_alerts),
                'severity_distribution': severity_counts,
                'alert_type_distribution': alert_types,
                'generated_at': datetime.now().isoformat()
            }
            
            return self.quality_trends

Data Lineage & Governance
-------------------------

**Data Lineage Tracking**

Implement comprehensive data lineage tracking:

.. code-block:: python

    import ray
    import json
    from typing import Dict, List
    from datetime import datetime

    class DataLineageTracker:
        """Track data lineage and transformations."""
        
        def __init__(self):
            self.lineage_graph = {}
            self.transformation_log = []
            
        def track_transformation(self, 
                               source_datasets: List[str],
                               target_dataset: str,
                               transformation_type: str,
                               transformation_details: Dict):
            """Track a data transformation step."""
            
            lineage_entry = {
                'timestamp': datetime.now().isoformat(),
                'source_datasets': source_datasets,
                'target_dataset': target_dataset,
                'transformation_type': transformation_type,
                'transformation_details': transformation_details,
                'execution_id': self._generate_execution_id()
            }
            
            self.transformation_log.append(lineage_entry)
            
            # Update lineage graph
            if target_dataset not in self.lineage_graph:
                self.lineage_graph[target_dataset] = {
                    'sources': [],
                    'transformations': []
                }
            
            self.lineage_graph[target_dataset]['sources'].extend(source_datasets)
            self.lineage_graph[target_dataset]['transformations'].append(lineage_entry)
        
        def create_lineage_aware_pipeline(self):
            """Create a pipeline with automatic lineage tracking."""
            
            def tracked_transformation(batch, transformation_name, transformation_func):
                """Apply transformation with lineage tracking."""
                
                # Record transformation start
                start_time = datetime.now()
                
                # Apply transformation
                result = transformation_func(batch)
                
                # Record transformation completion
                end_time = datetime.now()
                
                # Track lineage
                self.track_transformation(
                    source_datasets=['input_batch'],
                    target_dataset='output_batch',
                    transformation_type=transformation_name,
                    transformation_details={
                        'start_time': start_time.isoformat(),
                        'end_time': end_time.isoformat(),
                        'duration_seconds': (end_time - start_time).total_seconds(),
                        'input_rows': len(batch),
                        'output_rows': len(result),
                        'transformation_function': transformation_func.__name__
                    }
                )
                
                return result
            
            return tracked_transformation
        
        def generate_lineage_report(self, dataset_name: str) -> Dict:
            """Generate comprehensive lineage report for a dataset."""
            
            if dataset_name not in self.lineage_graph:
                return {'error': f'No lineage data found for dataset {dataset_name}'}
            
            lineage_data = self.lineage_graph[dataset_name]
            
            report = {
                'dataset_name': dataset_name,
                'source_datasets': lineage_data['sources'],
                'transformation_count': len(lineage_data['transformations']),
                'transformations': lineage_data['transformations'],
                'data_quality_history': self._get_quality_history_for_dataset(dataset_name),
                'compliance_status': self._check_compliance_status(dataset_name)
            }
            
            return report

**Advanced Lineage Tracking with Ray Data**

Implement comprehensive lineage tracking integrated with Ray Data operations:

.. code-block:: python

    class RayDataLineageTracker:
        """Advanced lineage tracking for Ray Data pipelines."""
        
        def __init__(self):
            self.operation_lineage = {}
            self.data_dependencies = {}
            self.performance_metrics = {}
            self.schema_evolution = {}
            
        def track_ray_data_operation(self, 
                                   operation_name: str,
                                   input_datasets: List[str],
                                   output_dataset: str,
                                   operation_config: Dict,
                                   performance_metrics: Dict):
            """Track Ray Data operation with detailed metadata."""
            
            lineage_entry = {
                'operation_name': operation_name,
                'timestamp': datetime.now().isoformat(),
                'input_datasets': input_datasets,
                'output_dataset': output_dataset,
                'operation_config': operation_config,
                'performance_metrics': performance_metrics,
                'ray_cluster_info': self._get_ray_cluster_info(),
                'execution_context': self._get_execution_context()
            }
            
            # Store operation lineage
            if output_dataset not in self.operation_lineage:
                self.operation_lineage[output_dataset] = []
            self.operation_lineage[output_dataset].append(lineage_entry)
            
            # Track data dependencies
            self._update_data_dependencies(input_datasets, output_dataset)
            
            # Track schema evolution
            self._track_schema_changes(input_datasets, output_dataset, operation_name)
            
            return lineage_entry
        
        def _get_ray_cluster_info(self) -> Dict:
            """Get current Ray cluster information."""
            try:
                import ray
                if ray.is_initialized():
                    return {
                        'cluster_resources': ray.cluster_resources(),
                        'available_resources': ray.available_resources(),
                        'nodes': len(ray.nodes()),
                        'object_store_memory': ray.get_object_store_memory()
                    }
            except:
                pass
            return {}
        
        def _get_execution_context(self) -> Dict:
            """Get current execution context."""
            try:
                import ray
                from ray.data.context import DataContext
                
                ctx = DataContext.get_current()
                return {
                    'target_max_block_size': ctx.target_max_block_size,
                    'target_min_block_size': ctx.target_min_block_size,
                    'max_errored_blocks': ctx.max_errored_blocks,
                    'execution_options': {
                        'resource_limits': ctx.execution_options.resource_limits.__dict__ if hasattr(ctx.execution_options, 'resource_limits') else {},
                        'preserve_order': ctx.execution_options.preserve_order if hasattr(ctx.execution_options, 'preserve_order') else None
                    }
                }
            except:
                return {}
        
        def _update_data_dependencies(self, input_datasets: List[str], output_dataset: str):
            """Update data dependency graph."""
            
            if output_dataset not in self.data_dependencies:
                self.data_dependencies[output_dataset] = {
                    'direct_dependencies': input_datasets,
                    'transitive_dependencies': set(),
                    'dependency_depth': 0
                }
            
            # Calculate transitive dependencies
            all_dependencies = set(input_datasets)
            for input_dataset in input_datasets:
                if input_dataset in self.data_dependencies:
                    all_dependencies.update(self.data_dependencies[input_dataset]['transitive_dependencies'])
                    dependency_depth = self.data_dependencies[input_dataset]['dependency_depth'] + 1
                    self.data_dependencies[output_dataset]['dependency_depth'] = max(
                        self.data_dependencies[output_dataset]['dependency_depth'],
                        dependency_depth
                    )
            
            self.data_dependencies[output_dataset]['transitive_dependencies'] = all_dependencies
        
        def _track_schema_changes(self, input_datasets: List[str], output_dataset: str, operation_name: str):
            """Track schema evolution across operations."""
            
            schema_entry = {
                'operation_name': operation_name,
                'timestamp': datetime.now().isoformat(),
                'input_schemas': {},
                'output_schema': None,
                'schema_changes': []
            }
            
            # Record input schemas
            for input_dataset in input_datasets:
                if input_dataset in self.schema_evolution:
                    schema_entry['input_schemas'][input_dataset] = self.schema_evolution[input_dataset]
            
            # Store schema evolution
            self.schema_evolution[output_dataset] = schema_entry
        
        def generate_impact_analysis(self, dataset_name: str) -> Dict:
            """Generate impact analysis for a dataset."""
            
            if dataset_name not in self.data_dependencies:
                return {'error': f'No dependency data found for dataset {dataset_name}'}
            
            dependencies = self.data_dependencies[dataset_name]
            
            # Find downstream datasets that depend on this one
            downstream_datasets = []
            for dataset, deps in self.data_dependencies.items():
                if dataset_name in deps['transitive_dependencies']:
                    downstream_datasets.append(dataset)
            
            impact_analysis = {
                'dataset_name': dataset_name,
                'direct_dependencies': dependencies['direct_dependencies'],
                'transitive_dependencies': list(dependencies['transitive_dependencies']),
                'dependency_depth': dependencies['dependency_depth'],
                'downstream_datasets': downstream_datasets,
                'impact_score': self._calculate_impact_score(dataset_name, downstream_datasets),
                'risk_assessment': self._assess_change_risk(dataset_name)
            }
            
            return impact_analysis
        
        def _calculate_impact_score(self, dataset_name: str, downstream_datasets: List[str]) -> float:
            """Calculate impact score for dataset changes."""
            
            # Base score from number of downstream datasets
            base_score = min(len(downstream_datasets) * 0.1, 1.0)
            
            # Additional score from dependency depth
            depth_score = min(self.data_dependencies.get(dataset_name, {}).get('dependency_depth', 0) * 0.05, 0.5)
            
            # Critical dataset bonus
            critical_bonus = 0.2 if 'customer' in dataset_name.lower() or 'financial' in dataset_name.lower() else 0.0
            
            return min(base_score + depth_score + critical_bonus, 1.0)
        
        def _assess_change_risk(self, dataset_name: str) -> Dict:
            """Assess risk of changing a dataset."""
            
            risk_factors = []
            risk_level = 'low'
            
            # Factor 1: Number of downstream dependencies
            downstream_count = len([d for d, deps in self.data_dependencies.items() 
                                  if dataset_name in deps.get('transitive_dependencies', [])])
            if downstream_count > 10:
                risk_factors.append('High number of downstream dependencies')
                risk_level = 'high'
            elif downstream_count > 5:
                risk_factors.append('Moderate downstream dependencies')
                risk_level = 'medium'
            
            # Factor 2: Data sensitivity
            if any(sensitive in dataset_name.lower() for sensitive in ['customer', 'financial', 'patient', 'pii']):
                risk_factors.append('Contains sensitive data')
                risk_level = 'high'
            
            # Factor 3: Update frequency
            if dataset_name in self.operation_lineage:
                update_frequency = len(self.operation_lineage[dataset_name])
                if update_frequency > 100:
                    risk_factors.append('High update frequency')
                    risk_level = 'high'
                elif update_frequency > 50:
                    risk_factors.append('Moderate update frequency')
                    risk_level = 'medium'
            
            return {
                'risk_level': risk_level,
                'risk_factors': risk_factors,
                'recommendations': self._generate_risk_recommendations(risk_level, risk_factors)
            }
        
        def _generate_risk_recommendations(self, risk_level: str, risk_factors: List[str]) -> List[str]:
            """Generate recommendations based on risk assessment."""
            
            recommendations = []
            
            if risk_level == 'high':
                recommendations.extend([
                    'Perform comprehensive testing before deployment',
                    'Schedule change during maintenance window',
                    'Prepare rollback plan',
                    'Notify all downstream teams',
                    'Monitor closely after deployment'
                ])
            elif risk_level == 'medium':
                recommendations.extend([
                    'Test changes in staging environment',
                    'Coordinate with downstream teams',
                    'Monitor for issues after deployment'
                ])
            else:
                recommendations.extend([
                    'Standard testing procedures',
                    'Monitor for any unexpected issues'
                ])
            
            return recommendations
        
        def _generate_execution_id(self) -> str:
            """Generate unique execution ID."""
            return f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(datetime.now()) % 10000}"
        
        def _get_quality_history_for_dataset(self, dataset_name: str) -> List[Dict]:
            """Get data quality history for dataset."""
            # Implementation depends on your quality monitoring system
            return []
        
        def _check_compliance_status(self, dataset_name: str) -> Dict:
            """Check compliance status for dataset."""
            return {
                'gdpr_compliant': True,
                'hipaa_compliant': False,
                'sox_compliant': True,
                'last_audit_date': '2024-01-15',
                'compliance_notes': 'Dataset contains PII - ensure proper access controls'
            }

Governance Framework
-------------------

**Enterprise Governance Implementation**

.. code-block:: python

    class DataGovernanceFramework:
        """Comprehensive data governance framework."""
        
        def __init__(self):
            self.data_catalog = {}
            self.access_policies = {}
            self.classification_rules = {}
            
        def classify_data(self, batch: pd.DataFrame) -> Dict[str, str]:
            """Classify data sensitivity levels."""
            
            classification = {}
            
            for column in batch.columns:
                col_data = batch[column]
                
                # Check for PII data
                if self._contains_pii(column, col_data):
                    classification[column] = 'PII'
                # Check for financial data
                elif self._contains_financial_data(column, col_data):
                    classification[column] = 'Financial'
                # Check for health data
                elif self._contains_health_data(column, col_data):
                    classification[column] = 'Health'
                else:
                    classification[column] = 'General'
            
            return classification
        
        def apply_data_masking(self, batch: pd.DataFrame, classification: Dict[str, str]) -> pd.DataFrame:
            """Apply data masking based on classification."""
            
            masked_batch = batch.copy()
            
            for column, sensitivity in classification.items():
                if sensitivity == 'PII':
                    masked_batch[column] = self._mask_pii_data(masked_batch[column])
                elif sensitivity == 'Financial':
                    masked_batch[column] = self._mask_financial_data(masked_batch[column])
                elif sensitivity == 'Health':
                    masked_batch[column] = self._mask_health_data(masked_batch[column])
            
            return masked_batch
        
        def audit_data_access(self, user_id: str, dataset_name: str, operation: str):
            """Audit data access for compliance."""
            
            audit_entry = {
                'timestamp': datetime.now().isoformat(),
                'user_id': user_id,
                'dataset_name': dataset_name,
                'operation': operation,
                'ip_address': self._get_client_ip(),
                'session_id': self._get_session_id()
            }
            
            # Log to audit system
            self._log_audit_entry(audit_entry)
            
            # Check for suspicious activity
            if self._is_suspicious_access(audit_entry):
                self._trigger_security_alert(audit_entry)

**Comprehensive Compliance Frameworks**

Implement enterprise compliance frameworks for GDPR, HIPAA, SOX, and other regulations:

.. code-block:: python

    class ComplianceFramework:
        """Enterprise compliance framework for multiple regulations."""
        
        def __init__(self):
            self.compliance_rules = {}
            self.audit_logs = []
            self.compliance_reports = {}
            self.regulatory_requirements = {}
            
        def setup_gdpr_compliance(self):
            """Setup GDPR compliance requirements."""
            
            self.regulatory_requirements['gdpr'] = {
                'data_minimization': True,
                'purpose_limitation': True,
                'storage_limitation': True,
                'data_accuracy': True,
                'accountability': True,
                'data_subject_rights': True
            }
            
            # GDPR-specific validation rules
            def gdpr_data_minimization(batch: pd.DataFrame) -> Dict:
                """Ensure only necessary data is collected."""
                validation_result = {
                    'passed': True,
                    'violations': [],
                    'recommendations': []
                }
                
                # Check for unnecessary PII columns
                unnecessary_pii = ['ssn', 'passport', 'drivers_license']
                for col in batch.columns:
                    if any(pii_type in col.lower() for pii_type in unnecessary_pii):
                        validation_result['recommendations'].append({
                            'column': col,
                            'recommendation': 'Consider if this PII is necessary for business purpose'
                        })
                
                return validation_result
            
            def gdpr_purpose_limitation(batch: pd.DataFrame) -> Dict:
                """Ensure data is used only for specified purposes."""
                validation_result = {
                    'passed': True,
                    'violations': [],
                    'recommendations': []
                }
                
                # Check for purpose metadata
                if 'data_purpose' not in batch.columns:
                    validation_result['recommendations'].append({
                        'column': 'data_purpose',
                        'recommendation': 'Add data purpose column for GDPR compliance'
                    })
                
                return validation_result
            
            # Register GDPR rules
            self.compliance_rules['gdpr'] = {
                'data_minimization': gdpr_data_minimization,
                'purpose_limitation': gdpr_purpose_limitation
            }
        
        def setup_hipaa_compliance(self):
            """Setup HIPAA compliance requirements."""
            
            self.regulatory_requirements['hipaa'] = {
                'privacy_rule': True,
                'security_rule': True,
                'breach_notification': True,
                'minimum_necessary': True,
                'access_controls': True
            }
            
            # HIPAA-specific validation rules
            def hipaa_phi_identification(batch: pd.DataFrame) -> Dict:
                """Identify Protected Health Information (PHI)."""
                validation_result = {
                    'passed': True,
                    'phi_columns': [],
                    'recommendations': []
                }
                
                # PHI identifiers
                phi_indicators = [
                    'patient_id', 'medical_record', 'diagnosis', 'treatment',
                    'medication', 'lab_result', 'vital_sign', 'procedure'
                ]
                
                for col in batch.columns:
                    if any(phi in col.lower() for phi in phi_indicators):
                        validation_result['phi_columns'].append(col)
                        validation_result['recommendations'].append({
                            'column': col,
                            'recommendation': 'Ensure proper access controls and encryption'
                        })
                
                return validation_result
            
            def hipaa_access_control_validation(batch: pd.DataFrame) -> Dict:
                """Validate HIPAA access control requirements."""
                validation_result = {
                    'passed': True,
                    'violations': [],
                    'recommendations': []
                }
                
                # Check for access control metadata
                required_metadata = ['access_level', 'user_role', 'audit_trail']
                missing_metadata = [meta for meta in required_metadata if meta not in batch.columns]
                
                if missing_metadata:
                    validation_result['recommendations'].extend([
                        {'metadata': meta, 'recommendation': f'Add {meta} for HIPAA compliance'} 
                        for meta in missing_metadata
                    ])
                
                return validation_result
            
            # Register HIPAA rules
            self.compliance_rules['hipaa'] = {
                'phi_identification': hipaa_phi_identification,
                'access_control_validation': hipaa_access_control_validation
            }
        
        def setup_sox_compliance(self):
            """Setup SOX compliance requirements."""
            
            self.regulatory_requirements['sox'] = {
                'financial_controls': True,
                'audit_trails': True,
                'data_integrity': True,
                'access_controls': True,
                'change_management': True
            }
            
            # SOX-specific validation rules
            def sox_financial_data_validation(batch: pd.DataFrame) -> Dict:
                """Validate SOX financial data requirements."""
                validation_result = {
                    'passed': True,
                    'violations': [],
                    'recommendations': []
                }
                
                # Financial data columns
                financial_columns = ['amount', 'revenue', 'expense', 'balance', 'transaction']
                financial_data_found = [col for col in batch.columns 
                                      if any(fin in col.lower() for fin in financial_columns)]
                
                if financial_data_found:
                    # Check for audit trail
                    if 'audit_timestamp' not in batch.columns:
                        validation_result['recommendations'].append({
                            'metadata': 'audit_timestamp',
                            'recommendation': 'Add audit timestamp for SOX compliance'
                        })
                    
                    # Check for approval metadata
                    if 'approval_status' not in batch.columns:
                        validation_result['recommendations'].append({
                            'metadata': 'approval_status',
                            'recommendation': 'Add approval status for SOX compliance'
                        })
                
                return validation_result
            
            def sox_change_management_validation(batch: pd.DataFrame) -> Dict:
                """Validate SOX change management requirements."""
                validation_result = {
                    'passed': True,
                    'violations': [],
                    'recommendations': []
                }
                
                # Check for change management metadata
                required_metadata = ['change_request_id', 'approval_chain', 'implementation_date']
                missing_metadata = [meta for meta in required_metadata if meta not in batch.columns]
                
                if missing_metadata:
                    validation_result['recommendations'].extend([
                        {'metadata': meta, 'recommendation': f'Add {meta} for SOX compliance'} 
                        for meta in missing_metadata
                    ])
                
                return validation_result
            
            # Register SOX rules
            self.compliance_rules['sox'] = {
                'financial_data_validation': sox_financial_data_validation,
                'change_management_validation': sox_change_management_validation
            }
        
        def run_compliance_audit(self, batch: pd.DataFrame, regulations: List[str]) -> Dict:
            """Run comprehensive compliance audit."""
            
            audit_results = {
                'timestamp': datetime.now().isoformat(),
                'regulations': regulations,
                'overall_compliance': True,
                'regulation_results': {},
                'violations': [],
                'recommendations': []
            }
            
            for regulation in regulations:
                if regulation in self.compliance_rules:
                    regulation_results = {}
                    regulation_violations = []
                    regulation_recommendations = []
                    
                    # Run all rules for this regulation
                    for rule_name, rule_func in self.compliance_rules[regulation].items():
                        try:
                            rule_result = rule_func(batch)
                            regulation_results[rule_name] = rule_result
                            
                            if not rule_result.get('passed', True):
                                regulation_violations.append(rule_name)
                                audit_results['overall_compliance'] = False
                            
                            if 'recommendations' in rule_result:
                                regulation_recommendations.extend(rule_result['recommendations'])
                            
                        except Exception as e:
                            regulation_results[rule_name] = {
                                'error': str(e),
                                'passed': False
                            }
                            regulation_violations.append(f"{rule_name}_error")
                            audit_results['overall_compliance'] = False
                    
                    audit_results['regulation_results'][regulation] = {
                        'results': regulation_results,
                        'violations': regulation_violations,
                        'recommendations': regulation_recommendations,
                        'compliance_status': len(regulation_violations) == 0
                    }
                    
                    # Aggregate violations and recommendations
                    audit_results['violations'].extend(regulation_violations)
                    audit_results['recommendations'].extend(regulation_recommendations)
            
            # Store audit results
            self.audit_logs.append(audit_results)
            
            return audit_results
        
        def generate_compliance_report(self, regulation: str, time_period: str = 'monthly') -> Dict:
            """Generate compliance report for specific regulation."""
            
            # Filter audit logs for regulation and time period
            cutoff_date = self._get_cutoff_date(time_period)
            relevant_audits = [
                audit for audit in self.audit_logs
                if regulation in audit['regulation_results']
                and datetime.fromisoformat(audit['timestamp']) > cutoff_date
            ]
            
            if not relevant_audits:
                return {'error': f'No audit data found for {regulation} in {time_period} period'}
            
            # Calculate compliance metrics
            total_audits = len(relevant_audits)
            compliant_audits = sum(1 for audit in relevant_audits 
                                 if audit['regulation_results'][regulation]['compliance_status'])
            compliance_rate = compliant_audits / total_audits if total_audits > 0 else 0
            
            # Identify common violations and recommendations
            all_violations = []
            all_recommendations = []
            
            for audit in relevant_audits:
                reg_results = audit['regulation_results'][regulation]
                all_violations.extend(reg_results['violations'])
                all_recommendations.extend(reg_results['recommendations'])
            
            # Count violations and recommendations
            violation_counts = {}
            for violation in all_violations:
                violation_counts[violation] = violation_counts.get(violation, 0) + 1
            
            recommendation_counts = {}
            for recommendation in all_recommendations:
                rec_key = recommendation.get('recommendation', 'Unknown')
                recommendation_counts[rec_key] = recommendation_counts.get(rec_key, 0) + 1
            
            return {
                'regulation': regulation,
                'time_period': time_period,
                'total_audits': total_audits,
                'compliance_rate': compliance_rate,
                'common_violations': violation_counts,
                'common_recommendations': recommendation_counts,
                'trend_analysis': self._analyze_compliance_trends(relevant_audits, regulation),
                'generated_at': datetime.now().isoformat()
            }
        
        def _get_cutoff_date(self, time_period: str) -> datetime:
            """Get cutoff date for time period."""
            
            now = datetime.now()
            if time_period == 'daily':
                return now - timedelta(days=1)
            elif time_period == 'weekly':
                return now - timedelta(weeks=1)
            elif time_period == 'monthly':
                return now - timedelta(days=30)
            elif time_period == 'quarterly':
                return now - timedelta(days=90)
            elif time_period == 'yearly':
                return now - timedelta(days=365)
            else:
                return now - timedelta(days=30)  # Default to monthly
        
        def _analyze_compliance_trends(self, audits: List[Dict], regulation: str) -> Dict:
            """Analyze compliance trends over time."""
            
            if len(audits) < 2:
                return {'trend': 'insufficient_data', 'message': 'Need at least 2 audits for trend analysis'}
            
            # Sort audits by timestamp
            sorted_audits = sorted(audits, key=lambda x: x['timestamp'])
            
            # Calculate trend
            first_compliance = sorted_audits[0]['regulation_results'][regulation]['compliance_status']
            last_compliance = sorted_audits[-1]['regulation_results'][regulation]['compliance_status']
            
            if first_compliance and last_compliance:
                trend = 'stable_compliant'
            elif not first_compliance and last_compliance:
                trend = 'improving'
            elif first_compliance and not last_compliance:
                trend = 'declining'
            else:
                trend = 'stable_non_compliant'
            
            return {
                'trend': trend,
                'first_audit_date': sorted_audits[0]['timestamp'],
                'last_audit_date': sorted_audits[-1]['timestamp'],
                'total_audits_analyzed': len(sorted_audits)
            }
        
        def _contains_pii(self, column_name: str, column_data: pd.Series) -> bool:
            """Check if column contains PII data."""
            pii_indicators = ['email', 'phone', 'ssn', 'name', 'address']
            return any(indicator in column_name.lower() for indicator in pii_indicators)

**Comprehensive Audit Systems**

Implement enterprise-grade audit logging and reporting systems:

.. code-block:: python

    class DataAuditSystem:
        """Comprehensive data audit system for compliance and security."""
        
        def __init__(self):
            self.audit_logs = []
            self.audit_policies = {}
            self.retention_policies = {}
            self.alert_thresholds = {}
            
        def setup_audit_policies(self):
            """Setup comprehensive audit policies."""
            
            self.audit_policies = {
                'data_access': {
                    'enabled': True,
                    'log_level': 'detailed',
                    'retention_days': 2555,  # 7 years for SOX
                    'alert_on': ['unauthorized_access', 'bulk_download', 'sensitive_data_access']
                },
                'data_modification': {
                    'enabled': True,
                    'log_level': 'detailed',
                    'retention_days': 2555,
                    'alert_on': ['schema_changes', 'data_deletion', 'bulk_updates']
                },
                'data_quality': {
                    'enabled': True,
                    'log_level': 'summary',
                    'retention_days': 1095,  # 3 years
                    'alert_on': ['quality_threshold_breach', 'validation_failures']
                },
                'compliance': {
                    'enabled': True,
                    'log_level': 'detailed',
                    'retention_days': 3650,  # 10 years for long-term compliance
                    'alert_on': ['compliance_violation', 'audit_failure']
                }
            }
            
            # Setup retention policies
            self.retention_policies = {
                'short_term': 90,      # 3 months
                'medium_term': 1095,   # 3 years
                'long_term': 3650,     # 10 years
                'permanent': -1        # Never delete
            }
            
            # Setup alert thresholds
            self.alert_thresholds = {
                'failed_logins': 5,           # Alert after 5 failed logins
                'data_access_frequency': 100, # Alert if user accesses >100 records in 1 hour
                'quality_degradation': 0.1,   # Alert if quality drops by 10%
                'unauthorized_attempts': 3     # Alert after 3 unauthorized attempts
            }
        
        def log_data_access(self, user_id: str, dataset_name: str, operation: str, 
                           record_count: int = 0, columns_accessed: List[str] = None,
                           ip_address: str = None, session_id: str = None):
            """Log comprehensive data access events."""
            
            audit_entry = {
                'event_type': 'data_access',
                'timestamp': datetime.now().isoformat(),
                'user_id': user_id,
                'dataset_name': dataset_name,
                'operation': operation,
                'record_count': record_count,
                'columns_accessed': columns_accessed or [],
                'ip_address': ip_address or self._get_client_ip(),
                'session_id': session_id or self._get_session_id(),
                'access_context': self._get_access_context(),
                'risk_level': self._assess_access_risk(user_id, dataset_name, operation, record_count)
            }
            
            # Add to audit logs
            self.audit_logs.append(audit_entry)
            
            # Check for alert conditions
            self._check_access_alerts(audit_entry)
            
            # Apply retention policies
            self._apply_retention_policies()
            
            return audit_entry
        
        def log_data_modification(self, user_id: str, dataset_name: str, operation: str,
                                 modification_type: str, record_count: int = 0,
                                 before_state: Dict = None, after_state: Dict = None):
            """Log data modification events with before/after states."""
            
            audit_entry = {
                'event_type': 'data_modification',
                'timestamp': datetime.now().isoformat(),
                'user_id': user_id,
                'dataset_name': dataset_name,
                'operation': operation,
                'modification_type': modification_type,
                'record_count': record_count,
                'before_state': before_state,
                'after_state': after_state,
                'change_summary': self._generate_change_summary(before_state, after_state),
                'ip_address': self._get_client_ip(),
                'session_id': self._get_session_id(),
                'approval_status': self._get_approval_status(user_id, operation),
                'risk_level': self._assess_modification_risk(dataset_name, modification_type, record_count)
            }
            
            # Add to audit logs
            self.audit_logs.append(audit_entry)
            
            # Check for alert conditions
            self._check_modification_alerts(audit_entry)
            
            return audit_entry
        
        def log_data_quality_event(self, dataset_name: str, quality_metrics: Dict,
                                  validation_results: Dict, threshold_breaches: List[str]):
            """Log data quality events and validation results."""
            
            audit_entry = {
                'event_type': 'data_quality',
                'timestamp': datetime.now().isoformat(),
                'dataset_name': dataset_name,
                'quality_metrics': quality_metrics,
                'validation_results': validation_results,
                'threshold_breaches': threshold_breaches,
                'overall_quality_score': self._calculate_overall_quality_score(quality_metrics),
                'trend_analysis': self._analyze_quality_trends(dataset_name),
                'recommendations': self._generate_quality_recommendations(validation_results)
            }
            
            # Add to audit logs
            self.audit_logs.append(audit_entry)
            
            # Check for alert conditions
            self._check_quality_alerts(audit_entry)
            
            return audit_entry
        
        def log_compliance_event(self, regulation: str, compliance_status: str,
                                violations: List[str], recommendations: List[str],
                                audit_date: str = None):
            """Log compliance audit events."""
            
            audit_entry = {
                'event_type': 'compliance',
                'timestamp': datetime.now().isoformat(),
                'regulation': regulation,
                'compliance_status': compliance_status,
                'violations': violations,
                'recommendations': recommendations,
                'audit_date': audit_date or datetime.now().isoformat(),
                'compliance_score': self._calculate_compliance_score(regulation, violations),
                'risk_assessment': self._assess_compliance_risk(regulation, violations),
                'next_audit_date': self._calculate_next_audit_date(regulation, compliance_status)
            }
            
            # Add to audit logs
            self.audit_logs.append(audit_entry)
            
            # Check for alert conditions
            self._check_compliance_alerts(audit_entry)
            
            return audit_entry
        
        def generate_audit_report(self, report_type: str = 'comprehensive', 
                                filters: Dict = None, time_range: str = 'monthly') -> Dict:
            """Generate comprehensive audit reports."""
            
            # Apply filters
            filtered_logs = self._apply_audit_filters(self.audit_logs, filters or {})
            
            # Generate report based on type
            if report_type == 'comprehensive':
                return self._generate_comprehensive_report(filtered_logs, time_range)
            elif report_type == 'compliance':
                return self._generate_compliance_report(filtered_logs, time_range)
            elif report_type == 'security':
                return self._generate_security_report(filtered_logs, time_range)
            elif report_type == 'data_quality':
                return self._generate_quality_report(filtered_logs, time_range)
            else:
                return {'error': f'Unknown report type: {report_type}'}
        
        def _generate_comprehensive_report(self, logs: List[Dict], time_range: str) -> Dict:
            """Generate comprehensive audit report."""
            
            # Calculate time-based metrics
            time_metrics = self._calculate_time_based_metrics(logs, time_range)
            
            # Calculate user activity metrics
            user_metrics = self._calculate_user_activity_metrics(logs)
            
            # Calculate dataset access metrics
            dataset_metrics = self._calculate_dataset_access_metrics(logs)
            
            # Calculate risk metrics
            risk_metrics = self._calculate_risk_metrics(logs)
            
            return {
                'report_type': 'comprehensive',
                'time_range': time_range,
                'generated_at': datetime.now().isoformat(),
                'summary': {
                    'total_events': len(logs),
                    'unique_users': len(user_metrics),
                    'unique_datasets': len(dataset_metrics),
                    'high_risk_events': risk_metrics['high_risk_count']
                },
                'time_metrics': time_metrics,
                'user_metrics': user_metrics,
                'dataset_metrics': dataset_metrics,
                'risk_metrics': risk_metrics,
                'recommendations': self._generate_audit_recommendations(logs)
            }
        
        def _calculate_time_based_metrics(self, logs: List[Dict], time_range: str) -> Dict:
            """Calculate time-based audit metrics."""
            
            # Group logs by time period
            if time_range == 'daily':
                group_key = lambda x: x['timestamp'][:10]  # YYYY-MM-DD
            elif time_range == 'weekly':
                group_key = lambda x: f"{x['timestamp'][:4]}-W{datetime.fromisoformat(x['timestamp']).isocalendar()[1]}"
            elif time_range == 'monthly':
                group_key = lambda x: x['timestamp'][:7]  # YYYY-MM
            else:
                group_key = lambda x: x['timestamp'][:4]  # YYYY
            
            grouped_logs = {}
            for log in logs:
                key = group_key(log)
                if key not in grouped_logs:
                    grouped_logs[key] = []
                grouped_logs[key].append(log)
            
            # Calculate metrics for each time period
            time_metrics = {}
            for period, period_logs in grouped_logs.items():
                time_metrics[period] = {
                    'total_events': len(period_logs),
                    'event_types': self._count_event_types(period_logs),
                    'unique_users': len(set(log['user_id'] for log in period_logs if 'user_id' in log)),
                    'unique_datasets': len(set(log['dataset_name'] for log in period_logs if 'dataset_name' in log)),
                    'risk_distribution': self._calculate_risk_distribution(period_logs)
                }
            
            return time_metrics
        
        def _count_event_types(self, logs: List[Dict]) -> Dict[str, int]:
            """Count events by type."""
            event_counts = {}
            for log in logs:
                event_type = log.get('event_type', 'unknown')
                event_counts[event_type] = event_counts.get(event_type, 0) + 1
            return event_counts
        
        def _calculate_risk_distribution(self, logs: List[Dict]) -> Dict[str, int]:
            """Calculate risk distribution for logs."""
            risk_counts = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
            for log in logs:
                risk_level = log.get('risk_level', 'low')
                risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1
            return risk_counts
        
        def _generate_audit_recommendations(self, logs: List[Dict]) -> List[Dict]:
            """Generate audit recommendations based on patterns."""
            
            recommendations = []
            
            # Check for unusual access patterns
            user_access_counts = {}
            for log in logs:
                if log.get('event_type') == 'data_access':
                    user_id = log.get('user_id', 'unknown')
                    user_access_counts[user_id] = user_access_counts.get(user_id, 0) + 1
            
            # Flag users with unusually high access
            avg_access = sum(user_access_counts.values()) / len(user_access_counts) if user_access_counts else 0
            for user_id, access_count in user_access_counts.items():
                if access_count > avg_access * 3:  # 3x above average
                    recommendations.append({
                        'type': 'access_pattern',
                        'priority': 'medium',
                        'message': f'User {user_id} has unusually high access frequency',
                        'details': f'Access count: {access_count}, Average: {avg_access:.1f}'
                    })
            
            # Check for quality degradation trends
            quality_logs = [log for log in logs if log.get('event_type') == 'data_quality']
            if len(quality_logs) > 1:
                recent_quality = quality_logs[-1].get('overall_quality_score', 0)
                if recent_quality < 0.8:  # Quality below 80%
                    recommendations.append({
                        'type': 'data_quality',
                        'priority': 'high',
                        'message': 'Data quality is below acceptable threshold',
                        'details': f'Current quality score: {recent_quality:.2%}'
                    })
            
            return recommendations
        
        def _contains_financial_data(self, column_name: str, column_data: pd.Series) -> bool:
            """Check if column contains financial data."""
            financial_indicators = ['salary', 'income', 'revenue', 'amount', 'price']
            return any(indicator in column_name.lower() for indicator in financial_indicators)

**Governance Automation and Policy Enforcement**

Implement automated governance and policy enforcement systems:

.. code-block:: python

    class AutomatedGovernanceSystem:
        """Automated governance and policy enforcement system."""
        
        def __init__(self):
            self.policies = {}
            self.enforcement_rules = {}
            self.automated_actions = {}
            self.policy_violations = []
            self.remediation_actions = []
            
        def setup_governance_policies(self):
            """Setup comprehensive governance policies."""
            
            self.policies = {
                'data_classification': {
                    'auto_classify': True,
                    'sensitivity_levels': ['public', 'internal', 'confidential', 'restricted'],
                    'classification_rules': {
                        'pii': 'restricted',
                        'financial': 'confidential',
                        'health': 'restricted',
                        'customer': 'confidential',
                        'employee': 'confidential'
                    }
                },
                'access_control': {
                    'auto_enforce': True,
                    'role_based_access': True,
                    'least_privilege': True,
                    'access_reviews': 'quarterly',
                    'auto_revocation': True
                },
                'data_retention': {
                    'auto_enforce': True,
                    'retention_schedules': {
                        'pii': '7_years',
                        'financial': '7_years',
                        'health': '10_years',
                        'general': '3_years'
                    },
                    'auto_archival': True,
                    'auto_deletion': True
                },
                'data_quality': {
                    'auto_monitor': True,
                    'quality_thresholds': {
                        'completeness': 0.95,
                        'accuracy': 0.90,
                        'consistency': 0.85
                    },
                    'auto_remediation': True,
                    'quality_gates': True
                }
            }
            
            # Setup enforcement rules
            self.enforcement_rules = {
                'data_access': {
                    'unauthorized_access': 'block_and_alert',
                    'bulk_download': 'require_approval',
                    'sensitive_data_access': 'log_and_monitor'
                },
                'data_modification': {
                    'schema_changes': 'require_approval',
                    'bulk_updates': 'require_approval',
                    'data_deletion': 'require_approval'
                },
                'data_quality': {
                    'threshold_breach': 'auto_remediation',
                    'validation_failure': 'block_and_alert',
                    'quality_degradation': 'auto_notification'
                }
            }
            
            # Setup automated actions
            self.automated_actions = {
                'data_classification': self._auto_classify_data,
                'access_control': self._auto_enforce_access_controls,
                'retention_management': self._auto_manage_retention,
                'quality_remediation': self._auto_remediate_quality_issues
            }
        
        def auto_classify_data(self, batch: pd.DataFrame, metadata: Dict = None) -> Dict:
            """Automatically classify data based on content and metadata."""
            
            classification_result = {
                'timestamp': datetime.now().isoformat(),
                'dataset_name': metadata.get('dataset_name', 'unknown'),
                'classification_rules_applied': [],
                'sensitivity_level': 'internal',  # Default
                'classification_confidence': 0.0,
                'recommendations': []
            }
            
            # Apply classification rules
            applied_rules = []
            sensitivity_scores = {'public': 0, 'internal': 0, 'confidential': 0, 'restricted': 0}
            
            for column in batch.columns:
                column_classification = self._classify_column(column, batch[column])
                applied_rules.append(column_classification)
                
                # Aggregate sensitivity scores
                sensitivity_level = column_classification['sensitivity_level']
                sensitivity_scores[sensitivity_level] += column_classification['confidence']
            
            # Determine overall sensitivity level
            total_score = sum(sensitivity_scores.values())
            if total_score > 0:
                max_sensitivity = max(sensitivity_scores, key=sensitivity_scores.get)
                classification_result['sensitivity_level'] = max_sensitivity
                classification_result['classification_confidence'] = sensitivity_scores[max_sensitivity] / total_score
            
            classification_result['classification_rules_applied'] = applied_rules
            
            # Apply automated actions based on classification
            self._apply_classification_actions(classification_result, batch)
            
            return classification_result
        
        def _classify_column(self, column_name: str, column_data: pd.Series) -> Dict:
            """Classify individual column based on name and content."""
            
            classification = {
                'column_name': column_name,
                'sensitivity_level': 'internal',
                'confidence': 0.5,
                'indicators': [],
                'recommendations': []
            }
            
            # Check column name patterns
            column_lower = column_name.lower()
            
            # PII indicators
            pii_patterns = ['ssn', 'passport', 'drivers_license', 'credit_card', 'phone', 'email']
            if any(pattern in column_lower for pattern in pii_patterns):
                classification['sensitivity_level'] = 'restricted'
                classification['confidence'] = 0.9
                classification['indicators'].append('pii_pattern_match')
                classification['recommendations'].append('Apply strict access controls and encryption')
            
            # Financial indicators
            financial_patterns = ['salary', 'income', 'revenue', 'balance', 'account']
            elif any(pattern in column_lower for pattern in financial_patterns):
                classification['sensitivity_level'] = 'confidential'
                classification['confidence'] = 0.8
                classification['indicators'].append('financial_pattern_match')
                classification['recommendations'].append('Apply financial data controls')
            
            # Health indicators
            health_patterns = ['medical', 'diagnosis', 'treatment', 'patient', 'health']
            elif any(pattern in column_lower for pattern in health_patterns):
                classification['sensitivity_level'] = 'restricted'
                classification['confidence'] = 0.9
                classification['indicators'].append('health_pattern_match')
                classification['recommendations'].append('Apply HIPAA compliance controls')
            
            # Customer indicators
            customer_patterns = ['customer', 'client', 'user', 'account']
            elif any(pattern in column_lower for pattern in customer_patterns):
                classification['sensitivity_level'] = 'confidential'
                classification['confidence'] = 0.7
                classification['indicators'].append('customer_pattern_match')
                classification['recommendations'].append('Apply customer data protection')
            
            return classification
        
        def auto_enforce_access_controls(self, user_id: str, dataset_name: str, 
                                       operation: str, data_classification: str) -> Dict:
            """Automatically enforce access controls based on policies."""
            
            enforcement_result = {
                'timestamp': datetime.now().isoformat(),
                'user_id': user_id,
                'dataset_name': dataset_name,
                'operation': operation,
                'data_classification': data_classification,
                'access_granted': False,
                'enforcement_actions': [],
                'approval_required': False,
                'reason': ''
            }
            
            # Check user permissions
            user_permissions = self._get_user_permissions(user_id)
            dataset_permissions = self._get_dataset_permissions(dataset_name)
            
            # Apply least privilege principle
            if not self._has_required_permissions(user_permissions, dataset_permissions, operation):
                enforcement_result['reason'] = 'Insufficient permissions'
                enforcement_result['enforcement_actions'].append('access_denied')
                return enforcement_result
            
            # Check sensitivity-based restrictions
            if data_classification == 'restricted':
                if not self._has_restricted_access_permissions(user_permissions):
                    enforcement_result['reason'] = 'Restricted data access requires special permissions'
                    enforcement_result['approval_required'] = True
                    enforcement_result['enforcement_actions'].append('approval_required')
                    return enforcement_result
            
            # Check operation-based restrictions
            if operation in ['delete', 'bulk_update', 'schema_change']:
                if not self._has_admin_permissions(user_permissions):
                    enforcement_result['reason'] = 'Administrative operation requires admin permissions'
                    enforcement_result['approval_required'] = True
                    enforcement_result['enforcement_actions'].append('approval_required')
                    return enforcement_result
            
            # Access granted
            enforcement_result['access_granted'] = True
            enforcement_result['reason'] = 'Access granted based on policies'
            enforcement_result['enforcement_actions'].append('access_granted')
            
            # Log access for audit
            self._log_access_event(enforcement_result)
            
            return enforcement_result
        
        def auto_manage_retention(self, dataset_name: str, data_classification: str, 
                                 last_accessed: str, retention_policy: str) -> Dict:
            """Automatically manage data retention based on policies."""
            
            retention_result = {
                'timestamp': datetime.now().isoformat(),
                'dataset_name': dataset_name,
                'data_classification': data_classification,
                'retention_policy': retention_policy,
                'actions_taken': [],
                'next_review_date': None
            }
            
            # Calculate retention period
            retention_periods = {
                'pii': 2555,           # 7 years
                'financial': 2555,     # 7 years
                'health': 3650,        # 10 years
                'general': 1095        # 3 years
            }
            
            retention_days = retention_periods.get(data_classification, 1095)
            
            # Check if data should be archived or deleted
            last_accessed_date = datetime.fromisoformat(last_accessed)
            days_since_access = (datetime.now() - last_accessed_date).days
            
            if days_since_access > retention_days:
                # Data should be deleted
                retention_result['actions_taken'].append('scheduled_for_deletion')
                retention_result['next_review_date'] = (datetime.now() + timedelta(days=30)).isoformat()
                
                # Schedule deletion
                self._schedule_data_deletion(dataset_name, 'retention_policy')
                
            elif days_since_access > retention_days * 0.8:
                # Data approaching retention limit
                retention_result['actions_taken'].append('retention_warning')
                retention_result['next_review_date'] = (datetime.now() + timedelta(days=7)).isoformat()
                
                # Send warning notification
                self._send_retention_warning(dataset_name, data_classification, retention_days - days_since_access)
            
            return retention_result
        
        def auto_remediate_quality_issues(self, dataset_name: str, quality_metrics: Dict, 
                                        validation_results: Dict) -> Dict:
            """Automatically remediate data quality issues."""
            
            remediation_result = {
                'timestamp': datetime.now().isoformat(),
                'dataset_name': dataset_name,
                'issues_detected': [],
                'remediation_actions': [],
                'success_rate': 0.0,
                'manual_intervention_required': False
            }
            
            # Check completeness issues
            if quality_metrics.get('completeness_rate', 1.0) < 0.95:
                remediation_result['issues_detected'].append('low_completeness')
                
                # Auto-fill missing values where possible
                if self._can_auto_fill_missing_values(dataset_name):
                    remediation_result['remediation_actions'].append('auto_fill_missing_values')
                    self._execute_auto_fill(dataset_name)
                else:
                    remediation_result['manual_intervention_required'] = True
            
            # Check accuracy issues
            if quality_metrics.get('accuracy_rate', 1.0) < 0.90:
                remediation_result['issues_detected'].append('low_accuracy')
                remediation_result['manual_intervention_required'] = True
                remediation_result['remediation_actions'].append('flag_for_review')
            
            # Check consistency issues
            if quality_metrics.get('consistency_rate', 1.0) < 0.85:
                remediation_result['issues_detected'].append('low_consistency')
                
                # Auto-standardize formats where possible
                if self._can_standardize_formats(dataset_name):
                    remediation_result['remediation_actions'].append('standardize_formats')
                    self._execute_format_standardization(dataset_name)
                else:
                    remediation_result['manual_intervention_required'] = True
            
            # Calculate success rate
            total_issues = len(remediation_result['issues_detected'])
            auto_resolved = len([action for action in remediation_result['remediation_actions'] 
                               if not action.startswith('flag_')])
            
            if total_issues > 0:
                remediation_result['success_rate'] = auto_resolved / total_issues
            
            return remediation_result
        
        def _apply_classification_actions(self, classification_result: Dict, batch: pd.DataFrame):
            """Apply automated actions based on data classification."""
            
            sensitivity_level = classification_result['sensitivity_level']
            
            if sensitivity_level == 'restricted':
                # Apply strict controls
                self._apply_restricted_controls(classification_result['dataset_name'])
                
            elif sensitivity_level == 'confidential':
                # Apply confidential controls
                self._apply_confidential_controls(classification_result['dataset_name'])
                
            elif sensitivity_level == 'internal':
                # Apply internal controls
                self._apply_internal_controls(classification_result['dataset_name'])
                
            # Log classification for audit
            self._log_classification_event(classification_result)
        
        def _has_required_permissions(self, user_permissions: Dict, dataset_permissions: Dict, 
                                     operation: str) -> bool:
            """Check if user has required permissions for operation."""
            
            # Implementation depends on your permission system
            required_permissions = {
                'read': ['read'],
                'write': ['read', 'write'],
                'delete': ['read', 'write', 'delete'],
                'admin': ['read', 'write', 'delete', 'admin']
            }
            
            operation_permissions = required_permissions.get(operation, [])
            
            for permission in operation_permissions:
                if not (permission in user_permissions and permission in dataset_permissions):
                    return False
            
            return True
        
        def _schedule_data_deletion(self, dataset_name: str, reason: str):
            """Schedule data deletion for retention management."""
            
            # Implementation depends on your scheduling system
            deletion_task = {
                'dataset_name': dataset_name,
                'reason': reason,
                'scheduled_date': (datetime.now() + timedelta(days=7)).isoformat(),
                'status': 'scheduled'
            }
            
            # Add to deletion queue
            # self.deletion_queue.append(deletion_task)
            print(f"Scheduled deletion of {dataset_name} for {deletion_task['scheduled_date']}")
        
        def _send_retention_warning(self, dataset_name: str, data_classification: str, days_remaining: int):
            """Send retention warning notification."""
            
            warning_message = {
                'type': 'retention_warning',
                'dataset_name': dataset_name,
                'data_classification': data_classification,
                'days_remaining': days_remaining,
                'timestamp': datetime.now().isoformat(),
                'action_required': 'Review data retention requirements'
            }
            
            # Send notification
            # self.notification_system.send(warning_message)
            print(f"Retention warning sent for {dataset_name}: {days_remaining} days remaining")
        
        def _contains_health_data(self, column_name: str, column_data: pd.Series) -> bool:
            """Check if column contains health data."""
            health_indicators = ['medical', 'diagnosis', 'treatment', 'patient']
            return any(indicator in column_name.lower() for indicator in health_indicators)
        
        def _mask_pii_data(self, data: pd.Series) -> pd.Series:
            """Mask PII data."""
            return data.apply(lambda x: '***MASKED***' if pd.notna(x) else x)
        
        def _mask_financial_data(self, data: pd.Series) -> pd.Series:
            """Mask financial data."""
            return data.apply(lambda x: 0.0 if pd.notna(x) else x)
        
        def _mask_health_data(self, data: pd.Series) -> pd.Series:
            """Mask health data."""
            return data.apply(lambda x: '***CONFIDENTIAL***' if pd.notna(x) else x)

Best Practices Summary
---------------------

**1. Implement Multi-Layer Validation**

* Schema validation at ingestion
* Business rule validation during processing
* Data quality monitoring throughout pipeline
* Compliance checks before output

**2. Establish Quality Thresholds**

* Define acceptable quality levels for each metric
* Set up automated alerting for threshold breaches
* Implement escalation procedures for critical issues
* Regular review and adjustment of thresholds

**3. Maintain Comprehensive Lineage**

* Track all data transformations and dependencies
* Document business logic and transformation rules
* Maintain audit trails for compliance requirements
* Enable impact analysis for changes

**4. Implement Governance Controls**

* Classify data by sensitivity level
* Apply appropriate access controls and masking
* Maintain audit logs for all data access
* Regular compliance reviews and assessments

**5. Monitor and Alert Proactively**

* Continuous quality monitoring during execution
* Automated alerting for quality degradation
* Integration with existing monitoring systems
* Regular quality reporting and trend analysis

Automated Quality Monitoring Patterns
-------------------------------------

**Continuous Quality Assessment Framework**

Implement automated data quality monitoring that continuously assesses data across multiple dimensions and provides actionable insights for data stewardship teams.

.. code-block:: python

    def comprehensive_quality_monitoring():
        """Monitor data quality across all processing stages."""
        
        def assess_quality_dimensions(batch):
            """Assess multiple quality dimensions."""
            quality_report = {}
            
            # Completeness assessment
            total_rows = len(batch)
            for column in batch.columns:
                null_count = batch[column].isnull().sum()
                completeness_ratio = (total_rows - null_count) / total_rows
                quality_report[f"{column}_completeness"] = completeness_ratio
            
            # Validity assessment
            quality_report['validity_score'] = validate_business_rules(batch)
            
            # Overall quality score
            quality_report['overall_quality'] = calculate_overall_quality(quality_report)
            
            return batch.assign(**quality_report)
        
        # Apply quality monitoring across data pipeline
        monitored_data = ray.data.read_parquet("s3://data-pipeline/") \
            .map_batches(assess_quality_dimensions)
        
        return monitored_data

**Quality Alerting and Response Framework**

.. code-block:: python

    def implement_quality_alerting():
        """Implement automated quality alerting system."""
        
        def quality_threshold_monitoring(batch):
            """Monitor quality against business thresholds."""
            quality_metrics = calculate_quality_metrics(batch)
            
            # Check quality thresholds
            if quality_metrics['overall_quality'] < 0.95:
                send_quality_alert({
                    'severity': 'high',
                    'message': f"Data quality below threshold: {quality_metrics['overall_quality']}",
                    'timestamp': datetime.now()
                })
            
            return batch
        
        return quality_threshold_monitoring

Next Steps
----------

* **Monitoring & Observability**: Set up comprehensive monitoring → :ref:`monitoring-observability`
* **Troubleshooting**: Learn to diagnose and resolve issues → :ref:`troubleshooting`
* **Production Deployment**: Deploy with proper governance → :ref:`production-deployment`
