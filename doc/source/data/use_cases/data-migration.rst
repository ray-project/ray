.. _data-migration:

Data Migration: Legacy System Modernization with Ray Data
=========================================================

**Keywords:** data migration, legacy system modernization, database migration, cloud migration, data transformation, ETL migration, system integration, data warehouse migration, platform migration

**Navigation:** :ref:`Ray Data <data>` → :ref:`Use Cases <use_cases>` → Data Migration

This use case demonstrates how to use Ray Data for large-scale data migration projects, including legacy system modernization, cloud migration, and platform consolidation with data transformation and validation.

**What you'll build:**

* Legacy database to cloud data warehouse migration
* Multi-system data consolidation pipeline
* Data format transformation and standardization
* Migration validation and quality assurance

Data Migration Scenarios
------------------------

**Common Migration Patterns**

:::list-table
   :header-rows: 1

- - **Migration Type**
  - **Source Systems**
  - **Target Systems**
  - **Ray Data Benefits**
- - Legacy to Cloud
  - On-premise databases, mainframes
  - Cloud data warehouses, lakes
  - Distributed processing, format flexibility
- - System Consolidation
  - Multiple databases, files
  - Unified data platform
  - Multi-source integration, transformation
- - Platform Modernization
  - Legacy ETL tools, batch systems
  - Modern data platforms
  - Python-native processing, GPU acceleration
- - Format Migration
  - Proprietary formats, legacy files
  - Modern formats (Parquet, Delta)
  - Format conversion, optimization

:::

Use Case 1: Legacy Database to Cloud Migration
-----------------------------------------------

**Business Scenario:** Migrate customer and transaction data from legacy on-premise databases to modern cloud data warehouse with data transformation and validation.

.. code-block:: python

    import ray
    import pandas as pd
    from datetime import datetime, timedelta

    def legacy_to_cloud_migration():
        """Migrate legacy database to cloud data warehouse."""
        
        # Define migration configuration
        migration_config = {
            "source_db": "oracle://legacy-system:1521/PROD",
            "target_warehouse": "snowflake://account/MODERN_DW/PUBLIC",
            "batch_size": 100000,
            "validation_sample_rate": 0.01
        }
        
        def extract_legacy_customer_data(batch_id):
            """Extract customer data from legacy system in batches."""
            # Read from legacy database with pagination
            offset = batch_id * migration_config["batch_size"]
            limit = migration_config["batch_size"]
            
            legacy_customers = ray.data.read_sql(
                migration_config["source_db"],
                f"""
                SELECT customer_id, first_name, last_name, email, phone,
                       address_line1, address_line2, city, state, zip_code,
                       registration_date, last_login_date, customer_status
                FROM customers 
                ORDER BY customer_id
                OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                """
            )
            
            return legacy_customers
        
        def transform_legacy_format(batch):
            """Transform legacy data format to modern schema."""
            # Standardize column names
            column_mapping = {
                "customer_id": "id",
                "first_name": "first_name",
                "last_name": "last_name",
                "email": "email_address",
                "phone": "phone_number",
                "registration_date": "created_at",
                "last_login_date": "last_active_at",
                "customer_status": "status"
            }
            
            # Rename columns
            for old_col, new_col in column_mapping.items():
                if old_col in batch.columns:
                    batch = batch.rename(columns={old_col: new_col})
            
            # Data type conversions
            if "created_at" in batch.columns:
                batch["created_at"] = pd.to_datetime(batch["created_at"])
            if "last_active_at" in batch.columns:
                batch["last_active_at"] = pd.to_datetime(batch["last_active_at"])
            
            # Data standardization
            if "email_address" in batch.columns:
                batch["email_address"] = batch["email_address"].str.lower().str.strip()
            
            if "phone_number" in batch.columns:
                # Standardize phone numbers
                batch["phone_number"] = batch["phone_number"].str.replace(r"[^\d]", "", regex=True)
            
            # Add migration metadata
            batch["migrated_at"] = datetime.now()
            batch["migration_batch_id"] = hash(str(batch.iloc[0]["id"])) % 1000
            batch["data_source"] = "legacy_system"
            
            # Data quality flags
            batch["email_valid"] = batch["email_address"].str.contains("@", na=False)
            batch["phone_valid"] = batch["phone_number"].str.len() == 10
            batch["data_quality_score"] = (
                batch["email_valid"].astype(int) + 
                batch["phone_valid"].astype(int)
            ) / 2
            
            return batch
        
        def validate_migration_quality(batch):
            """Validate migrated data quality."""
            validation_results = []
            
            for _, row in batch.iterrows():
                customer_id = row["id"]
                
                # Data completeness checks
                required_fields = ["id", "first_name", "last_name", "email_address"]
                completeness_score = sum(1 for field in required_fields 
                                       if pd.notna(row.get(field))) / len(required_fields)
                
                # Data format validation
                email_format_valid = "@" in str(row.get("email_address", ""))
                phone_format_valid = len(str(row.get("phone_number", ""))) == 10
                
                # Business rule validation
                reasonable_dates = True
                if pd.notna(row.get("created_at")):
                    created_at = pd.to_datetime(row["created_at"])
                    reasonable_dates = created_at >= datetime(1990, 1, 1) and created_at <= datetime.now()
                
                # Overall validation score
                validation_score = (
                    completeness_score * 0.4 +
                    (email_format_valid + phone_format_valid) / 2 * 0.3 +
                    reasonable_dates * 0.3
                )
                
                validation_results.append({
                    "customer_id": customer_id,
                    "completeness_score": completeness_score,
                    "format_validation_score": (email_format_valid + phone_format_valid) / 2,
                    "business_rule_score": reasonable_dates,
                    "overall_validation_score": validation_score,
                    "migration_quality": "high" if validation_score > 0.8 else 
                                       "medium" if validation_score > 0.6 else "low",
                    "requires_manual_review": validation_score < 0.7
                })
            
            return ray.data.from_pylist(validation_results)
        
        # Execute migration in batches
        migration_batches = []
        total_customers = 1000000  # Example total count
        batch_count = (total_customers // migration_config["batch_size"]) + 1
        
        for batch_id in range(min(batch_count, 10)):  # Process first 10 batches as example
            # Extract legacy data
            legacy_batch = extract_legacy_customer_data(batch_id)
            
            # Transform to modern format
            transformed_batch = legacy_batch.map_batches(transform_legacy_format)
            
            # Validate migration quality
            validated_batch = transformed_batch.map_batches(validate_migration_quality)
            
            migration_batches.append(validated_batch)
        
        # Combine all migration batches
        if migration_batches:
            all_migrated_data = migration_batches[0]
            for batch in migration_batches[1:]:
                all_migrated_data = all_migrated_data.union(batch)
        else:
            all_migrated_data = ray.data.from_items([])
        
        # Filter high-quality migrated data
        high_quality_data = all_migrated_data.filter(
            lambda row: row["migration_quality"] in ["high", "medium"]
        )
        
        # Flag data requiring review
        review_required = all_migrated_data.filter(
            lambda row: row["requires_manual_review"]
        )
        
        # Save migration results
        high_quality_data.write_parquet("s3://migrated-data/customers/validated/")
        review_required.write_parquet("s3://migrated-data/customers/review-required/")
        
        # Create migration summary report
        migration_summary = all_migrated_data.groupby("migration_quality").aggregate(
            ray.data.aggregate.Count("customer_id"),
            ray.data.aggregate.Mean("overall_validation_score")
        )
        
        migration_summary.write_csv("s3://reports/migration-summary.csv")
        
        return high_quality_data, review_required, migration_summary

**Data Migration Quality Checklist**

**Pre-Migration Planning:**
- [ ] **Data inventory**: Complete catalog of source systems and data volumes
- [ ] **Schema mapping**: Document transformation rules and data mappings
- [ ] **Quality baseline**: Establish current data quality metrics
- [ ] **Business rules**: Define validation rules and acceptance criteria
- [ ] **Rollback plan**: Prepare rollback procedures for migration issues

**Migration Execution:**
- [ ] **Batch processing**: Use appropriate batch sizes for source system capacity
- [ ] **Error handling**: Handle source system failures and data issues gracefully
- [ ] **Progress monitoring**: Track migration progress and performance metrics
- [ ] **Quality validation**: Validate data quality at each transformation step
- [ ] **Incremental approach**: Migrate in phases to reduce risk

**Post-Migration Validation:**
- [ ] **Data completeness**: Verify all expected data was migrated successfully
- [ ] **Data accuracy**: Validate transformed data matches business requirements
- [ ] **Performance testing**: Test query performance on migrated data
- [ ] **Business validation**: Confirm business processes work with migrated data
- [ ] **Monitoring setup**: Implement ongoing data quality monitoring

Next Steps
----------

Explore related migration and integration patterns:

* **Enterprise Integration**: Legacy system connectivity → :ref:`Enterprise Integration <enterprise-integration>`
* **Data Quality**: Validation and governance → :ref:`Data Quality & Governance <data-quality-governance>`
* **ETL Pipelines**: Modern data processing → :ref:`ETL Pipeline Guide <etl-pipelines>`
* **Performance Optimization**: Scale migration workloads → :ref:`Performance Optimization <performance-optimization>`
