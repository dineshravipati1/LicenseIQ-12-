--
-- PostgreSQL database dump
--

-- Dumped from database version 16.9 (415ebe8)
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: neondb_owner
--

-- *not* creating schema, since initdb creates it


ALTER SCHEMA public OWNER TO neondb_owner;

--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: neondb_owner
--

COMMENT ON SCHEMA public IS '';


--
-- Name: vector; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;


--
-- Name: EXTENSION vector; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION vector IS 'vector data type and ivfflat and hnsw access methods';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: audit_trail; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.audit_trail (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    user_id character varying NOT NULL,
    action character varying NOT NULL,
    resource_type character varying,
    resource_id character varying,
    details jsonb,
    ip_address character varying,
    user_agent text,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.audit_trail OWNER TO neondb_owner;

--
-- Name: business_units; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.business_units (
    org_id character varying DEFAULT gen_random_uuid() NOT NULL,
    company_id character varying NOT NULL,
    org_name character varying(500) NOT NULL,
    org_descr text,
    address1 character varying(500),
    contact_person character varying(300),
    contact_email character varying(300),
    contact_phone character varying(50),
    contact_preference character varying(50),
    status character varying(1) DEFAULT 'A'::character varying NOT NULL,
    created_by character varying NOT NULL,
    creation_date timestamp without time zone DEFAULT now() NOT NULL,
    last_updated_by character varying NOT NULL,
    last_update_date timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.business_units OWNER TO neondb_owner;

--
-- Name: companies; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.companies (
    company_id character varying DEFAULT gen_random_uuid() NOT NULL,
    company_name character varying(500) NOT NULL,
    company_descr text,
    address1 character varying(500),
    address2 character varying(500),
    address3 character varying(500),
    city character varying(200),
    state_province character varying(200),
    county character varying(200),
    country character varying(200),
    contact_person character varying(300),
    contact_email character varying(300),
    contact_phone character varying(50),
    contact_preference character varying(50),
    status character varying(1) DEFAULT 'A'::character varying NOT NULL,
    created_by character varying NOT NULL,
    creation_date timestamp without time zone DEFAULT now() NOT NULL,
    last_updated_by character varying NOT NULL,
    last_update_date timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.companies OWNER TO neondb_owner;

--
-- Name: compliance_analysis; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.compliance_analysis (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    compliance_score numeric(5,2),
    regulatory_frameworks jsonb,
    jurisdiction_analysis jsonb,
    data_protection_compliance boolean,
    industry_standards jsonb,
    risk_factors jsonb,
    recommended_actions jsonb,
    last_compliance_check timestamp without time zone DEFAULT now(),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.compliance_analysis OWNER TO neondb_owner;

--
-- Name: contract_analysis; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_analysis (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    summary text,
    key_terms jsonb,
    risk_analysis jsonb,
    insights jsonb,
    confidence numeric(5,2),
    processing_time integer,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_analysis OWNER TO neondb_owner;

--
-- Name: contract_approvals; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_approvals (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_version_id character varying NOT NULL,
    approver_id character varying NOT NULL,
    status character varying NOT NULL,
    decision_notes text,
    decided_at timestamp without time zone DEFAULT now(),
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_approvals OWNER TO neondb_owner;

--
-- Name: contract_comparisons; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_comparisons (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    similar_contracts jsonb,
    clause_variations jsonb,
    term_comparisons jsonb,
    best_practices jsonb,
    anomalies jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_comparisons OWNER TO neondb_owner;

--
-- Name: contract_documents; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_documents (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    extraction_run_id character varying,
    document_section character varying,
    section_order integer,
    raw_text text NOT NULL,
    normalized_text text,
    page_number integer,
    metadata jsonb,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_documents OWNER TO neondb_owner;

--
-- Name: contract_embeddings; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_embeddings (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    embedding_type character varying NOT NULL,
    source_text text NOT NULL,
    embedding public.vector(384),
    metadata jsonb,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_embeddings OWNER TO neondb_owner;

--
-- Name: contract_graph_edges; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_graph_edges (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    extraction_run_id character varying,
    source_node_id character varying NOT NULL,
    target_node_id character varying NOT NULL,
    relationship_type character varying NOT NULL,
    properties jsonb,
    confidence numeric(5,2),
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_graph_edges OWNER TO neondb_owner;

--
-- Name: contract_graph_nodes; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_graph_nodes (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    extraction_run_id character varying,
    node_type character varying NOT NULL,
    label character varying NOT NULL,
    properties jsonb NOT NULL,
    confidence numeric(5,2),
    source_document_id character varying,
    source_text text,
    embedding public.vector(384),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_graph_nodes OWNER TO neondb_owner;

--
-- Name: contract_obligations; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_obligations (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    obligation_type character varying NOT NULL,
    description text NOT NULL,
    due_date timestamp without time zone,
    responsible character varying,
    status character varying DEFAULT 'pending'::character varying,
    priority character varying DEFAULT 'medium'::character varying,
    completion_date timestamp without time zone,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_obligations OWNER TO neondb_owner;

--
-- Name: contract_royalty_calculations; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_royalty_calculations (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    name character varying NOT NULL,
    period_start timestamp without time zone,
    period_end timestamp without time zone,
    status character varying DEFAULT 'pending_approval'::character varying,
    total_sales_amount numeric(15,2),
    total_royalty numeric(15,2),
    currency character varying DEFAULT 'USD'::character varying,
    sales_count integer,
    breakdown jsonb,
    chart_data jsonb,
    calculated_by character varying,
    approved_by character varying,
    approved_at timestamp without time zone,
    rejected_by character varying,
    rejected_at timestamp without time zone,
    rejection_reason text,
    comments text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_royalty_calculations OWNER TO neondb_owner;

--
-- Name: contract_versions; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contract_versions (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    version_number integer NOT NULL,
    editor_id character varying NOT NULL,
    change_summary text,
    metadata_snapshot jsonb NOT NULL,
    file_reference character varying,
    approval_state character varying DEFAULT 'draft'::character varying NOT NULL,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.contract_versions OWNER TO neondb_owner;

--
-- Name: contracts; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.contracts (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    file_name character varying NOT NULL,
    original_name character varying NOT NULL,
    file_size integer NOT NULL,
    file_type character varying NOT NULL,
    file_path character varying NOT NULL,
    contract_type character varying,
    priority character varying DEFAULT 'normal'::character varying NOT NULL,
    status character varying DEFAULT 'uploaded'::character varying NOT NULL,
    uploaded_by character varying NOT NULL,
    notes text,
    processing_started_at timestamp without time zone,
    processing_completed_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    contract_number character varying,
    display_name character varying,
    effective_start timestamp without time zone,
    effective_end timestamp without time zone,
    renewal_terms text,
    governing_law character varying,
    counterparty_name character varying,
    contract_owner_id character varying,
    approval_state character varying DEFAULT 'draft'::character varying NOT NULL,
    current_version integer DEFAULT 1 NOT NULL,
    organization_name character varying,
    use_erp_matching boolean DEFAULT false NOT NULL
);


ALTER TABLE public.contracts OWNER TO neondb_owner;

--
-- Name: data_import_jobs; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.data_import_jobs (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    mapping_id character varying NOT NULL,
    customer_id character varying,
    job_name character varying NOT NULL,
    upload_meta jsonb,
    status character varying DEFAULT 'pending'::character varying NOT NULL,
    records_total integer DEFAULT 0,
    records_processed integer DEFAULT 0,
    records_failed integer DEFAULT 0,
    error_log jsonb,
    created_by character varying NOT NULL,
    started_at timestamp without time zone,
    completed_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.data_import_jobs OWNER TO neondb_owner;

--
-- Name: demo_requests; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.demo_requests (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    email character varying NOT NULL,
    plan_tier character varying NOT NULL,
    source character varying DEFAULT 'pricing_section'::character varying,
    status character varying DEFAULT 'new'::character varying NOT NULL,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.demo_requests OWNER TO neondb_owner;

--
-- Name: early_access_signups; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.early_access_signups (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    email character varying NOT NULL,
    name character varying,
    company character varying,
    source character varying DEFAULT 'landing_page'::character varying,
    status character varying DEFAULT 'new'::character varying NOT NULL,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.early_access_signups OWNER TO neondb_owner;

--
-- Name: erp_entities; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.erp_entities (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    system_id character varying NOT NULL,
    name character varying NOT NULL,
    technical_name character varying NOT NULL,
    entity_type character varying NOT NULL,
    description text,
    sample_data jsonb,
    status character varying DEFAULT 'active'::character varying NOT NULL,
    created_by character varying NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.erp_entities OWNER TO neondb_owner;

--
-- Name: erp_fields; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.erp_fields (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    entity_id character varying NOT NULL,
    field_name character varying NOT NULL,
    data_type character varying NOT NULL,
    constraints jsonb,
    sample_values text,
    description text,
    is_primary_key boolean DEFAULT false,
    is_required boolean DEFAULT false,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.erp_fields OWNER TO neondb_owner;

--
-- Name: erp_systems; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.erp_systems (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    name character varying NOT NULL,
    vendor character varying NOT NULL,
    version character varying,
    description text,
    category character varying DEFAULT 'enterprise'::character varying,
    status character varying DEFAULT 'active'::character varying NOT NULL,
    created_by character varying NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.erp_systems OWNER TO neondb_owner;

--
-- Name: extraction_runs; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.extraction_runs (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    run_type character varying NOT NULL,
    status character varying DEFAULT 'processing'::character varying NOT NULL,
    overall_confidence numeric(5,2),
    nodes_extracted integer,
    edges_extracted integer,
    rules_extracted integer,
    validation_results jsonb,
    ai_model character varying DEFAULT 'llama-3.1-8b'::character varying,
    processing_time integer,
    error_log text,
    triggered_by character varying,
    created_at timestamp without time zone DEFAULT now(),
    completed_at timestamp without time zone
);


ALTER TABLE public.extraction_runs OWNER TO neondb_owner;

--
-- Name: financial_analysis; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.financial_analysis (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    total_value numeric(15,2),
    currency character varying DEFAULT 'USD'::character varying,
    payment_schedule jsonb,
    royalty_structure jsonb,
    revenue_projections jsonb,
    cost_impact jsonb,
    currency_risk numeric(5,2),
    payment_terms text,
    penalty_clauses jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.financial_analysis OWNER TO neondb_owner;

--
-- Name: human_review_tasks; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.human_review_tasks (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    extraction_run_id character varying,
    task_type character varying NOT NULL,
    priority character varying DEFAULT 'normal'::character varying,
    status character varying DEFAULT 'pending'::character varying,
    target_id character varying,
    target_type character varying,
    original_data jsonb NOT NULL,
    suggested_correction jsonb,
    confidence numeric(5,2),
    review_notes text,
    assigned_to character varying,
    reviewed_by character varying,
    reviewed_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.human_review_tasks OWNER TO neondb_owner;

--
-- Name: imported_erp_records; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.imported_erp_records (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    job_id character varying NOT NULL,
    mapping_id character varying NOT NULL,
    customer_id character varying,
    source_record jsonb NOT NULL,
    target_record jsonb NOT NULL,
    embedding public.vector(384),
    metadata jsonb,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.imported_erp_records OWNER TO neondb_owner;

--
-- Name: licenseiq_entities; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.licenseiq_entities (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    name character varying(100) NOT NULL,
    technical_name character varying(100) NOT NULL,
    description text,
    category character varying(50),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.licenseiq_entities OWNER TO neondb_owner;

--
-- Name: licenseiq_entity_records; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.licenseiq_entity_records (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    entity_id character varying NOT NULL,
    record_data jsonb NOT NULL,
    created_by character varying,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    grp_id character varying NOT NULL,
    org_id character varying NOT NULL,
    loc_id character varying NOT NULL
);


ALTER TABLE public.licenseiq_entity_records OWNER TO neondb_owner;

--
-- Name: licenseiq_fields; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.licenseiq_fields (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    entity_id character varying NOT NULL,
    field_name character varying(100) NOT NULL,
    data_type character varying(50) NOT NULL,
    description text,
    is_required boolean DEFAULT false NOT NULL,
    default_value character varying,
    validation_rules text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.licenseiq_fields OWNER TO neondb_owner;

--
-- Name: locations; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.locations (
    loc_id character varying DEFAULT gen_random_uuid() NOT NULL,
    company_id character varying NOT NULL,
    org_id character varying NOT NULL,
    loc_name character varying(500) NOT NULL,
    loc_descr text,
    address1 character varying(500),
    contact_person character varying(300),
    contact_email character varying(300),
    contact_phone character varying(50),
    contact_preference character varying(50),
    status character varying(1) DEFAULT 'A'::character varying NOT NULL,
    created_by character varying NOT NULL,
    creation_date timestamp without time zone DEFAULT now() NOT NULL,
    last_updated_by character varying NOT NULL,
    last_update_date timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.locations OWNER TO neondb_owner;

--
-- Name: market_benchmarks; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.market_benchmarks (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_type character varying NOT NULL,
    industry character varying,
    benchmark_data jsonb,
    average_value numeric(15,2),
    standard_terms jsonb,
    risk_factors jsonb,
    last_updated timestamp without time zone DEFAULT now(),
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.market_benchmarks OWNER TO neondb_owner;

--
-- Name: master_data_mappings; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.master_data_mappings (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    mapping_name character varying NOT NULL,
    erp_system character varying NOT NULL,
    entity_type character varying NOT NULL,
    source_schema jsonb NOT NULL,
    target_schema jsonb NOT NULL,
    mapping_results jsonb NOT NULL,
    status character varying DEFAULT 'active'::character varying NOT NULL,
    ai_model character varying DEFAULT 'llama-3.3-70b-versatile'::character varying,
    created_by character varying NOT NULL,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    customer_id character varying
);


ALTER TABLE public.master_data_mappings OWNER TO neondb_owner;

--
-- Name: navigation_permissions; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.navigation_permissions (
    id character varying DEFAULT (gen_random_uuid())::text NOT NULL,
    item_key character varying NOT NULL,
    item_name character varying NOT NULL,
    href character varying NOT NULL,
    icon_name character varying,
    default_roles jsonb DEFAULT '[]'::jsonb,
    is_active boolean DEFAULT true,
    sort_order integer DEFAULT 0,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.navigation_permissions OWNER TO neondb_owner;

--
-- Name: performance_metrics; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.performance_metrics (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    performance_score numeric(5,2),
    milestone_completion numeric(5,2),
    on_time_delivery boolean DEFAULT true,
    budget_variance numeric(10,2),
    quality_score numeric(5,2),
    client_satisfaction numeric(5,2),
    renewal_probability numeric(5,2),
    last_review_date timestamp without time zone,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.performance_metrics OWNER TO neondb_owner;

--
-- Name: role_navigation_permissions; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.role_navigation_permissions (
    id character varying DEFAULT (gen_random_uuid())::text NOT NULL,
    role character varying NOT NULL,
    nav_item_key character varying NOT NULL,
    is_enabled boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.role_navigation_permissions OWNER TO neondb_owner;

--
-- Name: roles; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.roles (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    role_name character varying NOT NULL,
    display_name character varying NOT NULL,
    description text,
    is_system_role boolean DEFAULT false,
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.roles OWNER TO neondb_owner;

--
-- Name: royalty_rules; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.royalty_rules (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    rule_type character varying NOT NULL,
    rule_name character varying NOT NULL,
    description text,
    product_categories text[],
    territories text[],
    container_sizes text[],
    seasonal_adjustments jsonb,
    territory_premiums jsonb,
    volume_tiers jsonb,
    base_rate numeric(15,2),
    minimum_guarantee numeric(15,2),
    calculation_formula text,
    priority integer DEFAULT 10,
    is_active boolean DEFAULT true,
    confidence numeric(5,2),
    source_section character varying,
    source_text text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    formula_definition jsonb,
    formula_version character varying DEFAULT '1.0'::character varying
);


ALTER TABLE public.royalty_rules OWNER TO neondb_owner;

--
-- Name: rule_definitions; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.rule_definitions (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    extraction_run_id character varying,
    linked_graph_node_id character varying,
    rule_type character varying NOT NULL,
    rule_name character varying NOT NULL,
    description text,
    formula_definition jsonb NOT NULL,
    applicability_filters jsonb,
    confidence numeric(5,2),
    validation_status character varying DEFAULT 'pending'::character varying,
    validation_errors jsonb,
    is_active boolean DEFAULT false,
    version integer DEFAULT 1,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.rule_definitions OWNER TO neondb_owner;

--
-- Name: rule_node_definitions; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.rule_node_definitions (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    node_type character varying NOT NULL,
    display_name character varying NOT NULL,
    description text,
    schema jsonb NOT NULL,
    evaluation_adapter text,
    examples jsonb,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.rule_node_definitions OWNER TO neondb_owner;

--
-- Name: rule_validation_events; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.rule_validation_events (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    rule_definition_id character varying NOT NULL,
    validation_type character varying NOT NULL,
    validation_result character varying NOT NULL,
    issues jsonb,
    recommendations jsonb,
    validator_id character varying,
    validated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.rule_validation_events OWNER TO neondb_owner;

--
-- Name: sales_data; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.sales_data (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    matched_contract_id character varying,
    match_confidence numeric(5,2),
    transaction_date timestamp without time zone NOT NULL,
    transaction_id character varying,
    product_code character varying,
    product_name character varying,
    category character varying,
    territory character varying,
    currency character varying DEFAULT 'USD'::character varying,
    gross_amount numeric(15,2) NOT NULL,
    net_amount numeric(15,2),
    quantity numeric(12,4),
    unit_price numeric(15,2),
    custom_fields jsonb,
    import_job_id character varying,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.sales_data OWNER TO neondb_owner;

--
-- Name: sales_field_mappings; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.sales_field_mappings (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying,
    source_field_name character varying NOT NULL,
    target_field_type character varying NOT NULL,
    mapping_confidence numeric(5,2),
    mapping_method character varying DEFAULT 'ai_semantic'::character varying,
    sample_values jsonb,
    approved_by character varying,
    approved_at timestamp without time zone,
    usage_count integer DEFAULT 0,
    last_used_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.sales_field_mappings OWNER TO neondb_owner;

--
-- Name: semantic_index_entries; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.semantic_index_entries (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    index_type character varying NOT NULL,
    source_id character varying,
    content text NOT NULL,
    embedding public.vector(384),
    metadata jsonb,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.semantic_index_entries OWNER TO neondb_owner;

--
-- Name: session; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.session (
    sid character varying NOT NULL,
    sess json NOT NULL,
    expire timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.session OWNER TO neondb_owner;

--
-- Name: sessions; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.sessions (
    sid character varying NOT NULL,
    sess jsonb NOT NULL,
    expire timestamp without time zone NOT NULL
);


ALTER TABLE public.sessions OWNER TO neondb_owner;

--
-- Name: strategic_analysis; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.strategic_analysis (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    contract_id character varying NOT NULL,
    strategic_value numeric(5,2),
    market_alignment numeric(5,2),
    competitive_advantage jsonb,
    risk_concentration numeric(5,2),
    standardization_score numeric(5,2),
    negotiation_insights jsonb,
    benchmark_comparison jsonb,
    recommendations jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.strategic_analysis OWNER TO neondb_owner;

--
-- Name: system_embeddings; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.system_embeddings (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    document_id character varying NOT NULL,
    category character varying NOT NULL,
    title character varying NOT NULL,
    source_text text NOT NULL,
    embedding public.vector(384),
    metadata jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.system_embeddings OWNER TO neondb_owner;

--
-- Name: user_navigation_overrides; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.user_navigation_overrides (
    id character varying DEFAULT (gen_random_uuid())::text NOT NULL,
    user_id character varying NOT NULL,
    nav_item_key character varying NOT NULL,
    is_enabled boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.user_navigation_overrides OWNER TO neondb_owner;

--
-- Name: users; Type: TABLE; Schema: public; Owner: neondb_owner
--

CREATE TABLE public.users (
    id character varying DEFAULT gen_random_uuid() NOT NULL,
    email character varying,
    first_name character varying,
    last_name character varying,
    profile_image_url character varying,
    role character varying DEFAULT 'viewer'::character varying NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    username character varying,
    password character varying DEFAULT 'temp'::character varying NOT NULL
);


ALTER TABLE public.users OWNER TO neondb_owner;

--
-- Data for Name: audit_trail; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.audit_trail (id, user_id, action, resource_type, resource_id, details, ip_address, user_agent, created_at) FROM stdin;
5480e25b-6e10-4fc8-8541-bf7074beb078	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	f3b22140-51eb-4bb8-a6e0-685ee48c44fc	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.7.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 16:59:13.677776
b1dce8b2-c348-4237-afc0-12e463abf4cf	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data (2).csv", "contractId": "f3b22140-51eb-4bb8-a6e0-685ee48c44fc", "rowsImported": 15}	10.81.7.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:03:28.3537
992aa4f6-d782-4f6f-90db-30b616f07c74	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	72e50ccb-56ca-4955-8c61-487b67be432d	{"updates": {"baseRate": null, "priority": 1, "ruleName": "Aurora Flame Maple - 1-gallon with volume tiers", "ruleType": "formula_based", "description": "1-gallon containers: $1.25 per unit, $1.10 for 5000+ units with seasonal adjustments", "territories": ["Primary", "Secondary"], "volumeTiers": [{"max": 4999, "min": 0, "rate": 1.25, "label": "< 5000 units"}, {"max": null, "min": 5000, "rate": 1.1, "label": "5000+ units"}], "containerSizes": ["1-gallon"], "minimumGuarantee": null, "productCategories": ["Ornamental Trees"], "territoryPremiums": {}, "seasonalAdjustments": {"Fall": 0.95, "Spring": 1.1, "Summer": 1, "Holiday": 1.2}}, "contractId": "f3b22140-51eb-4bb8-a6e0-685ee48c44fc"}	10.81.0.33	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:05:07.810881
defc3796-6aa7-4485-b5d3-d59594f4ae00	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	4118fe02-2a39-474c-9cd6-84b5810bb722	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.0.33	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:06:04.235018
3cdc9c04-0327-486e-bf9f-0803702c9de6	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	85f71371-5fa1-4dbb-9398-499116ea60fa	{"updates": {"baseRate": null, "priority": 1, "ruleName": "Tier 1 — Ornamental Trees & Shrubs", "ruleType": "tiered_pricing", "description": "Plant Size CategoryRoyalty per UnitVolume Discount ThresholdDiscounted Rate\\n1-gallon containers$1.255,000+ units annually$1.10\\n3-gallon containers$2.852,000+ units annually$2.50\\n5-gallon containers$4.501,000+ units annually$3.95\\n15-gallon+ specimens$12.75200+ units annually$11.25", "territories": ["Primary Territory"], "volumeTiers": [{"max": 4999, "min": 0, "rate": 1.25}, {"min": 5000, "rate": 1.1}], "containerSizes": [], "minimumGuarantee": null, "productCategories": ["Ornamental Trees", "Shrubs"], "territoryPremiums": {}, "seasonalAdjustments": {}}, "contractId": "f3b22140-51eb-4bb8-a6e0-685ee48c44fc"}	10.81.6.3	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:11:53.878971
6df87b45-49e5-4da7-ac2b-cf662d4415bf	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	10b75927-b7a5-4bac-b611-b84ec16be08d	{"fileSize": 383171, "originalName": "Pharmaceutical Patent License & Drug Royalty Agreement.pdf"}	10.81.6.3	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:12:19.750451
ef39c071-849c-4939-8247-49432e92531a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	9a97f4d8-d384-4123-862e-ca98b1f181a4	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.7.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:14:43.720433
2835ffc5-19b6-429e-b139-3a764436e196	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	9a97f4d8-d384-4123-862e-ca98b1f181a4	{"fileName": "3e96f12f-e585-4ee8-9b49-b64dfc3544b8.pdf"}	10.81.6.3	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:15:46.163406
2f29a1a0-781c-4288-a7d0-8cde5988c50b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	10b75927-b7a5-4bac-b611-b84ec16be08d	{"fileName": "5bf7a6ef-f250-475d-ab23-ea65d0af6e7b.pdf"}	10.81.7.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:15:56.381061
36329239-1059-4a1b-b2b5-cda18d33f884	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	4118fe02-2a39-474c-9cd6-84b5810bb722	{"fileName": "95ff603a-42b7-4eb9-8a29-2ccccc113467.pdf"}	10.81.7.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:15:58.515471
9971dd49-f407-4f79-bc45-580cbfaf60d9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	f3b22140-51eb-4bb8-a6e0-685ee48c44fc	{"fileName": "4e92ec1e-1e7e-4dc9-8807-60281a2175df.pdf"}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:16:00.366243
8d8e0a0e-4679-4f8c-9e2f-80f1dd06f916	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	9c43b465-ae5b-4fa7-b3af-4e79c30ec50f	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:18:22.851813
da73dc29-e819-4887-bd39-321b9d20d364	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	9c43b465-ae5b-4fa7-b3af-4e79c30ec50f	{"fileName": "5a3b84ba-9d81-4151-9b24-163035834e03.pdf"}	10.81.8.35	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:26:16.6414
7640f11b-a9b2-488b-96e0-97aa67126d1f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	712eca87-fa18-4a34-a7b5-29469202f1eb	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.8.35	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:26:28.295752
279bc29d-0b7f-4bc5-b061-638ff57a7069	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data (2).csv", "contractId": "712eca87-fa18-4a34-a7b5-29469202f1eb", "rowsImported": 15}	10.81.9.5	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:32:12.109683
22a6028e-4f58-4316-a706-7997512a080a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	712eca87-fa18-4a34-a7b5-29469202f1eb	{"fileName": "c37cea2b-19ee-4ced-ae94-4414dbb0a828.pdf"}	10.81.0.33	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:51:17.997483
94cfb916-5d9b-4daf-ac91-78cea9a63bc9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	01fde547-a1b2-4341-a24b-659c98e42edc	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:51:33.181126
cee0922a-ca63-488c-b7cc-43de72bc289d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data (2).csv", "contractId": "01fde547-a1b2-4341-a24b-659c98e42edc", "rowsImported": 15}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:53:49.116684
dae1bd48-0d81-4720-adac-5564c89bf5f1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	01fde547-a1b2-4341-a24b-659c98e42edc	{"fileName": "02de3426-39f3-472c-a0fb-5b3b4db22cf9.pdf"}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:59:07.584377
47a3790b-3390-4185-8f45-47face83e16a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	1758543f-6cb3-45d2-ad18-1abefb1aaf95	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 17:59:26.113665
c46f8cfa-a63d-4853-a613-69d560574652	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data (2).csv", "contractId": "1758543f-6cb3-45d2-ad18-1abefb1aaf95", "rowsImported": 15}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 18:00:51.435125
37c20d98-947b-401c-8991-e53c0532e630	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_royalty_calculation	calculation	ed30a19c-4dd0-4bc1-bfc0-7c2185bb3069	{"contractId": "1758543f-6cb3-45d2-ad18-1abefb1aaf95", "contractName": "Plant Variety License & Royalty Agreement.pdf", "calculationName": "Q1 2024"}	10.81.7.95	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 20:07:09.568564
9fdaec97-0b1f-4b78-9f2c-79bf48e1b6c9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0, "sourcesCount": 0}	10.81.7.95	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 20:07:39.384998
071ff5e0-7b6b-4900-9257-a4429dc881d7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0, "sourcesCount": 0}	10.81.7.95	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 20:07:49.030629
522538c1-81d7-4947-83c0-6313adf122d6	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0, "sourcesCount": 0}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 20:15:21.848138
b23ae851-80c0-4e0a-97a9-27105e65ba5a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0, "sourcesCount": 0}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 20:15:34.653373
bfcbfdfe-ea84-4c5e-9f70-30fee860c538	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	1758543f-6cb3-45d2-ad18-1abefb1aaf95	{"fileName": "f54f0b43-6f76-4229-b05d-7d49a803db33.pdf"}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-13 22:30:17.097274
ba52b9a6-4294-4c23-89bf-3f16cb991da2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	423de8ff-bfd5-4869-9dc4-350087d904cd	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 18:14:36.52226
cb7cb7c6-ff63-4d84-8091-7ed9064b7aff	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data (2).csv", "contractId": "423de8ff-bfd5-4869-9dc4-350087d904cd", "rowsImported": 15}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 18:15:40.957562
eb587bc7-8d54-43c1-9c4f-f2815a6032a1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	423de8ff-bfd5-4869-9dc4-350087d904cd	{"fileName": "aaad1396-588e-4e09-b845-0cfe86ebcab2.pdf"}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 18:23:02.1282
0cc91dc3-cb25-477e-8587-117d2e510965	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	7f812882-ea64-4e26-a72e-d09651a201fc	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.6.44	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 18:23:24.129337
fac2c221-9dc7-4370-bab0-617d6f807aa8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data (2).csv", "contractId": "7f812882-ea64-4e26-a72e-d09651a201fc", "rowsImported": 15}	10.81.6.44	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 18:24:37.258819
d1c20a75-6413-477c-8f75-47f72267f138	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	7f812882-ea64-4e26-a72e-d09651a201fc	{"fileName": "6c0e92a1-1ed4-4e8c-8bed-e3a642f411a0.pdf"}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 18:40:31.857241
c6b9186d-6826-4a8d-bae5-f00fc858d70b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	46fcb371-8c9a-4f3b-ba92-b9857f3667e7	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 18:40:50.095878
a92e771b-90ce-4f23-a8f9-668be2e1166a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data (2).csv", "contractId": "46fcb371-8c9a-4f3b-ba92-b9857f3667e7", "rowsImported": 15}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 18:41:33.58163
45fcfa92-f0cd-42d0-bcf0-2ac216923371	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	46fcb371-8c9a-4f3b-ba92-b9857f3667e7	{"fileName": "5774451e-70ee-44ee-a0dc-895611eec489.pdf"}	10.81.9.5	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:01:40.457561
292d1361-fced-43fb-9881-8dd582b31ef0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	8018607c-c2fa-48fc-8936-6a28d1c0e464	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.9.5	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:01:47.486075
6091b4f1-810a-406f-8d85-6bea18278ee4	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	8018607c-c2fa-48fc-8936-6a28d1c0e464	{"fileName": "d8fa6887-aacc-4478-ba6c-7e2a03a86a4c.pdf"}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:03:59.486886
d4108849-3f6b-4658-9760-22c10581fa83	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	b8984931-3bb0-4b5d-b8b3-467b3b145cc9	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:04:08.635314
070aa5b3-4836-4a59-bae4-ed605cd37c33	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	b8984931-3bb0-4b5d-b8b3-467b3b145cc9	{"fileName": "d3d2b41b-b6c8-43df-abe6-98ca1fa52e42.pdf"}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:08:22.134411
6e2d9eb1-4d03-4f80-92cc-c3429266de55	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	81e29011-16f3-4bc0-9ee2-c24b8aa19419	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:08:34.059027
c64ed805-691b-4d61-98a0-c675dfbbaddf	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "81e29011-16f3-4bc0-9ee2-c24b8aa19419", "rowsImported": 15}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:10:42.59546
3c96d910-bf15-4695-a3ad-b8db2c5b48ba	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.5034233651389687, "sourcesCount": 3}	10.81.10.86	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:16:03.846953
bed72af3-4087-4587-911c-bb4626e9ff09	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	user_role_update	user	deafebd3-22fb-4d7b-9426-f019c3203bce	{"previousRole": "unknown"}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:31:00.90492
8489edfb-244c-4d73-843c-ad51547d5080	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	81e29011-16f3-4bc0-9ee2-c24b8aa19419	{"fileName": "c47f0c85-4f30-41b4-b014-2d6b5baa1ff4.pdf"}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:31:34.928876
13ab58ef-a32f-4241-a8c5-f98c061c70a4	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	74a504c7-8a36-4e51-b31f-b06918800954	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:31:54.624735
6e47eb86-4608-4f19-b31c-907387f3a020	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "74a504c7-8a36-4e51-b31f-b06918800954", "rowsImported": 15}	10.81.5.37	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:33:41.329496
442eb0d6-24a6-4cd3-a14a-9661dbef86c0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What products are included?", "confidence": 0.5282160153710113, "sourcesCount": 3}	10.81.3.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-14 22:35:15.862559
01610e4d-4324-4c76-b5bf-4afa7aa8b974	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	74a504c7-8a36-4e51-b31f-b06918800954	{"fileName": "379e9339-d146-4749-99fc-de81f9d792e5.pdf"}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 15:09:18.557565
6b1d3533-2b51-406f-991b-c8bf2253263e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	83fea764-9585-469c-ab3c-c612c19f204b	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 15:09:34.981571
56d4d85c-44ff-4958-8c86-ad40f410a3a4	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	0cb1aaf1-1b9a-4202-a425-295f6d7dd2d7	{"updates": {"baseRate": "-5.00", "priority": 3, "ruleName": "Fall Seasonal Discount", "ruleType": "tiered_pricing", "description": "seasonal discount for fall sales", "territories": [], "volumeTiers": [], "containerSizes": [], "minimumGuarantee": null, "productCategories": [], "territoryPremiums": {}, "seasonalAdjustments": {}}, "contractId": "83fea764-9585-469c-ab3c-c612c19f204b"}	10.81.0.48	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 15:10:35.608725
92e885ee-0d7e-4f88-847e-5d4dfefa4a96	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.5159579765429815, "sourcesCount": 3}	10.81.0.48	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 15:11:58.254547
ca209ada-1628-49ea-977c-6829dfa37c30	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.48869690732732307, "sourcesCount": 3}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 15:18:08.690493
5879e37f-c5f4-49c1-a4a8-7d355371b98b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.5159579765429815, "sourcesCount": 3}	10.81.1.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 15:18:16.949009
4506087b-9beb-49c6-82ae-99a23ce02bf7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.75, "sourcesCount": 3}	10.81.1.28	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 15:23:46.310549
8aad807c-0cdf-49c7-8d2c-a72c1089ca37	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	83fea764-9585-469c-ab3c-c612c19f204b	{"fileName": "08a72163-c62a-4b9d-ae6a-3e110790cb6b.pdf"}	10.81.11.13	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 18:21:16.953123
b942165d-731a-44b2-8447-f28012a3e5af	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	f0be838a-ea40-4049-bfa7-9cfd5a1b4ec3	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.13	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 18:21:25.433953
99823d80-dc30-4196-9cdf-a05d99ac0346	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "f0be838a-ea40-4049-bfa7-9cfd5a1b4ec3", "rowsImported": 15}	10.81.3.22	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 18:22:57.143176
75af8bb0-ee7c-4bcf-8f7d-b18c01fdfe78	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	f0be838a-ea40-4049-bfa7-9cfd5a1b4ec3	{"fileName": "bda42201-6cf1-446a-95f6-64b38b4c94fc.pdf"}	10.81.0.48	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 18:29:22.94415
50edacc9-a7eb-4813-add2-b137d547c05a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	500477ab-8c84-4e1e-8fbe-221cb5d67149	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.0.48	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 18:29:37.972504
f2d4f8bb-0114-4d54-b8e3-41c59094f89d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.75, "sourcesCount": 3}	10.81.4.28	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 19:52:45.245914
ff960b10-f95e-4d81-990b-075219db1665	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 3}	10.81.4.28	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 19:52:58.56398
a1016023-2a98-44c7-a185-1f80bcbeed08	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What products are included?", "confidence": 0.75, "sourcesCount": 3}	10.81.4.28	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36	2025-10-15 19:53:18.908053
3d8d1ee5-44f0-498d-aa6f-d9534d641456	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "500477ab-8c84-4e1e-8fbe-221cb5d67149", "rowsImported": 15}	10.81.7.38	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 21:03:53.085613
26b1c905-f315-4108-a477-f06859c825bc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	500477ab-8c84-4e1e-8fbe-221cb5d67149	{"fileName": "7fd57fa2-26ba-43be-9954-9664e3cf7e56.pdf"}	10.81.5.33	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 21:48:27.574585
16ccef0c-50e3-4a28-9f23-ea04486d2cda	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	10e7faf5-85ca-43bc-abb7-989add138507	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.5.33	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 21:48:35.094147
4c5d31b7-828a-4a89-b6e3-e7f9cd5c1bbc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "10e7faf5-85ca-43bc-abb7-989add138507", "rowsImported": 15}	10.81.5.33	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 21:49:07.085166
1b9132f0-e1bf-4d24-ab19-afef57d965a5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	10e7faf5-85ca-43bc-abb7-989add138507	{"fileName": "a22bdddf-2547-45d4-a69d-233f341c7deb.pdf"}	10.81.4.28	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 22:33:03.356593
eaea97cd-07ba-48a4-b796-54ba2f27531a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	39cad06e-22eb-4706-8d25-fdc88d04a5eb	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.4.28	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 22:33:12.216313
3983b5a6-0e57-4475-bd3a-af43ba549656	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "39cad06e-22eb-4706-8d25-fdc88d04a5eb", "rowsImported": 15}	10.81.11.13	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 22:34:39.589574
425462c0-aa10-486c-ba0c-342661369e87	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 3}	10.81.3.22	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 22:37:19.341785
128f0685-b9ef-4148-8929-7de3f2159c57	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0.75, "sourcesCount": 3}	10.81.3.22	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 22:37:24.908811
01b96c6b-6306-47df-bd9c-650da5a612df	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "who is chiranjeevi", "confidence": 0.75, "sourcesCount": 3}	10.81.3.22	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 22:37:38.026223
cd397d64-3d6d-444b-adb9-e20101eac03a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "who is sunil gavaskar", "confidence": 0.75, "sourcesCount": 3}	10.81.3.22	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-15 22:38:36.812334
22896274-6191-4103-83b7-c2635f82b6a1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	user_profile_update	user	deafebd3-22fb-4d7b-9426-f019c3203bce	{"updatedFields": {"email": "testvendor123@example.com", "lastName": "Vendor", "firstName": "Test"}}	10.81.5.33	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-16 14:28:51.009294
a361361d-a84e-4518-a1f1-c7e0fa92bc57	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	39cad06e-22eb-4706-8d25-fdc88d04a5eb	{"fileName": "93481dde-63f2-4e9f-9a27-d5c96961e601.pdf"}	10.81.10.10	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-16 20:09:48.639179
a8c915cf-99c7-4410-a5be-f484acd1285d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	92bcceec-5aff-4f48-b430-82372b33809e	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.10.10	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-16 20:09:58.745836
57b1f98e-1797-46bc-86c5-6780b46628c3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "92bcceec-5aff-4f48-b430-82372b33809e", "rowsImported": 15}	10.81.3.22	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-16 20:10:43.914156
603b3ef1-1b63-4f0b-94a1-292af679eb63	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "0f3320ac-6393-4676-8744-351b10de5509", "rowsImported": 15}	10.81.4.40	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 20:17:12.816477
7dc019d4-f36c-483d-aa72-5780566f6ccd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	e9755d7b-79a3-44b4-978b-285ddcdc8648	{"updates": {"baseRate": null, "priority": 1, "ruleName": "Cascade Blue Hydrangea - Sales Volume (Annual) with tier", "ruleType": "formula_based", "description": "Sales Volume (Annual): 2.25% royalty for 1-2500 units, 1.95% for 2501-7500 units, 1.70% for 7501-15000 units, 1.45% for 15001+ units with seasonal adjustments", "territories": ["Primary", "Secondary"], "volumeTiers": [{"max": 2500, "min": 0, "rate": 2.25, "label": "1-2500 units"}, {"max": 7500, "min": 2501, "rate": 1.95, "label": "2501-7500 units"}, {"max": 15000, "min": 7501, "rate": 1.7, "label": "7501-15000 units"}, {"max": null, "min": 15001, "rate": 1.45, "label": "15001+ units"}], "containerSizes": [], "minimumGuarantee": null, "productCategories": ["Flowering Shrubs"], "territoryPremiums": {}, "seasonalAdjustments": {"Fall": 0.95, "Spring": 1.1, "Summer": 1, "Holiday": 1.2}}, "contractId": "92bcceec-5aff-4f48-b430-82372b33809e"}	10.81.7.97	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 21:47:07.997562
80ffa292-e650-456c-b1e8-4a4326417e15	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	92bcceec-5aff-4f48-b430-82372b33809e	{"fileName": "477bc78a-5c84-4a3c-b994-a0e6212a602c.pdf"}	10.81.9.49	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 21:48:37.313164
0f730620-2ce9-4a29-b237-e2bfe0b85f70	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	1d68d679-6f91-4958-96b9-0108b90377b2	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.7.97	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 21:49:42.5377
5e155b5f-47f3-406a-9501-46a5716cbebd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "1d68d679-6f91-4958-96b9-0108b90377b2", "rowsImported": 15}	10.81.7.97	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 21:52:03.932397
73d185a2-f13a-4bf1-afae-d2fc9edc2656	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 3}	10.81.9.49	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 21:55:19.666118
9becd4af-da6c-4192-9db0-57d49d0522b6	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Are there any volume discounts?", "confidence": 0.75, "sourcesCount": 3}	10.81.9.49	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 21:55:43.974531
3c0dd13c-1acd-4fb2-a3ea-d1ba8eacacff	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	a9234a4f-a00e-4415-aefb-aba328605825	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.9.49	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 21:59:33.119019
643fb02d-1924-4cbe-bea2-2a25dedd3377	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	a9234a4f-a00e-4415-aefb-aba328605825	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 2}	10.81.11.27	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 22:01:04.507247
f60e6c8b-6729-4849-b040-53d6f518d929	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	user_role_update	user	deafebd3-22fb-4d7b-9426-f019c3203bce	{"previousRole": "unknown"}	10.81.3.111	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-17 22:15:12.394447
3b04b91d-19af-4e5e-a80b-64ce938f892e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	a9234a4f-a00e-4415-aefb-aba328605825	{"fileName": "e8060262-3928-4baf-a9f3-2387aba27182.pdf"}	10.81.9.165	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 19:16:28.995167
75d27f8e-ae1a-4706-a2cb-5ce51eba6e40	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	1d68d679-6f91-4958-96b9-0108b90377b2	{"fileName": "ca9a7339-2599-4800-a35a-5de71353b1e3.pdf"}	10.81.12.254	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 19:16:31.15713
863ba82c-5f47-4b0c-849e-da0325597439	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	bf8ba3cd-fb66-4b25-b264-fd2dc5532c95	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.12.254	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 19:16:38.451604
cffec287-b68c-451e-b20b-882c3b989f1a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "bf8ba3cd-fb66-4b25-b264-fd2dc5532c95", "rowsImported": 15}	10.81.9.165	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 19:17:19.948692
569567fb-a2ea-49ec-90ce-45f69016f20d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	bf8ba3cd-fb66-4b25-b264-fd2dc5532c95	{"fileName": "a8d850dc-e510-4fcf-976d-25c517ec8994.pdf"}	10.81.3.215	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 20:00:28.632613
46db1021-8bcf-4f15-8d9f-cde1d9e276db	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	62ab9050-0233-46c5-961f-d32329a8253c	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.3.215	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 20:00:40.325857
5e09c17f-7531-43c4-b637-fb12709cfeb3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "62ab9050-0233-46c5-961f-d32329a8253c", "rowsImported": 15}	10.81.3.215	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 20:01:24.899656
e872909c-cdfa-45c3-b62f-1b60de455f6c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	62ab9050-0233-46c5-961f-d32329a8253c	{"fileName": "8f679ac1-bbc3-469d-aa55-2be3251fd6d7.pdf"}	10.81.8.100	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 20:14:25.001528
5bf9882a-2c04-4da6-a065-ea04b2f47f48	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	0f3320ac-6393-4676-8744-351b10de5509	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.8.100	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-21 20:14:44.434985
1e7cbea0-79fa-48b3-9963-73107ca2216a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	0f3320ac-6393-4676-8744-351b10de5509	{"fileName": "42655b9b-9476-419b-b2fc-8f14b1418b59.pdf"}	10.81.12.44	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-23 15:54:58.145051
a0b443f6-69df-4748-8e32-b0cd3a1d0151	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	36a4ef22-9334-4879-98ce-d8bcc225e3d5	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.12.44	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-23 15:55:05.952829
8ff581c4-8acf-47ba-8376-358efe667d8a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "36a4ef22-9334-4879-98ce-d8bcc225e3d5", "rowsImported": 15}	10.81.11.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-23 16:02:08.623986
9a49f4df-b702-4b70-bf95-d64ef6b4a7c7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 3}	10.81.12.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 17:25:49.33376
8abca531-cbab-4333-9e4c-882a39e82bd3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "hi", "confidence": 0.75, "sourcesCount": 3}	10.81.12.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 17:26:04.790503
be9ccf9e-932f-44d7-82c1-744d62be8d25	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 3}	10.81.12.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 17:30:53.109034
ed48b779-e00d-4a2a-9e0c-dcf094118af8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "hi", "confidence": 0.75, "sourcesCount": 3}	10.81.12.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 17:31:03.247357
6024e44f-3da8-4308-afc2-cbce9de16da7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0.75, "sourcesCount": 3}	10.81.12.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 17:31:40.188298
d65a8ada-92ed-474a-bf31-bcee994ae57d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "what royalty calculations", "confidence": 0.75, "sourcesCount": 3}	10.81.12.4	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 17:31:52.629078
6ccd5d64-b6e0-424f-968f-06bd13d5b721	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	36a4ef22-9334-4879-98ce-d8bcc225e3d5	{"fileName": "93ff6a5a-e4f3-4758-8965-49f009452625.pdf"}	10.81.3.39	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 21:39:24.716948
db913c08-7f63-4ea6-ad12-e24fe69e69e8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	734eac42-b3c7-4ba3-99ef-60dfe5bd7402	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.3.39	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 21:40:17.977877
4e54112e-b7b8-499a-b307-f1483822cb7b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "734eac42-b3c7-4ba3-99ef-60dfe5bd7402", "rowsImported": 15}	10.81.3.39	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-24 21:41:38.093078
bc11651c-1361-4f5a-bcf3-361ca68de5f2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	c74106fd-07ce-4954-a79d-7803a80458ca	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.7.22	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-25 23:59:46.045742
f8cafb26-7c3c-450e-bc41-fd842f78ce28	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	f2b54aad-0c5e-4230-8a09-6e2e9b2024ef	{"updates": {"baseRate": null, "priority": 1, "ruleName": "Aurora Flame Maple - 1-gallon with volume tiers", "ruleType": "formula_based", "description": "1-gallon containers: 1.25% royalty, 1.10% for 5000+ units with seasonal adjustments", "territories": ["Primary", "Secondary"], "volumeTiers": [{"max": 4999, "min": 0, "rate": 1.25, "label": "< 5000 units"}, {"max": null, "min": 5000, "rate": 1.1, "label": "5000+ units"}], "containerSizes": ["1-gallon"], "minimumGuarantee": null, "productCategories": ["Ornamental Trees"], "territoryPremiums": {}, "seasonalAdjustments": {"Fall": 0.95, "Spring": 1.1, "Summer": 1, "Holiday": 1.2}}, "contractId": "c74106fd-07ce-4954-a79d-7803a80458ca"}	10.81.5.41	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-26 00:00:28.405616
1bde0d20-8a1b-4f10-813e-72edc56abba1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	f75da448-234c-4cad-9d74-345a9a155431	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.232	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 14:27:12.035434
9a6bcd2d-3f7c-42d0-95e6-04e1ebd3f854	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	2ca14560-5640-42de-ad7a-a57b86729dfa	{"updates": {"baseRate": null, "priority": 1, "ruleName": "Aurora Flame Maple - 1-gallon with volume tiers", "ruleType": "formula_based", "description": "1-gallon containers: 1.25% royalty, 1.10% for 5000+ units with seasonal adjustments", "territories": ["Primary", "Secondary"], "volumeTiers": [{"max": 4999, "min": 0, "rate": 1.25, "label": "< 5000 units"}, {"max": null, "min": 5000, "rate": 1.1, "label": "5000+ units"}], "containerSizes": ["1-gallon"], "minimumGuarantee": null, "productCategories": ["Ornamental Trees"], "territoryPremiums": {}, "seasonalAdjustments": {"Fall": 0.95, "Spring": 1.1, "Summer": 1, "Holiday": 1.2}}, "contractId": "f75da448-234c-4cad-9d74-345a9a155431"}	10.81.11.232	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 14:27:58.077285
ae7b3323-3036-4b6e-8289-6bbcdab32079	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "734eac42-b3c7-4ba3-99ef-60dfe5bd7402", "rowsImported": 15}	10.81.11.232	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 14:28:18.224702
c8d31e2f-e7db-4ecc-9ecc-6aa82fb14710	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	f75da448-234c-4cad-9d74-345a9a155431	{"fileName": "35d91765-cc74-478e-9567-9ee2d311bbdd.pdf"}	10.81.11.7	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 16:47:01.85124
7e0d57d5-7f97-4a18-9bab-543067a5e5f4	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	734eac42-b3c7-4ba3-99ef-60dfe5bd7402	{"fileName": "4484d582-30e2-4e76-91bd-83d1bfe26eb3.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 16:47:03.64976
b0060971-98df-4d3d-9044-fe60975c7c24	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	c74106fd-07ce-4954-a79d-7803a80458ca	{"fileName": "37b9be51-3230-4102-9205-492f7e4cb9dd.pdf"}	10.81.11.7	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 16:47:05.609214
03bf61a3-e04c-48ef-a0f6-09b78ed18cff	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	cbbfd3a5-9406-47d2-aeb6-8e86970f5dc8	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.7	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 16:47:20.85915
ac60df05-d6b2-4b55-9b36-b42bec3c9655	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "cbbfd3a5-9406-47d2-aeb6-8e86970f5dc8", "rowsImported": 15}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 16:48:12.359059
89f6bd54-3a8f-49ee-bace-f6bdabe01d03	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	cbbfd3a5-9406-47d2-aeb6-8e86970f5dc8	{"fileName": "5c17e6de-2eed-4483-a33c-2ff56388df1c.pdf"}	10.81.9.225	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 22:36:09.66489
aca4841b-bb9a-4577-b4f5-afe20241e5b6	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	1762d4b6-1475-46a5-8ff7-476480020a5e	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.6.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 22:36:25.288748
3ee0a50c-36e7-444c-aa26-7ca647f5bdb5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	1762d4b6-1475-46a5-8ff7-476480020a5e	{"fileName": "092ed713-0f42-4559-bad5-1d7865f7dce8.pdf"}	10.81.2.64	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 22:51:56.547669
ef6df51d-1cd4-4948-8490-1fb04937bbf4	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	dd0b0554-933e-4f55-83fb-1fafe39f14b5	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.2.64	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 22:52:04.980986
8db6d64f-8658-40ef-befc-adb9bb3e8133	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	dd0b0554-933e-4f55-83fb-1fafe39f14b5	{"fileName": "5d4b0fc7-ef80-4b82-ae67-5c86f7061cc8.pdf"}	10.81.0.54	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 22:56:43.527068
d76531f8-3d0c-44b5-b859-87afb391cc53	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	2db80e5d-6e7a-48a9-8326-337b4d1f8d86	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.0.54	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 22:58:10.567075
d6dcaf22-5fb3-48c6-9e8e-88bf77ccbd02	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	2db80e5d-6e7a-48a9-8326-337b4d1f8d86	{"fileName": "72a24830-1571-4f9e-8964-6e7987b8e2f2.pdf"}	10.81.5.174	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 23:01:34.887452
73dbeb6a-7f2f-4deb-933c-017bbd238b7b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	71de5eed-e38f-4b4a-91a8-d0c35ea4e652	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.2.64	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 23:02:03.90846
1d47b14c-8831-4cbf-b894-3694c7ec1ac5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	20a3d665-3011-4c16-8282-f6e661974cc7	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 23:02:40.943614
d41685b3-07fd-45df-9773-cd03166062fb	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "20a3d665-3011-4c16-8282-f6e661974cc7", "rowsImported": 15}	10.81.9.225	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-27 23:04:56.45658
fc3df291-ec5b-4888-89c6-cfde7abe88df	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	20a3d665-3011-4c16-8282-f6e661974cc7	{"fileName": "4e6e3a42-eca7-48da-84c5-1a9c903057d3.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 22:50:17.939149
df8a2c81-196a-483d-bc23-643c1ca9ec4f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	71de5eed-e38f-4b4a-91a8-d0c35ea4e652	{"fileName": "f75647ad-c0b7-4663-a07c-c450fe855c47.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 22:50:45.199724
99690287-a183-4817-a949-07aa69849ea2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	4f579003-52b4-42eb-9373-f0718b37e2b9	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 22:51:05.374348
63eef34c-6dc4-4075-a387-f12670d70f11	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	67246b1a-f0f9-482a-9116-defe8d3102cc	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 22:51:53.808952
44ae6682-539e-4e66-a21d-221c19662952	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "67246b1a-f0f9-482a-9116-defe8d3102cc", "rowsImported": 15}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 22:52:56.583437
d383c6b6-e7ff-4c25-9960-582b5562ccba	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	67246b1a-f0f9-482a-9116-defe8d3102cc	{"fileName": "e78ccd7d-b956-4bf9-8d70-24a1ea840746.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 22:54:22.340195
f25d583f-9e2a-43cd-b2da-a7c770eb7413	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	4f579003-52b4-42eb-9373-f0718b37e2b9	{"fileName": "38e7f94e-0563-4920-9be4-3308f3cd2e3c.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 22:54:29.686638
d1328d92-ef07-4dd2-a06d-786152a702ea	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	867f97a6-07a1-475b-8014-107b5dfa36ec	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 22:54:57.10687
9731406f-dd44-4b03-9f1e-40b095708b6f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	867f97a6-07a1-475b-8014-107b5dfa36ec	{"fileName": "3fa00a3d-8a57-4655-ae0e-9054e76227c4.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 23:01:12.162335
1b0bd6be-06b4-41d5-8eb7-db8d19aafdc9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	613bdf0b-f9e5-400f-8d15-f4af89e1f942	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-28 23:01:24.575063
eb4209b0-cd1d-41d3-96fa-84f08e46c2f1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	613bdf0b-f9e5-400f-8d15-f4af89e1f942	{"fileName": "6ca8185d-4018-430e-b7fd-053b6b94b0e3.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 14:09:09.041053
b0a832e6-43e7-4ba8-8857-83b0e15e2606	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	6ba10946-4787-4a39-823a-c25386e593f4	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 14:09:15.694643
6f8e2cb9-a421-42be-88ae-72fa441dc1f9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	f2168032-f24c-4fce-bed9-6bb3b1e97f48	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 14:35:48.137431
0878b160-0e15-4065-b4e0-3734b23607d5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "f2168032-f24c-4fce-bed9-6bb3b1e97f48", "rowsImported": 15}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 14:38:17.681704
e48fc549-39ec-4115-ac39-9fa8db58785b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	f2168032-f24c-4fce-bed9-6bb3b1e97f48	{"fileName": "f97451fd-4d5c-42bf-8872-d7bbefc8e467.pdf"}	10.81.0.54	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:03:30.758825
08257863-52ca-4e36-953f-040c1a8fa89d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	865f076a-27b1-45e0-a249-6bf6f6a684ab	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.0.54	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:03:44.751918
0753acbe-7d6a-4f21-968e-469ebf8d4cdc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	865f076a-27b1-45e0-a249-6bf6f6a684ab	{"fileName": "b06d5358-7ba9-48d1-99fd-0211cd3cd529.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:07:56.781945
4b6014ad-70af-4721-b155-73032361e5a3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	fa63cbf4-50f7-4876-82c6-c44787d668aa	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:08:08.001765
6c0c5e76-b4fc-43a6-93dd-f489a8e4b1a6	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	fa63cbf4-50f7-4876-82c6-c44787d668aa	{"fileName": "c7e4c674-8dba-49bb-ac87-8968d08ececd.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:12:05.863706
4bcbc368-0c3c-417a-8bc1-b89c4f18c842	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	8c029a76-16ac-4aa9-8b95-5bc36d865103	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:12:11.57656
530ee0e1-1dec-4e05-ac13-c56fe4ad6c03	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	8c029a76-16ac-4aa9-8b95-5bc36d865103	{"fileName": "8dbf53c5-e810-4e90-a4b2-18f95c10acd7.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:15:46.99805
d58d3e98-1fdf-4d53-84b8-2eddcb942ffe	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	ddd84882-89d5-45a9-a976-13337ddc5550	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:16:00.730382
e7bdc482-68d3-4852-9fd6-3f5fcc7efdaa	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "ddd84882-89d5-45a9-a976-13337ddc5550", "rowsImported": 15}	10.81.11.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:16:56.72754
ba2e154a-fd7d-4316-be62-d32ba2d51b78	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	a3b5d4e8-f19a-4eaa-8d2b-c62ce65b5a48	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:18:07.376382
6b170e09-aca6-430e-a262-e50e2eb20562	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	ddd84882-89d5-45a9-a976-13337ddc5550	{"fileName": "8821d284-7e1f-43c5-a551-4f93317a3cf5.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:22:02.676478
cfa5445a-8e96-4391-b3cc-0e53257418fc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	a3b5d4e8-f19a-4eaa-8d2b-c62ce65b5a48	{"fileName": "04429596-d759-44d0-87ab-58314ade0d60.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:22:04.443667
9dde74ea-b84f-40e1-ba71-d1d267d43e6c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	a0f65611-af32-45d0-b0e7-3b45913c40a5	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:22:13.381935
3a12f079-6203-42ab-bbaa-b4bb035903b3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	18cfa305-c198-462e-af9b-7d29b64e64d1	{"updates": {"baseRate": null, "priority": 1, "ruleName": "Time and Materials not to exceed 40 hours per week", "ruleType": "tiered_pricing", "description": "Cimpleit shall not be liable for any fees resulting from work in excess of the dollar limitation unless the parties otherwise mutually agree in an amendment to the Work Order signed by authorized representatives of each party.", "territories": [], "volumeTiers": [], "containerSizes": [], "minimumGuarantee": null, "productCategories": [], "territoryPremiums": {}, "seasonalAdjustments": {}}, "contractId": "a0f65611-af32-45d0-b0e7-3b45913c40a5"}	10.81.11.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:28:55.376434
7848dd12-1e70-41ff-a3a9-6c4c79662944	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	74c167ea-c6d8-408b-8064-656ac6800caf	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:29:09.407211
f6cdf335-b865-4c49-a3b9-a8a614984463	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	5bf87a1d-116a-41f4-b399-1e77248fc2f4	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.0.54	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:29:42.2226
9a26d43b-b30f-444e-b952-a5c1b4979085	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	6a2c21b7-b29a-4f30-88dc-34799c56afe3	{"fileSize": 383171, "originalName": "Pharmaceutical Patent License & Drug Royalty Agreement.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:30:25.619172
4a60a4cc-117a-496b-916c-e6e995072b74	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "6a2c21b7-b29a-4f30-88dc-34799c56afe3", "rowsImported": 15}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:31:41.86068
683e936a-69a3-4130-b5b2-3a2d1e4d8a18	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	0372e63f-d7b2-48ce-9276-210038b66b64	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.0.54	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:33:02.971714
aa99590b-2c6e-4497-9d52-3fc63d0efeff	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "74c167ea-c6d8-408b-8064-656ac6800caf", "rowsImported": 15}	10.81.11.94	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 15:52:47.39094
181112e6-d982-4673-b470-1a4d83aa5f8e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	a0f65611-af32-45d0-b0e7-3b45913c40a5	{"fileName": "996fd2e0-eb06-49e4-bd33-71c288a7cd56.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:22:42.997619
6d9ef8e7-dec7-4b48-bc4e-d6542e770750	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	74c167ea-c6d8-408b-8064-656ac6800caf	{"fileName": "f5cfb04c-3c74-4dad-a0db-1697f4162250.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:22:52.511309
f4307736-428c-461e-8376-d76ffb9dada1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	0372e63f-d7b2-48ce-9276-210038b66b64	{"fileName": "bec19dfc-b381-455e-b0ff-c27fff8516e7.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:22:54.15041
e4ad4d6b-ef4a-47da-94b8-1a24384e1b57	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	5bf87a1d-116a-41f4-b399-1e77248fc2f4	{"fileName": "73e9bc73-afa4-423e-a69b-cc2840d26c38.pdf"}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:22:56.514876
b4243db9-7934-4593-a7d8-9ff919867261	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	6a2c21b7-b29a-4f30-88dc-34799c56afe3	{"fileName": "8d9e6857-75c1-49f0-ab82-93a8c880599c.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:22:58.202112
f89aed09-aa8b-4da2-94f8-dabdaebfb6e0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	17b66117-03c8-4fc7-9afb-b02ff86cddfd	{"fileSize": 5000, "originalName": "invoice-detailed-Calculation-10-29-2025.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:23:11.633927
0bb2f0b4-3339-45c5-9654-b4d1ec56edcb	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	17b66117-03c8-4fc7-9afb-b02ff86cddfd	{"fileName": "bbbec95f-cbd9-499f-a04b-76c14ce2bf1b.pdf"}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:24:21.15615
eab55ffa-135e-4479-b4ca-972942ebc541	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	b61073b0-77a6-4369-b992-dfbee3b347e3	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:24:29.292533
bc905788-8a21-420f-beac-c330e80eab75	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	b61073b0-77a6-4369-b992-dfbee3b347e3	{"fileName": "abc20d23-e581-4b77-a3c8-195d333ace06.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:32:28.560635
a294c35b-5561-4183-a066-f8efe3a997a1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	4bd24b59-955a-4756-bd79-314afeef896d	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:32:35.907033
30830f1d-7144-44ca-af47-625714dcc246	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	4604e6a8-cf56-43b9-a037-1734cab74cfa	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:33:09.80059
31c01862-25f9-4d09-89d8-d24034c46e27	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	4604e6a8-cf56-43b9-a037-1734cab74cfa	{"fileName": "cf0c3db8-a4fd-45cc-ae00-db50151f6f0e.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:36:01.03312
51b49a4e-4062-4497-93c7-78a672677327	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	4bd24b59-955a-4756-bd79-314afeef896d	{"fileName": "234e186f-3e87-4445-aa51-e25eaf7ef26b.pdf"}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:36:03.158702
211e869f-0202-4270-b7ef-468c2110fef7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	dc72732f-71d8-4398-a9b7-682fb363b6ed	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:36:14.871021
2fe8e8e3-ad16-4c6a-b319-acd33726de5e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	dc72732f-71d8-4398-a9b7-682fb363b6ed	{"fileName": "2ac7a343-d606-4bd1-baff-6164b27426bb.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:47:31.841876
b1b42fc2-f778-46cb-89ba-0a54953456de	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	b2e811d0-c29b-42f8-9e9d-c3123962493b	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.2.190	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:47:44.597951
1e722378-e41a-486c-a815-1ae42f5c8504	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	b2e811d0-c29b-42f8-9e9d-c3123962493b	{"fileName": "93a0a94c-49ea-4a3e-8bdf-49cb2b288dee.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:52:19.837752
f4f808d7-a1ad-4436-9b10-c732838050c0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	07044842-cc6a-455c-8a1a-e6871278f722	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:52:36.238543
d313f208-a0b5-4d5c-a4c5-67ea088d9a60	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "07044842-cc6a-455c-8a1a-e6871278f722", "rowsImported": 15}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:53:16.042434
8dd86f71-af29-4e16-bfa4-cb8677ae593e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	07044842-cc6a-455c-8a1a-e6871278f722	{"fileName": "7b993ac7-9c7d-4836-854d-04d571c8dee9.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:56:51.60969
7b06ff9c-3d5d-4400-baec-6475d0bc0837	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	0f4f7e03-fa76-44f8-b792-edfd00b3bded	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:57:00.20107
3385951f-2a7c-48ba-924a-59aea6e6fa3a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "0f4f7e03-fa76-44f8-b792-edfd00b3bded", "rowsImported": 15}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 20:57:52.106971
2bcd340b-f5d6-4e29-be02-e43780db6e21	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	0f4f7e03-fa76-44f8-b792-edfd00b3bded	{"fileName": "ed0e4811-83d2-4a2d-a18b-87e7214b5561.pdf"}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 21:54:08.268542
37ed3034-8cd7-442f-a689-fabb9f623c76	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	767c99ad-2ee5-4fe7-986f-1c3b2a80e5a3	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 21:54:18.371314
47e63d2a-6223-40d5-85d2-73659d8f367f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "767c99ad-2ee5-4fe7-986f-1c3b2a80e5a3", "rowsImported": 15}	10.81.0.54	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 21:57:20.386341
e1a3028b-a9c9-46d7-bdf9-96993d3a439c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	925d8d56-d978-4628-8bd9-f1301c82f747	{"updates": {"baseRate": null, "priority": 6, "ruleName": "Initial License Fee", "ruleType": "fixed_price", "description": "One-time license fee", "territories": [], "volumeTiers": [], "containerSizes": ["General"], "minimumGuarantee": null, "productCategories": ["General"], "territoryPremiums": {}, "seasonalAdjustments": {}}, "contractId": "767c99ad-2ee5-4fe7-986f-1c3b2a80e5a3"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 21:58:42.256664
67411c15-6b26-4834-b288-1ba52810dfc3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	7062966c-b72e-46a9-b0f1-0445940e480f	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:00:54.898187
a3101935-91bb-43dd-adc7-565f16aec8a3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	7062966c-b72e-46a9-b0f1-0445940e480f	{"fileName": "dacc462a-eaac-42d1-8b04-511ad53d3ba4.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:05:43.293957
53a162c0-7d7e-4663-9466-496a3f4842b2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	767c99ad-2ee5-4fe7-986f-1c3b2a80e5a3	{"fileName": "8f5fbda6-8549-49f8-85c6-5f2dcf7d8d70.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:05:45.81975
57cbd7f8-8910-4c88-9d41-089ab4aab7cd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	9ad78132-1898-4541-8cc4-30a82798f395	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:05:57.478662
23e809ac-62fe-4236-9c6a-252538245e04	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	9ad78132-1898-4541-8cc4-30a82798f395	{"fileName": "2de54715-a29f-4f6a-bd87-bc7e71fe2462.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:09:18.112432
54d5dd91-d847-49a7-9a61-a43c3609415a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	24096f39-7b77-4eef-9cd1-495898d6631a	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:09:26.134565
6479cd6b-f73a-47e2-9114-568fc3bb940c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	24096f39-7b77-4eef-9cd1-495898d6631a	{"fileName": "1a964f8b-7679-4af0-8539-305e38c3f916.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:14:18.854438
2728c1f2-5d14-417f-a4f6-ab731062a179	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	8f1f9300-98ac-4f0d-8093-3b8a197d73f6	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:14:28.103103
2cf29e09-9dcb-46d5-83b3-b849e75a6945	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	8cd86db8-900a-429f-a705-6889d5a9552e	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:15:00.702076
77b11d51-881e-40c2-b85a-0daba0f03b71	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "8cd86db8-900a-429f-a705-6889d5a9552e", "rowsImported": 15}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:16:11.361899
da8922df-21b4-460b-a370-ff43aec652ff	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "8cd86db8-900a-429f-a705-6889d5a9552e", "rowsImported": 15}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:16:46.390518
68ff6444-f221-4cac-ac21-48b513fc0dd3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	8f1f9300-98ac-4f0d-8093-3b8a197d73f6	{"fileName": "a3489fee-b6ac-458c-8509-8db4909fec31.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:22:13.137249
e11cf063-1dbf-482b-96c2-ebebf2b012dd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	35d70548-72c1-49c2-9ee1-41f2ffda4718	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:22:23.534859
c7b4adcc-b9ea-4215-8946-81eb4bb7473d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	b2d13146-a953-4390-8427-ffc1fe241f05	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:23:17.89606
637ec687-9716-43e5-b568-448ad9e42c42	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	b2d13146-a953-4390-8427-ffc1fe241f05	{"fileName": "ca03f70b-489b-4600-8c29-5db321090328.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:24:22.615452
2850eb8b-806e-4063-bfff-b4c3577323c5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	35d70548-72c1-49c2-9ee1-41f2ffda4718	{"fileName": "1e4acdbc-e133-4277-bafd-565816d483b0.pdf"}	10.81.0.54	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:24:25.445064
73d98cf5-4a29-41c5-8fff-fc7e94c04f04	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	8cd86db8-900a-429f-a705-6889d5a9552e	{"fileName": "9d9c9ffe-0fb3-4d5e-b993-152edce95c90.pdf"}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:24:27.46791
b270d852-b812-4f79-a065-35b472aa7d9a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	88fd4ea8-89ea-432e-b937-d6b2ab6936ea	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.9.43	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-29 22:24:37.435328
cb1a0545-613c-4768-82bc-43085709d97a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	88fd4ea8-89ea-432e-b937-d6b2ab6936ea	{"fileName": "62578f3f-015c-4f91-aaed-529b479fdc7c.pdf"}	10.81.9.100	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 13:57:30.28692
212edc4b-f4b0-4968-8886-0a69425c3b7b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	c062650a-a329-422d-abe9-455328b324eb	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.9.100	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 13:57:46.399207
4623eede-c4ef-4982-bde2-e2b05cbca026	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "c062650a-a329-422d-abe9-455328b324eb", "rowsImported": 15}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 13:58:18.423743
1d48c5b8-497f-4b6d-ac35-6bd168f44bd9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.75, "sourcesCount": 3}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 13:59:25.470427
84f2518d-ae49-4dae-bff2-506ad4731144	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Are there any volume discounts?", "confidence": 0.75, "sourcesCount": 3}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 13:59:30.677971
24e1ecfb-e06b-4a22-a270-e1be27c75071	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0.75, "sourcesCount": 3}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:00:15.155049
fb1e40d9-dc51-4798-a889-391152c7e6c0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	bf1d1078-0f97-451d-aef3-241131fed26d	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:01:03.792796
dda6f426-4f6f-4b9f-9856-c3c1c12ceac4	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	7ba6ddd1-8507-45ae-b22b-74c3780b8731	{"fileSize": 383171, "originalName": "Pharmaceutical Patent License & Drug Royalty Agreement.pdf"}	10.81.9.100	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:02:01.755469
4e4fb981-e4c2-4156-8cfd-9282dbbfc4ba	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	11b415a7-4561-4e28-9e52-da9347a8a098	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:02:39.655402
f75e1760-4533-4735-b236-dcfae1281bea	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	ef889289-8b3e-46a5-be90-abf33df6c6af	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:03:43.660126
a0caabb5-b5e4-4cf0-b3ef-ace0378dc9c3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	ef889289-8b3e-46a5-be90-abf33df6c6af	{"question": "what is hourly rate", "confidence": 0.75, "sourcesCount": 2}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:05:31.032148
b1180aad-6349-43fe-8813-5b091db8204e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	ef889289-8b3e-46a5-be90-abf33df6c6af	{"question": "80 dollors is from which document", "confidence": 0.75, "sourcesCount": 2}	10.81.14.120	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:06:23.332346
59e2ce49-84c5-4029-9612-35bc03527c0a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	c062650a-a329-422d-abe9-455328b324eb	{"fileName": "0af8a04a-ac63-4a2f-b3e4-3afc67829104.pdf"}	10.81.11.144	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:35:52.713573
64b025af-7d33-4b9b-a777-468b3dd8b057	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	bf1d1078-0f97-451d-aef3-241131fed26d	{"fileName": "20904e81-8591-469d-86c4-5ff69c5a2e97.pdf"}	10.81.9.100	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:35:54.730211
92c6c752-aa9e-435c-b149-907c30e85421	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	7ba6ddd1-8507-45ae-b22b-74c3780b8731	{"fileName": "75da7b41-2ecd-4050-ba7d-8f4300f2b2ee.pdf"}	10.81.11.144	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:35:56.97118
ff79106d-0c6d-4b06-b41d-e540f3a45aae	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	11b415a7-4561-4e28-9e52-da9347a8a098	{"fileName": "49f899d4-0c88-4166-9226-8b8fe546bf1a.pdf"}	10.81.9.100	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:35:58.592911
7797c990-6361-4220-8931-49c9e676ffbd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	ef889289-8b3e-46a5-be90-abf33df6c6af	{"fileName": "9c748098-b3ce-4b1c-9099-d9ae2c53afd5.pdf"}	10.81.11.144	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:36:00.630193
4894ae57-8703-4a40-8ffe-1c437cc339e2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	95ba4136-06b4-46fa-9f9c-f6a72956d21a	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.11.144	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:38:12.260807
cf86d3ca-e847-4ba3-9fc7-4725c05e6659	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	95ba4136-06b4-46fa-9f9c-f6a72956d21a	{"fileName": "2242273b-3679-4622-9c09-a97321ff460f.pdf"}	10.81.5.197	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:41:47.250345
3c3e4409-1c78-4bb6-a8e5-545b5f016862	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	aab08e43-d813-463e-b961-9aa440ecb759	{"fileSize": 408974, "originalName": "Master Subcontractor Agreement -Kmlnrao.pdf"}	10.81.2.15	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-30 14:41:58.406111
ede83081-e975-4e94-a596-5a39477496c0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	c7c99720-a838-4da9-98f6-2ff43a22dac1	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.203	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 16:01:01.32587
06bbdaf4-e2da-49b9-b185-6cd555d1a1f0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "c7c99720-a838-4da9-98f6-2ff43a22dac1", "rowsImported": 15}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 16:01:48.172859
bc7c96c2-4c40-478b-b9ee-ee6a878aba49	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	aab08e43-d813-463e-b961-9aa440ecb759	{"fileName": "d0d5988e-88b4-48df-b261-4fa5afa456b6.pdf"}	10.81.3.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 20:05:22.412158
a6381287-6601-4c6d-9b1a-f4b9a8d2da24	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	c7c99720-a838-4da9-98f6-2ff43a22dac1	{"fileName": "95b3de73-59fd-4203-85dd-f7f14285c25b.pdf"}	10.81.6.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 20:05:24.314947
a1e73b3a-1c33-4904-a495-e22c070e2c66	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	339cd02d-f2d7-41e8-8d78-ed6f1bbc3204	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.6.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 20:05:36.812327
254a3c51-ab86-4b1a-b98e-e2e45e7d5529	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	339cd02d-f2d7-41e8-8d78-ed6f1bbc3204	{"fileName": "c441229a-6d5c-4b76-a4ee-bde53c5c5e97.pdf"}	10.81.9.133	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 20:37:38.67539
8fc2e768-d66b-4651-a2e6-aee60ee4ca96	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	3bd0b138-dda8-436f-a777-2db2752be9e8	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.7.186	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 20:37:53.468707
209ade75-10bb-4790-b02e-688433866d3b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "3bd0b138-dda8-436f-a777-2db2752be9e8", "rowsImported": 15}	10.81.6.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 20:39:25.765268
f95478d8-80e1-4c9d-8ee3-8427ff375258	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	3bd0b138-dda8-436f-a777-2db2752be9e8	{"fileName": "80f6c949-4156-4de6-bbcf-04b8ed8d84c1.pdf"}	10.81.6.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 21:13:44.414103
7b8b32c6-d3a4-4fdb-8d5a-47b88627d067	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	1f30604b-3bf2-4f93-a590-34e4d24b6dd5	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.6.36	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 21:13:55.21203
2f554095-0114-4819-8f57-556a6a74c9d9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "1f30604b-3bf2-4f93-a590-34e4d24b6dd5", "rowsImported": 15}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 21:14:23.215526
4182599e-80cf-45d1-877f-f644157d6d35	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	1f30604b-3bf2-4f93-a590-34e4d24b6dd5	{"fileName": "9bbf5bef-48d5-4420-afc5-72d8f1d58171.pdf"}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 21:16:00.174429
eb89203f-9136-45c8-a221-4a54174a0969	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	3d077e64-5962-409c-a5e3-352bae62d547	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 21:16:06.853299
a7124111-b53b-4dda-8a0f-f8db16eb1e98	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "3d077e64-5962-409c-a5e3-352bae62d547", "rowsImported": 15}	10.81.12.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-10-31 21:16:42.421562
5a1633bd-7956-4dc3-b9ef-461c68632e23	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	3d077e64-5962-409c-a5e3-352bae62d547	{"fileName": "9c459527-c3e2-432a-8ba2-e02a6aba961e.pdf"}	10.81.0.105	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 02:15:42.552393
64c4d399-cc30-4b67-b810-4abfac522350	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	76d43ecb-c254-4734-b805-fa42aa81c599	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.0.105	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 02:16:10.060879
ae4a532e-6504-473c-9cc9-22ef19a07613	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "76d43ecb-c254-4734-b805-fa42aa81c599", "rowsImported": 15}	10.81.2.24	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 02:38:36.459853
4da2d5b0-5604-4062-b634-7e39cad1e3b9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	76d43ecb-c254-4734-b805-fa42aa81c599	{"fileName": "9505ed62-4e04-4bf6-bd03-f305a455bf05.pdf"}	10.81.5.103	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 16:36:45.759084
4c955cac-28e9-4be0-af4e-577ab7a584de	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	d08cc4c9-4c16-4fb9-b896-3be9368d9b98	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.9.154	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 16:37:17.528407
beecf982-faa3-4bcc-9653-d9e45e8b4987	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	d08cc4c9-4c16-4fb9-b896-3be9368d9b98	{"changes": "sdfdsf"}	10.81.12.227	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 16:38:55.713532
e3678287-99ab-4351-a97f-3d1af992f41c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	d08cc4c9-4c16-4fb9-b896-3be9368d9b98	{"fileName": "9a2d49ff-6292-42d4-9a54-6979249d6a95.pdf"}	10.81.12.221	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 16:53:31.688226
612ad041-d508-416b-bb44-5ed050cd399e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	5c3f2946-ad17-4f40-8544-0d234f50f55a	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.225	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 16:53:46.340142
bdd3e034-4f82-4b14-8c54-eefd16b664fd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	5c3f2946-ad17-4f40-8544-0d234f50f55a	{"fileName": "b98315f1-636b-457b-a0a5-2acf4a1e8b9b.pdf"}	10.81.12.227	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 16:57:48.323533
0a73b73b-9b63-4002-b0df-ae5e88aa0cbc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.225	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 16:57:57.065613
707f0740-2cb9-40a4-b718-384c1514850f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"changes": "test"}	10.81.5.103	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 17:42:39.638039
7ced6f9d-4c8c-4986-be74-6e9be765d0e1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	fe539b52-9359-4525-8ea5-586a1afb3213	\N	10.81.12.227	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 17:42:49.180589
58f30149-02a3-4952-a02d-dc4824f762f8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"changes": "test"}	10.81.12.221	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:13:12.561725
d545183c-40cc-4c5c-a792-52272cf7809b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	fe539b52-9359-4525-8ea5-586a1afb3213	\N	10.81.11.249	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:13:16.52155
19c5a571-2ef4-4a10-b829-2e9c2053ba3e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_approved	contract_version	7381b7cc-d01f-4b92-82e1-070d227709a7	{"notes": "kkk", "approver": "admin"}	10.81.7.139	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:44:00.376151
51093af1-f900-4d8c-89c8-7199cb2b26aa	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"changes": "Change License Type"}	10.81.2.24	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:52:47.891875
6abeb9f1-8255-4ee6-a754-2947ff4860bc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	fe539b52-9359-4525-8ea5-586a1afb3213	\N	10.81.1.14	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:53:18.130632
32329998-9dd7-4cdb-8c47-df0db83acc27	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_approved	contract_version	5fe66b7b-78cf-4a51-aeae-242631e8947f	{"approver": "admin"}	10.81.2.24	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:53:30.852961
8b9eb3cc-0dfe-4086-b9aa-be4f9ab6de69	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_approved	contract_version	00c194b8-f354-4472-936d-5ea5ca67cc3e	{"approver": "admin"}	10.81.1.14	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:53:47.366154
6f3fa714-780b-477e-83a6-50cab6d613c1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"changes": "Changed to Sales Agreement"}	10.81.2.24	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:54:23.997854
d91b0496-5a66-490b-848b-fed0c1c2c641	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	fe539b52-9359-4525-8ea5-586a1afb3213	\N	10.81.1.14	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:54:27.059511
788fb327-fb62-4fd5-8fbe-314ca91e2363	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_approved	contract_version	ead2453e-382d-4134-bfa1-981f24040821	{"approver": "admin"}	10.81.2.24	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 18:54:43.762174
48230b08-3513-482c-b505-471a4e036e1b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"changes": "modified"}	10.81.11.225	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 19:06:46.425957
c8c1941e-e8ca-4b31-8fa3-878488548be5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	fe539b52-9359-4525-8ea5-586a1afb3213	\N	10.81.4.174	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 19:06:55.553481
c057c141-4284-458f-818d-6ae132c578a4	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_rejected	contract_version	e438de74-c67b-4d9d-8290-f7338efd7967	{"notes": "sdssfd", "approver": "admin"}	10.81.11.225	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 19:07:08.809914
13285646-00d5-458b-b6ba-fa335483c0ab	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"changes": "Added Organization Name"}	10.81.4.174	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 19:07:31.432636
aa4fc567-b3de-4628-a794-7387463e4af1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	fe539b52-9359-4525-8ea5-586a1afb3213	\N	10.81.11.225	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 19:07:34.829419
cfd5b172-3588-4083-ac28-1807d3926f8f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"changes": "123"}	10.81.4.174	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 19:10:32.652038
cf238cc5-bbc4-46fc-aff1-a73a2081b847	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	fe539b52-9359-4525-8ea5-586a1afb3213	\N	10.81.6.95	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 19:10:36.633805
a676b126-1078-430a-ae08-5e3af26fdc9d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_approved	contract_version	2535e847-e70d-4cc0-a13b-58f916d9e318	{"approver": "admin"}	10.81.6.95	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-03 19:10:51.238956
c907d115-0af3-4e66-a0a2-7a00eeda3acc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	fe539b52-9359-4525-8ea5-586a1afb3213	{"fileName": "263efe74-208e-47e5-b94f-4e3925078648.pdf"}	10.81.11.17	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:13:41.449828
3ab9aa33-546e-4130-81a5-a08c12ff07a8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	b050bfe2-28c0-4d4f-9ef6-c65ab872723f	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.17	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:13:50.2694
53607824-a6d3-4a92-9770-20f321a5293f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	b050bfe2-28c0-4d4f-9ef6-c65ab872723f	{"fileName": "7fb8de79-7f59-4a24-9e46-5106db868dd5.pdf"}	10.81.12.230	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:18:13.51025
fcfba110-c54a-4ec4-a5c3-f8a7e652ab09	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	92e46b64-067c-4b90-b2b2-28488321ab3c	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.12.230	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:18:21.467424
515f5e3d-4034-4350-8687-dfe28c3d4c8a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 3}	10.81.5.75	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:18:46.499456
0ad53d9d-74b6-4a7d-b219-4b45bac173e9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0.75, "sourcesCount": 3}	10.81.5.75	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:18:50.003472
e76c1e3d-9662-471c-8409-2cfd2826bb3c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What products are included?", "confidence": 0.75, "sourcesCount": 3}	10.81.5.75	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:19:03.085412
d446b13c-ea06-42ac-aab2-1d8279b5cee0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 3}	10.81.5.75	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:19:27.737469
abfa9f51-42bb-43cd-b2da-5ec6cc002c34	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Are there any volume discounts?", "confidence": 0.75, "sourcesCount": 3}	10.81.5.75	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-04 15:19:42.702733
7a3da5da-965a-498a-beb6-832ed4b893ce	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	92e46b64-067c-4b90-b2b2-28488321ab3c	{"fileName": "fb613b05-40e7-4059-bb66-6afee850ec69.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 18:32:48.599025
93fe6b21-ab49-4150-85ae-353d2d967a1c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 18:33:03.477056
f9dac49c-06ee-415c-9cec-d3bfbe0e34a6	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "8964a14e-ca38-4178-b92e-1057289213e6", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.4.20	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 18:37:25.978084
e889a278-1f23-418d-9e1b-d559d2c05936	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.75, "sourcesCount": 3}	10.81.9.113	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 18:38:02.135659
e7739fba-0953-4ed6-82d7-e4e5f213718f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_erp_matching	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"enabled": true, "message": "ERP semantic matching enabled"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 18:40:55.850213
3eefbb97-553a-4411-bfd7-5178674f4d77	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_erp_matching	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"enabled": false, "message": "ERP semantic matching disabled"}	10.81.2.102	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 18:41:01.724966
5b79018e-7650-4887-bf61-60b925102bd0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_erp_matching	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"enabled": true, "message": "ERP semantic matching enabled"}	10.81.9.113	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 19:03:05.386012
20a01203-f16e-493a-acfe-5ac5b51e5b0f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_erp_matching	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"enabled": false, "message": "ERP semantic matching disabled"}	10.81.7.61	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 19:14:36.566421
a0563bf5-1cb7-48dd-b9ec-4c966730ad1f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_erp_matching	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"enabled": true, "message": "ERP semantic matching enabled"}	10.81.10.24	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-05 19:14:41.350068
38b3d461-bc2c-496e-a01a-56328adbcff7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_erp_matching	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"enabled": false, "message": "ERP semantic matching disabled"}	10.81.11.40	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:03:12.256298
9587dacc-86d3-4a9f-9303-29e57a8f16e1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_erp_matching	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"enabled": true, "message": "ERP semantic matching enabled"}	10.81.11.40	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:03:13.951172
f17ab909-e4ee-495e-a4ea-2e9b904ff3fa	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	8964a14e-ca38-4178-b92e-1057289213e6	{"fileName": "b4f51ef5-2d46-4c4d-b46a-8b0554cad32d.pdf"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:31:28.998808
ecfbb1a8-30ea-40b1-9d26-c19cec0cf063	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	4f9eba41-310d-46c3-b929-0aaaeb613fbd	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:31:44.123867
8808f3d3-b98f-4a3b-a92b-e6a52d8bc767	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "4f9eba41-310d-46c3-b929-0aaaeb613fbd", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.12.86	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:32:30.834399
e604e252-accf-449c-866a-03a78fbb112d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	4f9eba41-310d-46c3-b929-0aaaeb613fbd	{"fileName": "901d531a-3036-42b8-87ff-3fdf0e00b83d.pdf"}	10.81.4.85	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:38:06.910589
31feb6d3-d8f5-4bf5-a7c9-cb56b756ad91	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	174552dd-8819-4a49-995e-7ab5ae4395b2	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.4.85	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:38:26.538145
9d20a80b-8048-41cd-bb0b-d733c6003753	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_erp_matching	contract	174552dd-8819-4a49-995e-7ab5ae4395b2	{"enabled": true, "message": "ERP semantic matching enabled"}	10.81.11.40	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:38:57.656441
1b64ee5a-641a-48c3-96e2-d1a2227847fa	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	174552dd-8819-4a49-995e-7ab5ae4395b2	{"changes": "Added Organisation Name"}	10.81.11.40	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:39:36.85388
aa2e80d3-359e-4713-8e9a-e31903fe25a3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	174552dd-8819-4a49-995e-7ab5ae4395b2	\N	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:39:42.287803
6f1cc1c1-b9dd-40d8-95ae-2f3cb467540f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_approved	contract_version	a0f0a809-4e58-408f-a5a5-637001d545dd	{"notes": "sfsdfsdf", "approver": "admin"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 17:39:49.32391
0408c620-9c58-4183-b847-b67ace3be440	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.75, "sourcesCount": 3}	10.81.10.24	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 23:09:20.07796
c8525f8a-217c-481f-a68c-989f17ea969d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	174552dd-8819-4a49-995e-7ab5ae4395b2	{"fileName": "5a3fe1c8-2700-4425-98c0-6ab4c649f7ca.pdf"}	10.81.11.40	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 23:11:26.143379
6c98deb5-f8f2-480e-bed1-f7130f5a130b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	122c05f8-27fe-4c6d-aa0c-17b4685a560e	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.40	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 23:11:40.702141
6254c1f2-ca09-4b47-b833-47f8cef288fb	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	3a75b89f-84ec-408f-9414-ff151fcd79e2	{"fileName": "6fde8497-bdf9-49ca-9cec-7cd5a0003f32.pdf"}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:12:01.04314
1327e312-b60e-4fc7-8585-6b4ae6564e80	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "122c05f8-27fe-4c6d-aa0c-17b4685a560e", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.7.84	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 23:12:11.857015
6cafe0f1-8de3-43ce-90b5-f42c537af7a8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "122c05f8-27fe-4c6d-aa0c-17b4685a560e", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.10.24	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-06 23:16:51.333324
d7a1a98c-4b44-41d6-b02b-11c4ef1c7bd8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	create_company	company	bc2cc673-47a4-4c39-94b6-235553fe0576	{"companyName": "ZZZ"}	10.81.4.225	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-10 22:47:06.998606
84cf765f-f818-4197-95a6-e98943472fa2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_company	company	bc2cc673-47a4-4c39-94b6-235553fe0576	{}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-10 23:01:42.941087
ecf4e63a-fe73-463f-afeb-129d31593f07	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_company	company	cmp-001	{"changes": {"city": "San Francisco", "county": "", "country": "USA", "address1": "", "address2": "", "address3": "", "companyName": "Acme Corporationf", "companyDescr": "Global technology and innovation leader", "contactEmail": "john.smith@acmecorp.com", "contactPhone": "+1-415-555-0100", "contactPerson": "John Smith", "stateProvince": "California", "contactPreference": ""}}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-10 23:33:08.255815
42499b75-f4c0-4437-9c3e-03ffad9b27a9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_company	company	cmp-002	{"changes": {"city": "Austin", "county": "", "country": "USA", "address1": "", "address2": "", "address3": "", "companyName": "TechVentures Inc", "companyDescr": "Innovative software solutions provider", "contactEmail": "sarah.j@techventures.com", "contactPhone": "+1-512-555-0200", "contactPerson": "Sarah Johnson", "stateProvince": "Texas", "contactPreference": ""}}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-10 23:46:56.134522
f5383561-112b-45e0-a413-6b56fc6a03ae	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	create_company	company	e8dcc53f-8f59-4732-baae-224859cb66b7	{"companyName": "Test Company"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-10 23:47:14.935089
4b10f553-f41e-437b-a154-05ec91c90e7b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	122c05f8-27fe-4c6d-aa0c-17b4685a560e	{"fileName": "c9478f25-c04b-4751-9f47-cbaab14208b9.pdf"}	10.81.12.236	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 18:45:43.947456
26170cc4-7412-448b-b623-b7325021a799	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	9b18579b-7e31-448a-9dba-65727c217432	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.12.236	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 18:46:08.033765
5550cb80-1d49-456e-aa53-aa28df999e6b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "sample_sales_data.csv", "contractId": "9b18579b-7e31-448a-9dba-65727c217432", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.12.236	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 18:47:05.095332
c65e43ea-cd71-4471-9f85-c4571e68e38f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	dynamic_extraction_triggered	contract	9b18579b-7e31-448a-9dba-65727c217432	\N	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 19:02:03.172315
f1ed1c73-ef85-44ff-8248-16350fbec117	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	134be005-8b85-436e-bee3-5880e7796a20	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:44:02.705403
18861d68-6651-4d4d-b24c-88e7df3661e8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "electronics_license_sales_sample.csv", "contractId": "134be005-8b85-436e-bee3-5880e7796a20", "rowsImported": 0, "erpMatchingEnabled": false}	10.81.12.236	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:45:33.982643
639d6c07-1fe8-48a7-94dc-55ab87fa918a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.75, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:48:49.035229
d80de6b9-42ff-4c70-9307-ec7c21efa03f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0.75, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:48:56.775287
7ebe96f4-a0a4-469c-995f-16d08c685c39	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What products are included?", "confidence": 0.75, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:49:08.051848
fe6d8695-db9d-4df2-9be1-4f2f96d9da3f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	134be005-8b85-436e-bee3-5880e7796a20	{"fileName": "a8ede4c5-1edb-4999-8372-965a2ddcd334.pdf"}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:54:15.006722
b7a1aef5-a440-4bb1-afb3-6463a9a1dfdd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	9b18579b-7e31-448a-9dba-65727c217432	{"fileName": "3a4e4e21-86fa-46ea-ad4c-1e8065400f4c.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:54:16.800168
274dec50-9ead-4555-b374-ae3729239bb9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	f6482c91-c287-4d6b-8a75-70a08f5f7faf	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:54:29.438521
b8ab86de-4e41-4e21-ad66-34ad3bfbcd9e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "electronics_license_sales_sample.csv", "contractId": "f6482c91-c287-4d6b-8a75-70a08f5f7faf", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:55:52.057809
da9bcd7c-5ec9-4920-9003-fd699189bf11	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "electronics_license_sales_sample.csv", "contractId": "f6482c91-c287-4d6b-8a75-70a08f5f7faf", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 20:56:27.333824
a987b6c4-4919-4e7f-8854-6a4a099d03ea	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "what the calulations here ", "confidence": 0.5783736589648333, "sourcesCount": 3}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:01:12.067338
9ac1ce8b-0580-48d2-bbdb-a17ba8a7c703	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.8, "sourcesCount": 3}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:01:27.91508
01a473fe-0e0f-4439-b674-22ac0a946361	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Are there any volume discounts?", "confidence": 0.8, "sourcesCount": 3}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:01:31.799407
af4fd335-1e56-48ff-8692-234df0d1d4b5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What products are included?", "confidence": 0.8, "sourcesCount": 3}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:01:35.86595
16467b00-48d1-4306-a5ea-e6223d6e9050	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	84cb578e-953c-437f-9521-47be506c1d34	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.12.236	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:07:17.495479
ff83964f-8c1e-4d67-9491-5707398f4eca	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "manufacturing_license_sales_sample.csv", "contractId": "84cb578e-953c-437f-9521-47be506c1d34", "rowsImported": 0, "erpMatchingEnabled": false}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:08:34.398289
54a85e4e-c0a3-43bb-b33b-b342ccb27ac0	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	f6482c91-c287-4d6b-8a75-70a08f5f7faf	{"fileName": "e754f9b7-2429-49da-85cc-4c206fec7f5d.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:10:37.502835
5e0b9ebe-c8fe-4a92-905c-79e50efe40c8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	84cb578e-953c-437f-9521-47be506c1d34	{"fileName": "df669b85-9fbe-4f2d-94e1-918bb3a18d67.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:10:39.179717
de88fd69-891e-491a-ad0a-939f4c335e87	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	dd3b989c-7984-4366-ae6e-74b4e753d830	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:11:23.896847
df5c2036-e7d8-4c5e-81cd-884f5e7123dd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "manufacturing_license_sales_sample.csv", "contractId": "dd3b989c-7984-4366-ae6e-74b4e753d830", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:12:19.343014
25f04a59-bd25-4822-a772-66c89308e289	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	6040019d-8a97-48b6-9453-a5abdd9273dd	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.12.236	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:18:37.259387
3699a71b-e223-44d8-8b01-1dd65dbc3103	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "manufacturing_license_sales_sample (1).csv", "contractId": "6040019d-8a97-48b6-9453-a5abdd9273dd", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.12.236	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:19:37.064161
986bdfd7-d1a4-4597-936c-fe4962243e44	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	6040019d-8a97-48b6-9453-a5abdd9273dd	{"fileName": "eae22619-09d3-488c-850f-c542f79da805.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:19:51.416224
0262518a-7d21-4190-b867-101e7c5adf7d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	dd3b989c-7984-4366-ae6e-74b4e753d830	{"fileName": "7f1de59d-6f7e-45a0-a38b-04ef29bbe2dd.pdf"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:19:53.265312
fc421844-b5c4-400e-bd5e-23e893137935	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	c7394e5f-b4fc-4c04-b6fe-927b4e75aa04	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:21:04.245161
1057cb61-75a3-4794-9d3f-caf1fb3c5f36	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "manufacturing_license_sales_sample (2).csv", "contractId": "c7394e5f-b4fc-4c04-b6fe-927b4e75aa04", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:22:30.138751
5b40e979-9629-4019-836d-c33f7112fe40	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	c7394e5f-b4fc-4c04-b6fe-927b4e75aa04	{"fileName": "f874789e-d7bf-44d6-9ac6-0b470892f2fc.pdf"}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:25:29.308042
5b77e1ca-1c9b-4d05-96f8-5c460a336d37	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	0bac2720-ec6d-4d62-a463-8471f318ad0a	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:25:44.573756
8a477757-10b9-4ffb-95d1-2ee6a08986f5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "UPDATED_manufacturing_sales_MATCHES_RULES.csv", "contractId": "0bac2720-ec6d-4d62-a463-8471f318ad0a", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:26:33.896285
f0fa641e-0661-4449-9a5d-9551824593b2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Technology_manufacturing_sales.csv", "contractId": "0bac2720-ec6d-4d62-a463-8471f318ad0a", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.12.237	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:36:57.537623
5799e252-f37c-42e1-af65-c6ce5a469ba4	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	0bac2720-ec6d-4d62-a463-8471f318ad0a	{"fileName": "4221934d-3b80-4c78-91d5-3ca99f25684f.pdf"}	10.81.4.12	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:37:46.954136
2d5e2a1f-bc09-4d80-a34f-921c0d3a242c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	a929494b-7fd9-4894-b81a-76eebde6bb9c	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.12.237	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:42:21.432813
5e64a75f-2148-48cf-8884-6cc42edda4be	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Plant_sample_sales_data.csv", "contractId": "a929494b-7fd9-4894-b81a-76eebde6bb9c", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:44:01.205316
cadcb14c-8cd0-495d-9455-f65626ae3be9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	2c275de2-28f0-43a0-ab9c-34d9649275f6	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:44:34.135808
a5aabb11-63ba-403c-9d6a-a8ae110d356b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Technology_manufacturing_sales.csv", "contractId": "2c275de2-28f0-43a0-ab9c-34d9649275f6", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:45:23.508111
a5cd30a6-c459-4cb8-9059-1e1d89253818	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	aee97110-7349-4f91-82e2-d4b2ffa35096	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:45:55.428213
60e184e8-0e75-4fe4-bd74-214a73e65a54	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	aee97110-7349-4f91-82e2-d4b2ffa35096	{"fileName": "1d30e6fe-10ee-4c2f-8021-6afe1280aaa2.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:55:46.70223
1ff2a657-61a4-4606-8d06-72f7f0788d5a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	a929494b-7fd9-4894-b81a-76eebde6bb9c	{"fileName": "32f0f5cc-32b9-4652-b2aa-de973aca8c71.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:55:48.636894
120c31fb-2c86-4851-834f-e42009e52fa5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	2c275de2-28f0-43a0-ab9c-34d9649275f6	{"fileName": "6f835d7b-4d33-47da-9580-044207e9f152.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:55:50.192314
9359f655-b0ed-40a0-84b9-fd7e3d286734	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	a9cdf06a-b641-4331-8aa0-9b989548807e	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:56:03.779186
0b78d1a5-72e0-425b-af44-0103a30a9061	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	a9cdf06a-b641-4331-8aa0-9b989548807e	{"fileName": "a2a68f89-308d-4119-b601-ddd18a0c1781.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:58:27.369826
e00208d8-7993-497d-b0a8-6367ce848a63	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	69b1f45b-77bf-4821-9423-64b8d01c4edc	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 21:58:37.662094
8b0975da-124a-42cb-ab55-4ad04a1a2903	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	69b1f45b-77bf-4821-9423-64b8d01c4edc	{"fileName": "e08a3e99-9e82-4f44-a497-8027da698425.pdf"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:03:04.057135
cab39f38-8269-486f-87c2-84bd80ba0920	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	6e8d3502-900b-4e70-a1ab-f5b0b02a9043	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:03:11.712226
61aa50a9-453e-4732-8682-2296b4400f1d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Technology_manufacturing_sales.csv", "contractId": "6e8d3502-900b-4e70-a1ab-f5b0b02a9043", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.12.237	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:03:52.730559
9c4e67e4-539a-452f-bb80-5ccf514f7709	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	6e8d3502-900b-4e70-a1ab-f5b0b02a9043	{"fileName": "19737cc2-57b0-4495-9e84-7894595b4d88.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:08:22.900131
2a71142a-fb35-4e08-8253-136b030e9fad	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	c26e377d-a4eb-4a36-aa11-cb68c65aadee	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:08:34.09602
39aa4a1e-997b-4e59-b96b-4a900541769e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Technology_manufacturing_sales.csv", "contractId": "c26e377d-a4eb-4a36-aa11-cb68c65aadee", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:09:17.791245
b30882a6-8a29-4be1-9454-8df1f8e6fe39	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	14f6efc2-9ac6-4b44-86e6-587f30530447	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:09:48.763685
c1165876-d404-4180-b6c0-8f4ba2b397d2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	14f6efc2-9ac6-4b44-86e6-587f30530447	{"fileName": "51fee958-f049-480f-92fe-c2f222384083.pdf"}	10.81.0.129	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:13:34.811191
5098a826-4a83-436d-992c-30f234748a3f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	eb5970ce-7958-48af-8470-51fe11ec6543	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:13:51.668903
d0e86f62-67e5-432c-93ed-5bf7ec730d2e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "electronics_license_sales_sample.csv", "contractId": "eb5970ce-7958-48af-8470-51fe11ec6543", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:14:39.070078
b83b9c3f-64ae-4fc6-8cdd-bedfcf881157	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	a7950452-495f-4099-874e-49d59dfffdef	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:15:27.230167
09887092-6279-401f-abf6-0db1d50dfc7a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Plant_sample_sales_data.csv", "contractId": "a7950452-495f-4099-874e-49d59dfffdef", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:16:44.381906
1ad47700-28ac-4743-b9a5-1b9d8e9665ca	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the royalty rates?", "confidence": 0.5686775737898749, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:22:40.536399
c44af06a-852a-4386-98bf-16295dd33658	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Are there any volume discounts?", "confidence": 0.8, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:22:44.928378
4f9adb11-5b71-4450-9749-3a084a8bee80	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the Contract Type License IQ handles ?", "confidence": 0.5986926803272508, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:23:05.423863
61c7666e-e58a-4acb-ab70-527aa8b0bd41	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "what is license iq ", "confidence": 0.5955282275631545, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:23:34.780038
f5c78f1f-bd6a-49ad-92a2-32d070ff7efe	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Are there any volume discounts?", "confidence": 0.8, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:23:53.714889
8101b131-28dc-40c1-b2cc-aa8ed326de06	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "How many types of contract types now ", "confidence": 0.5940403267267983, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:24:07.616345
346ce564-6e3f-4227-a65f-0ad8bad1c6cd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What types of contracts does this platform support", "confidence": 0.7634645190952318, "sourcesCount": 3}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:46:14.781106
37234bd6-fcde-45fa-8a0f-3053c5e77af3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What is LicenseIQ?", "confidence": 0.8475980071781893, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:50:57.748243
4a6dd8f7-e7a6-448d-b52e-edbf8ca24e44	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What types of contracts does this platform support?", "confidence": 0.7544968128204346, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:51:02.116367
96b965a7-1d68-4c0e-8975-8714d60cac58	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What AI services are used?", "confidence": 0.6866151503691179, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:51:15.611337
80ba320c-e5e0-49ae-be69-4d0c2cff3608	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the license fee rates?", "confidence": 0.7423125726673777, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:51:23.613167
d2420ea9-b33c-4d5e-b79a-7408193bc9eb	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "When are payments due?", "confidence": 0.548589126723681, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:51:35.887406
f40fdd95-e757-48bf-8475-5d9aca8158cc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Are there any volume discounts?", "confidence": 0.552621738690786, "sourcesCount": 2}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:51:42.649797
d54333fc-3daf-433a-baf6-f22f28a40396	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the reporting requirements?", "confidence": 0.5581811832839606, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:51:50.324672
8c45a91a-6109-4143-86e5-5c3b7952ea4e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "How does the license fee calculation work?", "confidence": 0.8566416757298019, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:52:02.81327
5aa557d5-f389-4ece-9167-03a773b78f95	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "How does the license fee calculation work?", "confidence": 0.8566416757298019, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:52:17.099294
88371bdb-3467-4bfd-96c7-d1e782864def	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	a7950452-495f-4099-874e-49d59dfffdef	{"fileName": "acfefc48-9d6b-4721-a02b-dc9620f24274.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:53:45.669704
a6377b6d-aeec-40bb-8bf8-14a230987066	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	eb5970ce-7958-48af-8470-51fe11ec6543	{"fileName": "19a0a143-cf3c-4ae1-b2d2-08968723d668.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:53:48.265624
bba28d10-d3a3-4cd0-b44f-c86b6563c5da	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	c26e377d-a4eb-4a36-aa11-cb68c65aadee	{"fileName": "c3a28acb-a73f-4959-948a-e764d0ec1294.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:53:50.269395
2ba8f409-d005-4987-ba40-9b4584f29300	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	3a75b89f-84ec-408f-9414-ff151fcd79e2	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:54:04.264617
7f1c0048-d0a3-4d70-83f8-2f0b3b2bc76e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "electronics_license_sales_sample.csv", "contractId": "3a75b89f-84ec-408f-9414-ff151fcd79e2", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:54:41.517832
6ec7bbaa-bf52-447c-8811-017835f91c90	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What types of contracts does this platform support?", "confidence": 0.7544968128204346, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:55:27.117242
c30cdbb1-e231-4383-ac82-bb12fea5b90d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "How does the license fee calculation work?", "confidence": 0.8566416757298019, "sourcesCount": 3}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:55:39.163688
e6950c1f-086a-43e3-8815-4e7caecd4b80	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "How does LicenseIQ process sales data?", "confidence": 0.8125337250550821, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:59:04.64726
e2d75fc7-5d96-4561-b62f-5b3c09d51653	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What is the RAG-powered Q&A system?", "confidence": 0.5821263790130656, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:59:13.347317
0d196b10-5afd-4e0a-a46d-35c9c426560d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the license fee rates?", "confidence": 0.7423125726673777, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:59:39.434924
3402769b-d94f-4abe-942c-fc02df50d3b9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What is the RAG-powered Q&A system?", "confidence": 0.5821263790130656, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:59:47.162496
d5105ed3-2186-4a55-81ee-c9f1271025e7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "How do I navigate the platform?", "confidence": 0.5795311504462707, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 22:59:57.161633
7c4a64f5-eee5-4927-8a3f-05e27870608a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What is LicenseIQ?", "confidence": 0.8475980071781893, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:01:25.169237
57a61810-d26f-4e9a-92cb-6862e8652bf2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What types of contracts are supported?", "confidence": 0.7634543635519974, "sourcesCount": 3}	10.81.12.237	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:03:17.644499
98a33ed4-8f17-47b5-86dd-f850f6b2cae2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What security features are available?", "confidence": 0.6385417770904835, "sourcesCount": 3}	10.81.12.237	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:04:01.424965
b32507d1-c35b-46ad-92f9-9b2035fe8335	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Is there any reports avalialbe in liq ?", "confidence": 0.6705203063226306, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:05:46.579951
9abdbc53-9e7f-4dfb-b2d2-f3de385449d5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Any analytics available ?", "confidence": 0.5427561415057089, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:06:17.883775
cc5c78c3-fd95-4ee7-831f-0d42cef7bf56	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the license fee rates?", "confidence": 0.7423125726673777, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:06:31.846753
d5186f6b-576f-4fa3-9f91-f81eeb9e1f33	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What products are included?", "confidence": 0.552176077434864, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:06:37.995849
f1132b15-02c4-4710-b0d1-e5cd51c50a39	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0.6459659760038882, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:06:44.174957
29b7b97e-49f2-4ee1-9b26-36a8b2039db7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0.6459659760038882, "sourcesCount": 3}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:06:48.023669
5e1429ea-27b7-4afd-96fb-52a1edadf67b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	b0ddaf11-2877-45fe-81c8-78e6c638c1e4	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:16:03.307773
6345c7ee-5ef3-4670-b935-b4b1a25e41d7	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	b0ddaf11-2877-45fe-81c8-78e6c638c1e4	{"fileName": "6be3fba9-401f-4621-b3b7-000d702845d8.pdf"}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:16:52.972394
fe0adb69-f017-4508-b67a-2092d5a53546	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	2da365eb-5a1c-4de2-b1f5-4016762f3509	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:17:01.114784
a3887237-213d-4b21-bf28-999f6454f653	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Technology_manufacturing_sales.csv", "contractId": "2da365eb-5a1c-4de2-b1f5-4016762f3509", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:17:59.244057
8ec35892-836b-4b96-91dc-4aadf22f7ef2	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	186ad8a6-1175-4d17-a972-b07e0cb981c0	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:18:34.419437
b52235e5-7875-4946-80c1-3bffe728e816	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Plant_sample_sales_data.csv", "contractId": "186ad8a6-1175-4d17-a972-b07e0cb981c0", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:19:10.328844
b7ab1af3-0ea3-48dd-833d-ca1915069a7f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	create_company	company	eeca99c0-de3e-4d69-8599-8ff6f1dc9dcc	{"companyName": "Rao Groupof Companies"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:22:22.749683
6c534dba-23bb-464f-ae7f-90128f635b63	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	create_business_unit	business_unit	7e06ef6e-0dfb-4068-8e93-17c770b7d053	{"orgName": "Dallas Unit", "companyId": "eeca99c0-de3e-4d69-8599-8ff6f1dc9dcc"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:22:35.779222
a7240644-48e3-4df9-8e4a-67ce0b5b6acc	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	create_location	location	391f5e20-4161-4480-abca-a5b2a8f959f8	{"orgId": "7e06ef6e-0dfb-4068-8e93-17c770b7d053", "locName": "Frisco", "companyId": "eeca99c0-de3e-4d69-8599-8ff6f1dc9dcc"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:23:11.761156
a20b5eb4-0769-41bd-8c4b-0826a3c17fea	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	186ad8a6-1175-4d17-a972-b07e0cb981c0	{"changes": "Added Company Name"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:24:53.133244
161498f4-47e5-461d-b156-439e77077cd6	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	186ad8a6-1175-4d17-a972-b07e0cb981c0	\N	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:25:11.601454
a0fb1fa7-637f-4edf-bcb7-058a702c45ea	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_approved	contract_version	9fcec8d9-d8b7-4e94-89b8-76a006ea2aec	{"notes": "approved", "approver": "admin"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:25:35.092498
4221ec16-bf3f-4d4c-b9d5-316a7e1c0e5d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	update_contract_metadata	contract	186ad8a6-1175-4d17-a972-b07e0cb981c0	{"changes": "sdsfsdf"}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:26:21.996539
ee8eef57-2945-4dca-a247-1e1c5ce673af	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	submit_contract_approval	contract	186ad8a6-1175-4d17-a972-b07e0cb981c0	\N	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:26:23.60348
9f36a2a2-f6a9-4e78-9cfb-6895de7f96b9	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_rejected	contract_version	fa792396-fc2a-4217-a152-e9d0db914906	{"notes": "sdfdsfs", "approver": "admin"}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:26:33.089105
8c683198-bf8d-4bc8-8ca3-4875676167ea	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What is LicenseIQ?", "confidence": 0.8475980071781893, "sourcesCount": 3}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:27:10.396594
888088b2-0ccc-4d14-9b9e-8c1b6faf1e11	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What types of contracts are supported?", "confidence": 0.7634543635519974, "sourcesCount": 3}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:27:15.965576
431f8c67-91a0-4d48-b28d-aacd89667cd8	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "How does the license fee calculation work?", "confidence": 0.8566416757298019, "sourcesCount": 3}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:27:23.672346
e760fd32-7e04-46e2-b530-e6c36c19297f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "What are the payment terms?", "confidence": 0.6459659760038882, "sourcesCount": 3}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:27:38.1213
e2289e61-83ed-4fc3-9c39-ff6fd56e1518	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rag_query	contract	all	{"question": "Which territories are covered?", "confidence": 0.5322720408439636, "sourcesCount": 2}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:27:53.46088
d91507af-5f98-4338-aadc-d4004b13d551	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	6ba9823b-3f37-4400-a403-733ae1b02279	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.11.116	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:30:20.086994
e9b79698-4555-438e-85e5-3c8b55148485	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	6ba9823b-3f37-4400-a403-733ae1b02279	{"fileName": "1276df5a-b871-4ed7-8549-df4c3dd4e8f2.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:38:05.446685
755dc848-dd5a-4d68-967a-3aa3d1a23b8b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	48330ac2-3f3d-41da-b9c1-329cd1266a5c	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:38:20.149239
b67d57be-169f-4ca9-85c3-3067664b0c3f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	48330ac2-3f3d-41da-b9c1-329cd1266a5c	{"fileName": "c678d8af-c204-4683-a286-147e45402b97.pdf"}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:46:40.903008
b92a0c7b-2e1e-4f9c-b45b-3140c7893c15	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	a4f94864-ae00-44a6-87af-b31fc3c37d06	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.1.31	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-11 23:46:48.842509
343735ae-8603-4bf7-9a89-6fb40db18375	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	a4f94864-ae00-44a6-87af-b31fc3c37d06	{"fileName": "9e574a4a-2f28-441a-835f-a318bfc5b193.pdf"}	10.81.1.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:44:29.012862
e3835621-5774-4aae-88df-188b910d4ebb	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	417c9a23-00aa-4c7e-8bcf-803048670f91	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.1.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:44:41.545075
adf6ef97-7e8c-4fcf-8784-8f259964335f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	417c9a23-00aa-4c7e-8bcf-803048670f91	{"fileName": "c77db401-3807-45f0-9ded-41193129ba69.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:51:48.170202
f88a5579-d21e-4051-82f9-72b80325b9c3	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	b579b9e6-f4c1-4e81-a2af-612d8d8b195e	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.1.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:51:59.787845
9ff96a77-5920-4629-a4cd-cfca7a0f69b1	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	b579b9e6-f4c1-4e81-a2af-612d8d8b195e	{"fileName": "47391d8e-d4e1-45f9-9ecb-afb95b33284a.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:52:56.810013
d3d03813-f61b-424a-884f-839a86762248	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	2da365eb-5a1c-4de2-b1f5-4016762f3509	{"fileName": "490c87d2-ec6a-43b3-9235-f95ae54624ae.pdf"}	10.81.1.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:52:59.690073
550243d9-6031-40b4-a13f-7a6665023f19	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	186ad8a6-1175-4d17-a972-b07e0cb981c0	{"fileName": "c5661c06-0596-4427-87e4-fd397a938e0f.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:53:01.500364
573453fb-ed18-4b9a-ad0a-c7497297651a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	00af09e1-f7c6-46a8-8277-19036ae00d12	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.6.50	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:57:39.311942
17168336-7641-4828-bf56-e6658425ba43	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	00af09e1-f7c6-46a8-8277-19036ae00d12	{"fileName": "e2270d57-63f8-4f45-8e1e-d63f8b3336e4.pdf"}	10.81.1.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:58:15.198771
0de7bf3e-81e7-4af6-9f86-3f01928d0801	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	e3a08d4e-3db3-439e-b60c-fbe9fc23ff5a	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:58:32.571557
615c358c-9af0-4991-8cda-d07472893e73	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	f31ed06d-dd5e-4b1e-b1ee-e7bb02563905	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.1.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:59:10.626373
85299e05-63ff-4837-9b5d-038e0cfef659	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	314134db-5d9f-46a9-afd2-023ec65ec670	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.11.119	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 14:59:59.737567
b74f3d4d-f55b-4a14-b7f5-91dc3fe82c35	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	314134db-5d9f-46a9-afd2-023ec65ec670	{"fileName": "5abac1df-92b7-41eb-9df2-72f0468d4b6e.pdf"}	10.81.2.132	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:07:08.94615
edfc9a6d-0c97-46ce-b182-4210e58c6cf6	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	e3a08d4e-3db3-439e-b60c-fbe9fc23ff5a	{"fileName": "aa0f81b0-e101-44dc-bb1f-93779e8fd50c.pdf"}	10.81.12.251	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:07:10.920861
1e8000e6-4ada-4225-8adb-9a635922413b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	f31ed06d-dd5e-4b1e-b1ee-e7bb02563905	{"fileName": "0e431590-c5eb-4203-a941-fbeac96d6cd7.pdf"}	10.81.12.251	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:07:12.92356
596e1b64-6214-4537-b92c-f2d10806c134	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	e79100f1-bd60-4705-be17-05bc87beac5c	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.12.251	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:07:28.617423
39489601-286d-4a34-9545-247a85e1eb1e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	e79100f1-bd60-4705-be17-05bc87beac5c	{"fileName": "c1bd09c6-5b92-4f3a-8179-06a239cff37d.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:11:00.694105
2abf007a-1310-45db-a4a3-7e8bcae1cc93	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	e5dfc16b-a3d8-4176-b470-2e5dfbff7a64	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.12.251	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:11:10.471353
348395f3-aa4c-4073-961a-05f839b901fd	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	e5dfc16b-a3d8-4176-b470-2e5dfbff7a64	{"fileName": "4130aa19-c74c-4bc9-bf55-2805385d1e7f.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:14:48.057536
1c69d90e-4790-41db-b96b-110f043f95f5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	49453303-70c8-4af0-b5c0-5f89e72b79d2	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.7.156	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:15:02.637825
af6383bf-2a57-4bbe-b1e5-9e56a0f7f72b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	49453303-70c8-4af0-b5c0-5f89e72b79d2	{"fileName": "3d7c28c0-e069-4132-9fe3-0d0166bcea2c.pdf"}	10.81.4.39	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:19:11.328412
00f9e3b3-45e9-49e5-a947-05d43d005a86	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	ba2e191f-029a-411f-8b14-388760ff0d0a	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:19:20.499321
b1fce031-2d08-4956-95cf-d56d1d360efe	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	ba2e191f-029a-411f-8b14-388760ff0d0a	{"fileName": "be11e156-4c61-4019-9499-e5a6f8f8ee77.pdf"}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:23:43.400687
91bb3c30-e119-4e8a-828f-1ccdc829f33b	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	57e904c2-9388-4938-876e-7e9509b1b9fb	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:23:50.486485
ce3dcf1c-2cd2-417c-af45-5c4ab1d6fc51	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "electronics_license_sales_sample.csv", "contractId": "57e904c2-9388-4938-876e-7e9509b1b9fb", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:24:37.999831
b0c97ee9-1608-401e-b44c-adb6a5d235ce	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	6114867c-8709-4239-9069-dc5b7097990b	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:25:17.620033
801eb330-754b-49cb-8f31-44611efe4f85	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Technology_manufacturing_sales.csv", "contractId": "6114867c-8709-4239-9069-dc5b7097990b", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:25:55.603667
47121c6f-f90a-4d9f-add5-4f282143a24e	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	98f3a291-fbe3-4aa1-b1df-f9c56826b266	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:26:19.892357
9487d425-94cb-4fd1-8431-27ac7c7455b5	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Plant_sample_sales_data.csv", "contractId": "98f3a291-fbe3-4aa1-b1df-f9c56826b266", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.1.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:26:53.782216
b4a344ed-2498-4b21-bd33-5741b2b20f9d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	57e904c2-9388-4938-876e-7e9509b1b9fb	{"fileName": "bada8abb-5e09-46a9-be10-b8400160dd6e.pdf"}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:30:50.107791
a58aa6e5-236b-4023-9168-da717b44a70d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	6d0f0e4c-8d9e-4d4d-9442-9a7c16398d6a	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:31:00.972089
5746b8c9-109f-46b0-bfa7-82ba47ee7f00	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "electronics_license_sales_sample.csv", "contractId": "6d0f0e4c-8d9e-4d4d-9442-9a7c16398d6a", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:31:54.460009
71cf23f0-7e9c-439d-926d-acda5146790a	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	6d0f0e4c-8d9e-4d4d-9442-9a7c16398d6a	{"fileName": "50107907-f2ce-42c0-816d-2498ac0df82b.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:32:22.838146
232b2e94-1ed8-476c-ae99-4d3bd9d42d61	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	98f3a291-fbe3-4aa1-b1df-f9c56826b266	{"fileName": "e8c7d086-dda3-4468-85e4-6a08c57d2820.pdf"}	10.81.1.89	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:32:25.241809
894217f7-4e86-4fec-9825-c29937082653	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	delete_contract	contract	6114867c-8709-4239-9069-dc5b7097990b	{"fileName": "3e5d8913-88f2-4bab-ab1b-72fac14164af.pdf"}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:32:27.114853
d2bf581b-f3b3-4d09-82fd-f7644486db58	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	{"fileSize": 421738, "originalName": "Technology License & Royalty Agreement - Manufacturing.pdf"}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:32:41.348089
63f4178c-61bd-40c4-b196-ce4af2be3bbb	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Technology_manufacturing_sales.csv", "contractId": "8c7a356f-33ce-4eaa-8cfe-eb483b6a823f", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:33:03.733578
9e1e3b96-6ff7-4138-abc8-f88c7a7b7d41	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	8f321b79-1ab4-4b02-8806-26db3a21a8ff	{"fileSize": 456378, "originalName": "Electronics Patent License & Component Royalty Agreement.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:33:53.067149
36b49cb5-a893-4c6e-80ec-2378d0831a3c	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	rule_updated	royalty_rule	aa9a184f-5055-49e1-9171-7b043fe33e30	{"updates": {"baseRate": null, "priority": 2, "ruleName": "Min Payment", "ruleType": "minimum_guarantee", "description": "Quarterly minimum", "territories": ["US"], "volumeTiers": [], "containerSizes": [], "minimumGuarantee": "625000.00", "productCategories": ["All"], "territoryPremiums": {}, "seasonalAdjustments": {}}, "contractId": "8f321b79-1ab4-4b02-8806-26db3a21a8ff"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:35:15.440197
e8a27743-c711-4800-8459-7f11fcf82b5d	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "electronics_license_sales_sample.csv", "contractId": "8f321b79-1ab4-4b02-8806-26db3a21a8ff", "rowsImported": 20, "erpMatchingEnabled": false}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:35:45.557931
f0e09938-82b1-40a9-9865-8158fc67b7ea	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	contract_upload	contract	46d0e351-4296-45bb-a680-9ee65497967b	{"fileSize": 443464, "originalName": "Plant Variety License & Royalty Agreement.pdf"}	10.81.5.91	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:36:32.05789
f716b6aa-dc9c-4150-bb60-b8b1195ba79f	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	upload_sales_data	sales	\N	{"fileName": "Plant_sample_sales_data.csv", "contractId": "46d0e351-4296-45bb-a680-9ee65497967b", "rowsImported": 15, "erpMatchingEnabled": false}	10.81.4.34	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36	2025-11-12 15:37:10.718317
\.


--
-- Data for Name: business_units; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.business_units (org_id, company_id, org_name, org_descr, address1, contact_person, contact_email, contact_phone, contact_preference, status, created_by, creation_date, last_updated_by, last_update_date) FROM stdin;
org-001	cmp-001	Sales Division	Global sales and marketing operations	\N	Michael Chen	michael.chen@acmecorp.com	+1-415-555-0110	\N	A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407
org-002	cmp-001	Operations Division	Product development and logistics	\N	Emily Rodriguez	emily.r@acmecorp.com	+1-415-555-0120	\N	A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407
7e06ef6e-0dfb-4068-8e93-17c770b7d053	eeca99c0-de3e-4d69-8599-8ff6f1dc9dcc	Dallas Unit	\N	\N	\N	\N	\N	\N	A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-11 23:22:35.750395	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-11 23:22:35.750395
\.


--
-- Data for Name: companies; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.companies (company_id, company_name, company_descr, address1, address2, address3, city, state_province, county, country, contact_person, contact_email, contact_phone, contact_preference, status, created_by, creation_date, last_updated_by, last_update_date) FROM stdin;
cmp-001	Acme Corporationf	Global technology and innovation leader				San Francisco	California		USA	John Smith	john.smith@acmecorp.com	+1-415-555-0100		A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 23:33:08.174
cmp-002	TechVentures Inc	Innovative software solutions provider				Austin	Texas		USA	Sarah Johnson	sarah.j@techventures.com	+1-512-555-0200		A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 23:46:56.094
e8dcc53f-8f59-4732-baae-224859cb66b7	Test Company	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 23:47:14.905381	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 23:47:14.905381
eeca99c0-de3e-4d69-8599-8ff6f1dc9dcc	Rao Groupof Companies	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-11 23:22:22.720684	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-11 23:22:22.720684
\.


--
-- Data for Name: compliance_analysis; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.compliance_analysis (id, contract_id, compliance_score, regulatory_frameworks, jurisdiction_analysis, data_protection_compliance, industry_standards, risk_factors, recommended_actions, last_compliance_check, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: contract_analysis; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_analysis (id, contract_id, summary, key_terms, risk_analysis, insights, confidence, processing_time, created_at, updated_at) FROM stdin;
4cd93954-da95-48da-8f5f-a29602735b3b	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	This Technology License and Royalty Agreement grants Precision Industrial Solutions Inc. exclusive manufacturing rights to Advanced Materials Technology Corp.'s patented technologies and trade secrets. The agreement outlines royalty payments, manufacturing requirements, and termination conditions.	{"terms": [{"type": "Royalty Structure", "location": "Section 3.1", "confidence": 0.95, "description": "The agreement outlines a tiered royalty rate structure based on Net Sales of Licensed Products. For automotive components, the royalty rate ranges from 6.5% to 4.8% depending on the sales volume. For industrial and aerospace components, the royalty rate is 7.2% to 9.8% depending on the application."}, {"type": "Payment Terms", "location": "Section 3.1 and 3.2", "confidence": 0.9, "description": "Licensee must pay royalties based on Net Sales, with minimum annual guarantees ranging from $125,000 to $500,000 depending on the sales volume. Royalty payments are due within 30 days of quarterly report submission."}, {"type": "Manufacturing Requirements", "location": "Section 5.1 and 5.2", "confidence": 0.88, "description": "Licensee must manufacture Licensed Products in accordance with Licensor's specifications and quality standards, including ISO 9001:2015 and TS 16949 automotive quality certifications. Licensee must also meet production volume commitments, with a minimum of 50,000 units annually in the first two years."}, {"type": "Licensed Technology", "location": "Section 1.1", "confidence": 0.85, "description": "The agreement grants Licensee rights to use, manufacture, and distribute products incorporating patented technologies, including US Patent 11,247,839 and US Patent 11,089,472."}, {"type": "Termination Conditions", "location": "Section 9.1 and 9.2", "confidence": 0.8, "description": "The agreement can be terminated upon 90 days written notice for material breach. Upon termination, Licensee may continue selling existing inventory for 12 months, subject to continued royalty payments and reporting obligations."}, {"type": "Financial Obligations", "location": "Section 4.1 and 4.2", "confidence": 0.85, "description": "Licensee must pay a one-time license fee of $850,000 and an additional $275,000 for comprehensive technology transfer. Licensee must also pay royalties based on Net Sales, with minimum annual guarantees ranging from $125,000 to $500,000 depending on the sales volume."}, {"type": "Performance Requirements", "location": "Section 5.2 and 5.3", "confidence": 0.8, "description": "Licensee must meet production volume commitments, with a minimum of 50,000 units annually in the first two years. Licensee must also achieve market leadership in the defined automotive transmission component segment."}, {"type": "Territory and Scope", "location": "Section 2.2", "confidence": 0.85, "description": "The agreement grants Licensee exclusive manufacturing rights within the United States, Canada, and Mexico for manufacturing operations, and distribution rights extend to all of North and South America."}], "currency": "USD", "licensee": "Precision Industrial Solutions Inc.", "licensor": "Advanced Materials Technology Corp.", "paymentTerms": "Royalties based on Net Sales of Licensed Products", "effectiveDate": "January 8, 2024", "expirationDate": "January 8, 2034"}	[{"level": "high", "title": "Non-Compliance with Manufacturing Requirements", "description": "Failure to meet production volume commitments or manufacturing standards may result in termination of the agreement and loss of exclusive manufacturing rights."}, {"level": "medium", "title": "Royalty Rate Disputes", "description": "Disputes over royalty rates or calculations may lead to delays in payment or termination of the agreement."}, {"level": "low", "title": "Intellectual Property Disputes", "description": "Disputes over intellectual property ownership or infringement may lead to costly litigation."}]	[{"type": "opportunity", "title": "Market Leadership Opportunity", "description": "Licensee has the opportunity to achieve market leadership in the defined automotive transmission component segment and receive a $500,000 milestone payment."}, {"type": "alert", "title": "Non-Compliance Risk", "description": "Licensee must ensure compliance with manufacturing requirements to avoid termination of the agreement and loss of exclusive manufacturing rights."}]	0.92	0	2025-11-12 15:32:46.28523	2025-11-12 15:32:46.28523
b55ed8f2-3325-430f-bbf2-8754a22bff42	8f321b79-1ab4-4b02-8806-26db3a21a8ff	This is a comprehensive electronics patent license and royalty agreement between Advanced Chip Technologies Corp. (Licensor) and Nexus Electronics Manufacturing Inc. (Licensee). The agreement grants Licensee non-exclusive rights to manufacture, use, and distribute products incorporating Licensor's patented technologies. The agreement outlines various commercial terms, including royalty structures, payment terms, manufacturing requirements, and termination conditions.	{"terms": [{"type": "Royalty Structure", "location": "Section 3.1", "confidence": 0.95, "description": "Licensee shall pay royalties based on Net Sales of Licensed Products according to a tiered structure, with different rates for different product categories and geographic regions."}, {"type": "Payment Terms", "location": "Section 6.2", "confidence": 0.9, "description": "Licensee shall pay royalties quarterly, with the first payment due within 45 days of quarter end. Minimum guarantee payments are also due quarterly in advance."}, {"type": "Manufacturing Requirements", "location": "Section 5.1", "confidence": 0.88, "description": "All Licensed Products must be manufactured in accordance with ISO 9001:2015 quality management systems, ISO/TS 16949 automotive quality standards, IPC-A-610 electronic assembly standards, JEDEC standards for semiconductor reliability testing, and RoHS compliance for all electronic components."}, {"type": "Licensed Technology", "location": "Section 1", "confidence": 0.95, "description": "Licensor grants Licensee non-exclusive rights to manufacture, use, and distribute products incorporating Licensor's patented technologies, including semiconductor and microprocessor patents, circuit design and interface patents, and trade secrets and know-how."}, {"type": "Termination Conditions", "location": "Section 10.1 and 10.2", "confidence": 0.9, "description": "The agreement may be terminated by either party upon material breach, insolvency, bankruptcy, or assignment for benefit of creditors. Upon termination, Licensee may continue selling existing finished goods inventory for 24 months and complete products in various stages of production within 12 months."}, {"type": "Financial Obligations", "location": "Section 4.1 and 4.3", "confidence": 0.95, "description": "Licensee shall pay an initial license fee of $1,250,000 within 45 days of agreement execution. Licensee shall also pay annual support and updates fees of $185,000, and volume milestone bonuses based on cumulative sales achievements."}, {"type": "Performance Requirements", "location": "Section 5.4", "confidence": 0.9, "description": "Licensee shall maintain quality metrics, including defect rate below 50 PPM for all licensed components, customer return rate below 0.1% within 12 months of sale, and compliance with Licensor's electrical and mechanical specifications within ±2% tolerance."}, {"type": "Territory and Scope", "location": "Section 2.2", "confidence": 0.95, "description": "The agreement grants Licensee non-exclusive rights to manufacture, use, and distribute products incorporating Licensor's patented technologies within the United States, Canada, Mexico, European Union, United Kingdom, Australia, New Zealand, Japan, South Korea, and Taiwan."}], "currency": "USD", "licensee": "Not specified", "licensor": "Not specified", "paymentTerms": "Not specified", "effectiveDate": null, "expirationDate": null}	[{"level": "high", "title": "Business Risk of Non-Compliance with Manufacturing Requirements", "description": "Failure to comply with manufacturing requirements may result in quality issues, product recalls, and damage to reputation."}, {"level": "medium", "title": "Business Risk of Non-Payment of Royalties", "description": "Failure to pay royalties may result in termination of the agreement and loss of rights to manufacture, use, and distribute products incorporating Licensor's patented technologies."}]	[{"type": "opportunity", "title": "Opportunity to Increase Sales and Revenue", "description": "By meeting performance requirements and complying with manufacturing requirements, Licensee may increase sales and revenue by producing high-quality products that meet customer needs and expectations."}]	0.92	0	2025-11-12 15:34:02.75006	2025-11-12 15:34:02.75006
8820c2ce-5592-4eb7-a2ae-1051c55eb037	46d0e351-4296-45bb-a680-9ee65497967b	This document outlines a Plant Variety License & Royalty Agreement between Green Innovation Genetics LLC (Licensor) and Heritage Gardens Nursery & Landscaping (Licensee). The agreement grants exclusive regional plant variety rights to the Licensee for a period of eight years, with automatic renewal for successive four-year periods. The Licensee must pay royalties based on net plant sales, with varying rates depending on the type of plant and sales volume.	{"terms": [{"type": "Royalty Structure", "location": "Section 3.1", "confidence": 0.95, "description": "The Licensee must pay royalties based on net plant sales, with varying rates depending on the type of plant and sales volume. For example, for Tier 1 - Ornamental Trees & Shrubs, the royalty rate is $1.25 per unit for 1-gallon containers, with a discount rate of $1.10 per unit for 5,000+ units annually."}, {"type": "Payment Terms", "location": "Section 6.2", "confidence": 0.9, "description": "The Licensee must pay royalties within 45 days of the quarter end, with a late payment charge of 1.25% per month. The Licensee must also pay an annual certification and inspection fee of $12,500."}, {"type": "Manufacturing Requirements", "location": "Section 5.1", "confidence": 0.88, "description": "The Licensee must meet minimum annual production levels, with a minimum of 15,000 total plants across all varieties in Years 1-2, and a minimum of 35,000 total plants with 20% year-over-year growth in Years 3-4."}, {"type": "Licensed Technology & Patents", "location": "Section 1.2", "confidence": 0.85, "description": "The Licensor grants the Licensee access to proprietary propagation methods covered by US Patent 11,456,892 and US Patent 11,234,567, as well as trade secret 'GreenGrow™ Nutrient Formula and Application Methods'."}, {"type": "Termination & Post-Termination", "location": "Section 9.1", "confidence": 0.8, "description": "Either party may terminate the agreement upon 180 days written notice for material breach, or upon 12 months notice for convenience. Upon termination, the Licensee may sell existing finished inventory for 18 months subject to continued royalty payments."}, {"type": "Financial Obligations", "location": "Section 4.1", "confidence": 0.85, "description": "The Licensee must pay an initial license fee of $125,000, as well as an annual certification and inspection fee of $12,500. The Licensee must also guarantee minimum annual royalty payments totaling $85,000."}, {"type": "Performance Requirements", "location": "Section 5.1", "confidence": 0.88, "description": "The Licensee must meet minimum annual production levels, with a minimum of 15,000 total plants across all varieties in Years 1-2, and a minimum of 35,000 total plants with 20% year-over-year growth in Years 3-4."}, {"type": "Territory & Scope", "location": "Section 2.1", "confidence": 0.9, "description": "The agreement grants exclusive regional plant variety rights to the Licensee within the following geographic region: Oregon, Washington, Northern California (counties north of Fresno), and Idaho."}], "currency": "USD", "licensee": "Heritage Gardens Nursery & Landscaping", "licensor": "Green Innovation Genetics LLC", "paymentTerms": "Royalties based on Net Plant Sales", "effectiveDate": "February 12, 2024", "expirationDate": "February 12, 2032"}	[{"level": "high", "title": "Material Breach Risk", "description": "If the Licensee fails to meet minimum production levels or quality standards, the Licensor may terminate the agreement upon 180 days written notice, resulting in significant financial losses for the Licensee."}]	[{"type": "opportunity", "title": "Increased Sales Potential", "description": "The Licensee has the opportunity to increase sales by meeting minimum production levels and quality standards, and by taking advantage of the exclusive regional plant variety rights granted by the agreement."}]	0.92	0	2025-11-12 15:36:37.945108	2025-11-12 15:36:37.945108
\.


--
-- Data for Name: contract_approvals; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_approvals (id, contract_version_id, approver_id, status, decision_notes, decided_at, created_at) FROM stdin;
\.


--
-- Data for Name: contract_comparisons; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_comparisons (id, contract_id, similar_contracts, clause_variations, term_comparisons, best_practices, anomalies, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: contract_documents; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_documents (id, contract_id, extraction_run_id, document_section, section_order, raw_text, normalized_text, page_number, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: contract_embeddings; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_embeddings (id, contract_id, embedding_type, source_text, embedding, metadata, created_at) FROM stdin;
ffa72ebd-a545-4cd4-80b7-df15ff89a36b	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	summary	This Technology License and Royalty Agreement grants Precision Industrial Solutions Inc. exclusive manufacturing rights to Advanced Materials Technology Corp.'s patented technologies and trade secrets. The agreement outlines royalty payments, manufacturing requirements, and termination conditions.	[-0.07400301,-0.02281022,0.016548667,0.0019883914,0.02098627,-0.032682415,0.05090448,0.049742684,-0.03693885,0.043077394,0.022472054,-0.01701331,0.07385638,-0.05379945,-0.030584296,0.0059677567,0.03632615,0.002618637,0.020336816,0.058214124,0.044143178,-0.025351722,-0.032927435,-0.006639616,-0.016553404,0.015673578,-0.021958752,-0.058161166,-0.050954327,-0.12786059,-0.038063325,-0.010457294,0.023011742,-0.02164315,0.07093901,0.031568985,-0.11462762,-0.03185308,-0.025512194,-0.031712897,0.0059165205,-0.028209317,-0.0367236,-0.003904196,0.036850754,0.030059703,0.040229477,0.019646663,-0.01911766,-0.013313838,0.05318934,-0.0013456268,0.037846286,0.03801039,-0.04978841,0.06054448,0.010176077,0.040748585,0.04629024,0.033674512,0.056836326,-0.033733066,-0.14309679,0.064517334,-0.025917899,0.0563054,-0.054403506,-0.100827515,-0.034237593,0.00207023,0.025350735,-0.01401737,0.028667813,0.031180486,0.019450147,0.020688223,0.0430454,-0.07586321,-0.07291266,-0.0024473306,-0.016461875,-0.0432185,-0.0043249675,-0.06172895,-0.1182193,0.03220339,0.029931342,0.0052286065,0.08097666,0.057527274,0.0043999716,-0.046076924,-0.01611379,-0.027039584,-0.012192965,0.023131778,0.051891066,-0.010457706,0.028168831,0.34006536,-0.023459831,-0.0132967625,-0.031486437,-0.053453993,-0.009057914,0.029228376,-0.021672094,0.0055107605,0.034773424,-0.02346766,0.015543153,0.018094867,0.033472426,0.021928815,-0.03783485,0.021192135,-0.05643511,-0.0059470874,0.04912543,-0.0074427905,0.0005076377,0.08549981,0.012176312,0.04875815,-0.026508372,-0.06577255,0.018326985,0.058748774,-0.03504981,0.020555476,0.01871878,-0.05116439,-0.033568516,0.0020050032,0.075989805,0.031958453,-0.03898541,0.038081832,0.010527543,0.07120411,-0.08488468,0.04185923,-0.06334578,-0.060152583,-0.016199103,0.07806224,0.051549494,0.021967156,-0.013926014,-0.013285385,-0.024472998,0.0030118132,-0.04191543,0.035634603,-0.046564892,0.035174955,0.036279425,-0.04007627,-0.025566101,0.031546332,0.06280962,-0.009612588,-0.015287916,0.12388738,0.013618012,-0.1727658,-0.06329319,-0.011964314,0.0074394066,-0.004496149,-0.013305978,0.026435206,0.004335974,-0.015582216,-0.019042144,0.047614567,0.028284943,0.0050644334,-0.06408988,0.033519883,-0.018659035,-0.009770386,0.019026974,-0.027743554,0.016583672,-0.020298574,0.017766254,0.01291217,-0.0035760815,0.011625628,0.015819535,0.09121738,-0.0086750975,0.10507288,0.05670486,-0.04087665,-0.054929435,0.019403378,0.0038740938,-0.033601213,0.0044116615,0.023741484,-0.034532763,0.00531847,0.032144155,0.06499008,0.022315701,-0.027269715,-0.030698918,0.013275531,-0.08121811,-0.041186187,0.0013313092,-0.029263796,-0.05816049,-0.024043117,-0.0031363864,0.03678243,-0.047472835,-0.036939785,0.03099891,0.09492839,0.044528868,-0.26803997,0.042243507,-0.06806976,0.025549587,-0.0008946218,0.005484006,-0.04747539,-0.018886337,-0.038991198,-0.0051469393,0.11387758,0.065762974,0.026733296,-0.02127279,-0.04466284,-0.07596641,0.029285943,0.03505303,0.006871048,0.0051704114,-0.015962077,0.05951752,-0.09770189,-0.0041999393,0.07263726,0.031931143,0.09208587,-0.0766754,-0.042997476,0.03626427,-0.002447197,0.05829214,0.015715856,-0.02526535,0.034044936,0.00066423585,0.0035448063,0.037583157,0.0332511,-0.00990052,-0.048591487,-0.0033137705,-0.036375944,-0.06543086,0.04396916,-0.03797594,-0.0357205,0.025921017,0.0057596792,0.035341267,0.009467082,0.057008315,-0.01885197,0.010713252,0.0033426904,-0.0036997853,-0.030930968,0.060087238,-0.018055873,-0.030275315,0.034051917,0.007484089,0.034758672,0.01864938,0.00847429,-0.09357772,0.009554157,0.028437797,0.04907576,-0.051428385,0.021071542,0.09862236,0.07311051,0.00710074,-0.014095862,0.024052825,0.03633452,0.0174128,0.022818085,0.028221913,0.05721243,0.028371163,0.07136965,0.014693499,0.030064797,0.025609685,0.0061254334,-0.020936651,-0.058130547,-0.019430578,-0.03117751,0.038390867,-0.0048760874,-0.01675335,0.00575322,-0.036080807,-0.2672035,0.0013288998,0.007802344,-0.019366402,-0.09636686,-0.04679582,0.012368891,0.0014544898,-0.04970464,0.022063144,-0.036226943,0.019905632,-0.06672598,0.029591577,0.035094485,-0.0404968,0.07356208,-0.06474113,0.0013773603,-0.04499069,0.037224784,0.0011238679,0.16191442,0.005719171,0.028299572,-0.017191434,-0.0308525,-0.0062859533,0.039688174,0.0067296117,0.01997385,0.035850003,-0.016457383,0.006994402,0.0023982055,0.06205196,-0.0254411,-0.008836549,-0.07151901,0.037845887,-0.052108012,-0.053887177,0.0022057237,-0.0030849806,0.0486556,-0.0017126704,-0.0480877,-0.044531573,-0.024781711,0.008908277,-0.03658353,-0.00548051,-0.005323718,0.00050412456,-0.06252365,-0.035705294,0.053400084,0.009410003,0.04278035,-0.0016396018,0.018421065,-0.054589693,-0.120422766,0.11348843,0.018275969]	{"type": "summary"}	2025-11-12 15:32:46.705203
8870fd34-a45f-461c-9523-78a8050a2ce1	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	key_terms	[object Object], [object Object], [object Object], [object Object], [object Object], [object Object], [object Object], [object Object]	[-0.029917,0.010987811,-0.02382003,-0.061188996,0.014624095,-0.00058916677,0.02532213,-0.03843175,0.051462095,0.04100082,-0.02875065,-0.08338046,0.0062819785,-0.0055786488,0.032632545,-0.048051354,0.009598552,0.0607168,-0.08215711,-0.05672461,0.066856034,-0.017484061,-0.048984837,-0.03942319,-0.022723785,0.09761533,0.018220022,-0.0051568085,-0.036149222,-0.16640866,-0.04658365,-0.008414659,-0.01492506,0.028178914,0.020346968,0.017105382,-0.035115868,0.05646424,-0.07629423,0.026530989,0.055580504,-0.004666739,-0.036581688,-0.0601175,-0.0022288181,-0.017897947,-0.04863786,-0.017862258,0.036157183,-0.046498027,-0.07740227,-0.041764606,-0.0421569,0.05305529,0.024652982,0.04214629,0.044218145,0.047905833,-0.017685281,-0.008308193,0.0505556,-0.0018819355,-0.08148973,0.022040136,0.016469738,0.031764984,-0.008076806,-0.026056342,-0.011696514,0.04961493,0.04212531,0.04081058,0.02747504,0.020835932,0.019829746,-0.019216105,-0.035634708,-0.06912418,0.014050619,-0.005806741,-0.036622413,-0.007986283,0.0075921896,0.046868786,-0.008352877,-0.009519124,-0.056342445,-0.0947896,0.014106495,0.06868067,-0.05078709,0.019788243,0.063183434,0.025454415,-0.03410491,-0.017067818,-0.00083573716,-0.05074973,0.021976175,0.36882514,-0.08194472,-0.006157381,0.07370184,-0.031687148,-0.026119256,-0.017343072,-0.012132663,-0.094902985,0.012440363,0.058262404,-0.017435743,-0.064908475,0.032550503,-0.06601702,-0.066719815,-0.046608135,0.05213939,-0.027362848,0.0076824357,-0.008298543,0.010604633,0.011917416,0.035709344,0.024043353,-0.018812606,-0.076978445,0.008864149,0.12597567,0.05827304,0.0754369,0.010102635,0.1021886,-0.061479077,0.002718549,0.008881441,0.021520868,0.0024104689,-0.03197842,0.034504894,0.020508997,-0.04121871,-0.05047906,-0.00807553,-0.03137091,-0.0062890686,0.105400056,-0.046594813,0.02973203,-0.0041234526,0.043172043,0.016922014,0.08997135,0.031346284,-0.040496394,0.00083216955,-0.013534927,0.028431801,-0.0008303692,0.00354938,-0.015719716,-0.08906025,0.01792753,0.02062306,0.13254446,-0.06596746,-0.07543938,0.043838974,-0.020601619,0.0014131707,-0.024242332,-0.013187898,0.061149757,-0.06353913,0.024059206,0.057530444,0.023279943,-0.040468007,-0.012804531,0.0031173355,0.019944595,-0.021030255,-0.001217073,-0.049850058,0.037015066,0.047967643,0.030948201,-0.018281046,-0.033155266,-0.014171544,0.018736083,-0.010153218,0.014036103,-0.053305708,-0.03412274,-0.06789396,-0.048959758,0.026725058,-0.04774828,0.0016826252,-0.0068552285,0.09180729,0.005655985,-0.020669403,0.02557489,-0.011076421,-0.00779846,-0.006079689,-0.031833097,0.045965705,0.0087792175,-0.059230663,-0.023151014,0.014961968,0.02183771,-0.010505906,-0.015970264,0.02258985,0.041594192,-0.0041340888,-0.012155353,-0.056780607,-0.041355416,-0.04913164,-0.24758087,0.019636212,0.058624946,-0.038971152,-0.06776015,-0.0037790139,0.011993083,0.030474426,-0.011886876,0.017604174,-0.008837065,-0.068903185,-0.011050413,0.0017934546,-0.023384595,0.008665375,-0.029449405,-0.018833434,-0.07081523,0.028711826,-0.024277039,0.012482956,0.0362485,-0.036630627,-0.04800316,-0.03283225,0.19355382,0.014289806,0.012972047,0.07425716,0.05833281,0.032264616,0.006739806,-0.024912693,0.060430847,0.036074467,-0.017221687,0.029400352,0.021030555,-0.023370672,0.009454852,0.04467961,0.038449388,-0.04353083,0.03691935,-0.041428585,-0.0036865894,-0.028973734,0.007270557,0.020169685,-0.029109472,-0.039439872,0.056238964,0.00372726,-0.013324681,-0.009786537,-0.0021447516,-0.01766433,-0.020357685,0.09155146,0.02829088,-0.05236269,-0.06139067,0.033471867,0.02526662,-0.028719349,0.025219405,-0.020040449,-0.040859398,-0.04670629,-0.060033523,0.047520418,0.037746597,-0.018171983,0.010867564,0.04676861,0.016740808,0.04092023,0.024474747,0.0024983236,0.028467566,0.024904255,-0.023889054,0.02616694,0.061572235,-0.0048373193,0.038990993,-0.0021647958,0.009257482,-0.006774457,0.022792764,-0.015462018,0.021065056,0.007857894,0.081017725,-0.000396665,-0.3074742,0.02028618,0.07037545,0.048929438,0.065452665,0.048192967,0.011144737,-0.09278719,0.008617589,-0.017847499,0.009542779,0.03640893,0.07306826,-0.039358966,-0.0046897447,0.032846235,0.059392586,-0.050422687,0.045846954,-0.019936793,0.0059435028,0.049781397,0.18020721,-0.04633807,0.053447887,0.008819065,0.0063078096,-0.000364295,-0.00084595906,0.026460083,-0.043970186,-0.01062144,0.096798204,0.029828934,0.020925142,0.057587072,-0.00014328126,-0.018985523,0.011619387,-0.034529526,-0.011662715,-0.013330407,-0.037152883,-0.0015337664,0.00731611,0.03171014,-0.008791078,-0.05598366,-0.0035483136,-0.030346386,-0.05272621,-0.050662883,-0.025806747,-0.0027387817,0.03995339,-0.02852235,0.0018139189,0.048113838,-0.05879055,0.010792592,0.026156757,-0.016638683,0.0059629534,0.03564547,-0.008262379]	{"terms": [{"type": "Royalty Structure", "location": "Section 3.1", "confidence": 0.95, "description": "The agreement outlines a tiered royalty rate structure based on Net Sales of Licensed Products. For automotive components, the royalty rate ranges from 6.5% to 4.8% depending on the sales volume. For industrial and aerospace components, the royalty rate is 7.2% to 9.8% depending on the application."}, {"type": "Payment Terms", "location": "Section 3.1 and 3.2", "confidence": 0.9, "description": "Licensee must pay royalties based on Net Sales, with minimum annual guarantees ranging from $125,000 to $500,000 depending on the sales volume. Royalty payments are due within 30 days of quarterly report submission."}, {"type": "Manufacturing Requirements", "location": "Section 5.1 and 5.2", "confidence": 0.88, "description": "Licensee must manufacture Licensed Products in accordance with Licensor's specifications and quality standards, including ISO 9001:2015 and TS 16949 automotive quality certifications. Licensee must also meet production volume commitments, with a minimum of 50,000 units annually in the first two years."}, {"type": "Licensed Technology", "location": "Section 1.1", "confidence": 0.85, "description": "The agreement grants Licensee rights to use, manufacture, and distribute products incorporating patented technologies, including US Patent 11,247,839 and US Patent 11,089,472."}, {"type": "Termination Conditions", "location": "Section 9.1 and 9.2", "confidence": 0.8, "description": "The agreement can be terminated upon 90 days written notice for material breach. Upon termination, Licensee may continue selling existing inventory for 12 months, subject to continued royalty payments and reporting obligations."}, {"type": "Financial Obligations", "location": "Section 4.1 and 4.2", "confidence": 0.85, "description": "Licensee must pay a one-time license fee of $850,000 and an additional $275,000 for comprehensive technology transfer. Licensee must also pay royalties based on Net Sales, with minimum annual guarantees ranging from $125,000 to $500,000 depending on the sales volume."}, {"type": "Performance Requirements", "location": "Section 5.2 and 5.3", "confidence": 0.8, "description": "Licensee must meet production volume commitments, with a minimum of 50,000 units annually in the first two years. Licensee must also achieve market leadership in the defined automotive transmission component segment."}, {"type": "Territory and Scope", "location": "Section 2.2", "confidence": 0.85, "description": "The agreement grants Licensee exclusive manufacturing rights within the United States, Canada, and Mexico for manufacturing operations, and distribution rights extend to all of North and South America."}]}	2025-11-12 15:32:46.892313
616dd307-b47f-4f94-9b58-ee37b5ea3395	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	insights	[object Object] [object Object]	[-0.0057210443,-0.027690887,-0.026362017,-0.05964637,-0.018033432,-0.023903359,0.024498649,-0.025197392,0.07785308,0.050947107,-0.04039741,-0.090128526,-0.010948332,-0.0025661692,0.024257403,-0.058444154,0.0047887745,0.05237017,-0.06599474,-0.030929899,0.10420531,-0.02872921,-0.049771894,-0.03240712,-0.01516336,0.09229761,-0.016579099,0.028423222,-0.021239558,-0.16478474,-0.050401725,-0.0076541,-0.03385015,0.02916894,0.023839476,0.0038802868,0.0059751663,0.045877986,-0.08392884,0.06081424,0.054558147,0.008995479,-0.03714113,-0.051320236,0.01969138,-0.001147422,-0.057013184,-0.008385133,0.0556479,-0.0330155,-0.070961975,-0.010659587,-0.024792437,0.001221811,0.018144399,0.054582946,0.0629025,0.035505902,-0.003146937,-0.02765939,0.048624672,-0.035810232,-0.09621213,0.046358433,0.020302206,0.003197107,-0.005732629,-0.036999047,-0.05253206,0.029699283,0.036282506,0.03818099,-0.00024645915,0.027267896,0.019410865,-0.043132763,-0.03451179,-0.04877367,0.022092313,0.001729218,-0.03658551,-0.021275075,0.03112768,0.025395598,-0.0022776653,0.0014127406,-0.049370486,-0.076526724,0.03048346,0.061707277,-0.052832823,-0.009360549,0.09698572,0.031243322,-0.017829167,0.00996321,0.0120087145,-0.012966212,-0.008259127,0.37247124,-0.068252526,0.004483522,0.046338707,-0.021378385,-0.029892292,-0.0045614475,-0.008710193,-0.097206175,-0.0137373675,0.049406096,-0.010430672,-0.039357476,-0.006882908,-0.056400526,-0.03606077,-0.013872307,0.05286437,0.00065616757,-0.0048200167,0.009733095,0.012484451,0.0020397275,0.003650991,0.04534244,-0.009199618,-0.06289805,0.010646929,0.11053804,0.04356398,0.056065816,0.045117956,0.038513646,-0.092500366,0.00086546404,0.017862272,-0.00092510815,0.0036994438,0.0071306983,0.03547925,0.009264815,-0.03103319,-0.052254606,0.007827081,-0.00817308,-0.003944388,0.094455756,-0.042233255,0.02583302,-0.017073404,-0.0035536354,-0.008380745,0.07803927,1.6189822e-05,-0.0690013,-0.0066182744,-0.014216141,0.027931107,0.008585887,0.02843959,-0.048511844,-0.067439064,0.049349558,0.014460739,0.07720651,-0.062400877,-0.06838149,-0.0053422046,-0.020028803,0.005141274,-0.035501234,0.0047266777,0.05697333,-0.06676833,-0.0055076308,0.06752239,0.0077382475,-0.060538474,-0.01662626,0.0056340788,0.02056991,0.0054334747,-0.013917437,-0.056685373,0.0089017935,0.04654885,-0.01756945,-0.019551989,-0.05412063,-0.009870688,0.017253384,-0.047740646,0.028004553,-0.031138055,-0.037345853,-0.0851202,-0.031003822,0.04356046,-0.03453924,0.023243029,0.013539229,0.07605596,0.03190329,-0.009178587,0.037068993,-0.040505547,0.0056655854,0.02539819,-0.052648894,0.004548411,0.027683472,-0.07921694,-0.044786226,-0.013592887,0.037216987,-0.03972171,-0.03197329,0.0064076837,0.046620604,0.007995421,-0.02529797,-0.06501259,-0.056427166,-0.029213272,-0.27102172,0.0057979785,0.046694636,-0.041094728,-0.04814579,-0.006404869,0.000788078,0.02580614,-0.04216584,0.041090377,-0.0007918054,-0.0911036,-0.0009502648,-0.019778462,-0.05557245,0.02696773,-0.016357439,-0.03263521,-0.044566978,0.022397302,-0.014408429,-0.0013491695,0.045623958,-0.046845354,-0.02157305,-0.080585234,0.19026133,0.027216954,0.045789495,0.08660852,0.06711191,0.0030985342,0.009203799,-0.08973994,0.03526791,0.018407105,-0.060125116,0.04605819,0.01272002,-0.01161427,0.0071794316,0.04446071,0.023142036,-0.02027418,0.040432584,-0.023537377,0.015037986,-0.031549744,-0.005839236,0.04753956,-0.03263133,-0.017927047,0.038280766,0.029577356,0.019901741,-0.029784665,0.0017886396,-0.028840592,0.008661644,0.08197425,0.025722804,-0.01672059,-0.037619747,-0.018021567,0.041270506,0.003944543,0.011778718,-0.071422115,-0.0056135166,-0.04158126,-0.03601897,0.0791907,0.04886052,-4.3756223e-05,0.016424688,0.058340553,0.017934373,0.025385637,0.030105628,0.016573854,-0.0017931229,-0.008128477,-0.0036652829,0.06253881,0.062363006,-0.013134246,0.059364203,-0.0008104747,-0.012629765,-0.022630539,-0.00053453416,-0.01395162,0.04037205,-0.015969146,0.042606313,-0.04044761,-0.28681797,0.020103147,0.09332038,0.048850223,0.051785592,0.071357936,0.012800526,-0.075454764,-0.0075916955,-0.015053473,0.016352065,0.0483883,0.07849342,0.0029945492,-0.00035652818,0.050109193,0.071891434,-0.032480158,0.040857248,-0.00387888,0.027406272,0.069475934,0.17520405,-0.025335787,0.049322598,-0.0023952245,-0.004154655,-0.0053908583,0.008514138,0.0054244148,-0.051491678,0.013916455,0.10113065,0.0059997197,0.0041204207,0.019828873,0.010226581,-0.011496001,0.034861308,-0.036123786,-0.0017236291,-0.031475194,-0.04606283,0.0077261524,0.050306093,0.0257916,0.0012058889,-0.046696957,0.0037693698,-0.03647139,-0.05531651,-0.05487893,-0.024546191,0.016223049,0.051636994,-0.024830144,-0.034577712,0.0422727,-0.0552802,0.010778012,0.04491665,0.012241778,0.02267415,0.050002564,0.0121696135]	{"insights": [{"type": "opportunity", "title": "Market Leadership Opportunity", "description": "Licensee has the opportunity to achieve market leadership in the defined automotive transmission component segment and receive a $500,000 milestone payment."}, {"type": "alert", "title": "Non-Compliance Risk", "description": "Licensee must ensure compliance with manufacturing requirements to avoid termination of the agreement and loss of exclusive manufacturing rights."}]}	2025-11-12 15:32:47.053552
152dcd55-166b-467f-9add-8fcb6fd2e62c	8f321b79-1ab4-4b02-8806-26db3a21a8ff	summary	This is a comprehensive electronics patent license and royalty agreement between Advanced Chip Technologies Corp. (Licensor) and Nexus Electronics Manufacturing Inc. (Licensee). The agreement grants Licensee non-exclusive rights to manufacture, use, and distribute products incorporating Licensor's patented technologies. The agreement outlines various commercial terms, including royalty structures, payment terms, manufacturing requirements, and termination conditions.	[-0.048414685,0.03742835,0.026708573,-0.04702919,-0.0015120195,-0.06523172,0.05496886,0.06346619,0.006316651,0.02663753,-0.01996578,-0.01453698,0.07424681,-0.10769475,-0.002308136,-0.0277557,-0.0047434326,-0.06575885,0.052812204,0.016746165,0.055153564,-0.056755513,-0.000462582,0.021171693,-0.030385932,0.01506828,0.0020520075,-0.041929852,-0.06238413,-0.14552636,-0.0027926674,0.037427388,0.0019599453,-0.0104263555,-0.018349,-0.034367718,-0.046797406,-0.043809086,-0.006271212,-0.0096406005,-0.0040011373,-0.010253082,-0.02800361,-0.02363202,-0.018725818,-0.045418832,-0.0061480645,-0.00719053,0.028681118,-0.040209148,0.042625047,-0.033078086,0.08126044,-0.015839618,-0.0837707,0.040814422,0.055654027,0.027494475,0.056531254,0.0034754302,0.035411943,-0.015190729,-0.15847291,0.09874104,0.0066808043,0.018522378,-0.008260366,-0.092998356,-0.0595442,-0.028306773,0.04911179,0.03451025,-0.029549316,0.01693227,0.0044950647,0.036738608,0.041954022,-0.052909043,-0.0720977,-0.037399117,0.0022254125,-0.06508907,-0.051841732,-0.059815418,-0.057361238,0.07249388,0.0071153846,-0.011938321,0.043506287,0.055930644,-0.020294053,-0.042338002,-0.025672259,-0.037136022,-0.025588647,0.006215179,0.032449253,-0.02466142,0.011696867,0.33935088,-0.036008384,-0.0002868848,0.006364918,-0.052292757,-0.013395339,0.002288253,0.0017333218,0.014287971,-0.021225736,-0.017435437,-0.020271009,0.041584585,0.05802943,0.002657116,0.021843137,0.005818996,-0.016105652,0.028243173,0.06212846,-0.026417151,0.0027189949,0.06028407,-0.045965556,0.04933326,-0.021376563,-0.045639656,-0.022071958,0.070589505,-0.009990563,0.033292025,0.039812572,-0.049394097,0.024740463,0.054899905,0.06407163,0.014185858,-0.024565525,0.007550125,0.044594273,0.026957126,-0.02537385,0.027056444,-0.02619823,-0.05062355,0.0212806,-0.013938243,0.012016763,0.04316284,-0.053298462,0.025680415,-0.011940293,0.019398622,-0.018217962,-0.0029882395,-0.044645034,0.016154427,0.09085624,0.0054573035,-0.035128098,-0.00089655403,0.03805211,0.0063418723,0.009772584,0.17090328,-0.007759464,-0.16586667,-0.034252755,-0.031007743,0.024531215,-0.0053489143,-0.063636325,-0.0021272262,0.020433558,-0.02939306,-0.01764319,0.035925444,-0.0030949712,0.010363796,-0.035941254,0.031516846,0.022686183,-0.049762495,-0.03707244,-0.029079536,-0.007964509,0.010164748,0.0014352539,0.013342582,-0.0036389958,0.051889326,-0.041948687,0.10211294,-0.020540396,0.13446,0.070093244,-0.058183488,-0.1216379,-0.0013022248,0.051046595,-0.057435296,0.025998278,0.020421816,0.023282273,0.011401422,-0.017116865,-0.01565798,0.058055054,-0.057111293,0.02108442,0.025528487,-0.03827622,-0.0073510185,0.026999736,-0.013182413,0.0032170916,0.02459561,-0.0004325578,0.066275574,-0.027388364,0.025043437,0.009233427,0.0050660498,0.05334929,-0.28294677,0.0353182,-0.019399343,0.04846934,0.0126153985,0.06197224,-0.02977019,0.01737252,-0.050516583,0.053249,0.062369592,0.04854012,0.0007968592,0.03657614,-0.06524214,-0.033313524,0.055605028,-0.04178191,-0.0010481633,0.009938376,-0.04851137,0.050486248,-0.02284291,0.015128725,0.06821341,0.04126878,0.14078388,-0.04377458,0.028433973,-0.0026536945,0.023401627,0.07335258,-0.0073561845,-0.016699575,0.03825915,-0.022408135,-0.020207642,0.02128718,-0.029328374,-0.004520586,-0.07419581,-0.028564092,-0.028989255,-0.107756786,0.040955003,0.0153650725,-0.02656824,0.0399645,-0.014337063,0.06254737,-0.034883223,-0.020560518,-0.0059416723,0.0041607483,0.0071152556,-0.018528404,-0.038618594,0.07276026,-0.0426995,0.045676563,0.06578143,0.023836857,0.019942036,-0.016696095,-0.003457352,-0.024935324,0.012451354,0.0614807,0.048082434,-0.034114163,0.003637329,0.13197933,0.03036561,-0.025634708,0.011484126,0.0133704385,0.028404268,-0.011833776,0.039462354,0.046833906,0.026298907,0.029964807,0.058845967,0.038504787,-0.005286968,-0.0012302034,-0.0021298488,-0.02140082,-0.033523172,-0.04099995,-0.034569927,0.047627114,-0.0073710806,-0.037892155,-0.010572415,0.023290033,-0.2566479,0.04115488,0.0030074494,-0.0050808876,-0.05507005,-0.023641884,-0.028286079,0.023282688,-0.06959282,0.035094097,-0.049829368,-0.00185302,-0.068125166,0.017863976,0.029609159,-0.05756202,0.12697452,-0.08013331,-0.027462235,-0.014806133,0.064510405,0.0055905585,0.1065444,0.0036939692,0.02148439,-0.013349951,-0.05090021,0.06270919,-0.04021885,-0.031987265,-0.031504635,-0.01581116,0.014498435,-0.022612527,0.007863448,0.12386876,-0.008437328,0.008308165,0.0045933262,0.014473245,-0.006347034,-0.026795886,-0.02273563,0.011444977,0.019259151,0.014943213,-0.017639115,-0.011673673,-0.019879643,0.04094417,-0.01527291,-0.033877026,-0.02905822,0.017064214,-0.08858616,-0.056732677,0.02154333,-0.0040221754,0.017270986,0.01489858,-0.021316895,-0.019618299,-0.05381184,0.06923364,0.022674756]	{"type": "summary"}	2025-11-12 15:34:04.219081
f4bf0c95-3abf-4149-9281-e6a7c33ff563	8f321b79-1ab4-4b02-8806-26db3a21a8ff	key_terms	[object Object], [object Object], [object Object], [object Object], [object Object], [object Object], [object Object], [object Object]	[-0.029917,0.010987811,-0.02382003,-0.061188996,0.014624095,-0.00058916677,0.02532213,-0.03843175,0.051462095,0.04100082,-0.02875065,-0.08338046,0.0062819785,-0.0055786488,0.032632545,-0.048051354,0.009598552,0.0607168,-0.08215711,-0.05672461,0.066856034,-0.017484061,-0.048984837,-0.03942319,-0.022723785,0.09761533,0.018220022,-0.0051568085,-0.036149222,-0.16640866,-0.04658365,-0.008414659,-0.01492506,0.028178914,0.020346968,0.017105382,-0.035115868,0.05646424,-0.07629423,0.026530989,0.055580504,-0.004666739,-0.036581688,-0.0601175,-0.0022288181,-0.017897947,-0.04863786,-0.017862258,0.036157183,-0.046498027,-0.07740227,-0.041764606,-0.0421569,0.05305529,0.024652982,0.04214629,0.044218145,0.047905833,-0.017685281,-0.008308193,0.0505556,-0.0018819355,-0.08148973,0.022040136,0.016469738,0.031764984,-0.008076806,-0.026056342,-0.011696514,0.04961493,0.04212531,0.04081058,0.02747504,0.020835932,0.019829746,-0.019216105,-0.035634708,-0.06912418,0.014050619,-0.005806741,-0.036622413,-0.007986283,0.0075921896,0.046868786,-0.008352877,-0.009519124,-0.056342445,-0.0947896,0.014106495,0.06868067,-0.05078709,0.019788243,0.063183434,0.025454415,-0.03410491,-0.017067818,-0.00083573716,-0.05074973,0.021976175,0.36882514,-0.08194472,-0.006157381,0.07370184,-0.031687148,-0.026119256,-0.017343072,-0.012132663,-0.094902985,0.012440363,0.058262404,-0.017435743,-0.064908475,0.032550503,-0.06601702,-0.066719815,-0.046608135,0.05213939,-0.027362848,0.0076824357,-0.008298543,0.010604633,0.011917416,0.035709344,0.024043353,-0.018812606,-0.076978445,0.008864149,0.12597567,0.05827304,0.0754369,0.010102635,0.1021886,-0.061479077,0.002718549,0.008881441,0.021520868,0.0024104689,-0.03197842,0.034504894,0.020508997,-0.04121871,-0.05047906,-0.00807553,-0.03137091,-0.0062890686,0.105400056,-0.046594813,0.02973203,-0.0041234526,0.043172043,0.016922014,0.08997135,0.031346284,-0.040496394,0.00083216955,-0.013534927,0.028431801,-0.0008303692,0.00354938,-0.015719716,-0.08906025,0.01792753,0.02062306,0.13254446,-0.06596746,-0.07543938,0.043838974,-0.020601619,0.0014131707,-0.024242332,-0.013187898,0.061149757,-0.06353913,0.024059206,0.057530444,0.023279943,-0.040468007,-0.012804531,0.0031173355,0.019944595,-0.021030255,-0.001217073,-0.049850058,0.037015066,0.047967643,0.030948201,-0.018281046,-0.033155266,-0.014171544,0.018736083,-0.010153218,0.014036103,-0.053305708,-0.03412274,-0.06789396,-0.048959758,0.026725058,-0.04774828,0.0016826252,-0.0068552285,0.09180729,0.005655985,-0.020669403,0.02557489,-0.011076421,-0.00779846,-0.006079689,-0.031833097,0.045965705,0.0087792175,-0.059230663,-0.023151014,0.014961968,0.02183771,-0.010505906,-0.015970264,0.02258985,0.041594192,-0.0041340888,-0.012155353,-0.056780607,-0.041355416,-0.04913164,-0.24758087,0.019636212,0.058624946,-0.038971152,-0.06776015,-0.0037790139,0.011993083,0.030474426,-0.011886876,0.017604174,-0.008837065,-0.068903185,-0.011050413,0.0017934546,-0.023384595,0.008665375,-0.029449405,-0.018833434,-0.07081523,0.028711826,-0.024277039,0.012482956,0.0362485,-0.036630627,-0.04800316,-0.03283225,0.19355382,0.014289806,0.012972047,0.07425716,0.05833281,0.032264616,0.006739806,-0.024912693,0.060430847,0.036074467,-0.017221687,0.029400352,0.021030555,-0.023370672,0.009454852,0.04467961,0.038449388,-0.04353083,0.03691935,-0.041428585,-0.0036865894,-0.028973734,0.007270557,0.020169685,-0.029109472,-0.039439872,0.056238964,0.00372726,-0.013324681,-0.009786537,-0.0021447516,-0.01766433,-0.020357685,0.09155146,0.02829088,-0.05236269,-0.06139067,0.033471867,0.02526662,-0.028719349,0.025219405,-0.020040449,-0.040859398,-0.04670629,-0.060033523,0.047520418,0.037746597,-0.018171983,0.010867564,0.04676861,0.016740808,0.04092023,0.024474747,0.0024983236,0.028467566,0.024904255,-0.023889054,0.02616694,0.061572235,-0.0048373193,0.038990993,-0.0021647958,0.009257482,-0.006774457,0.022792764,-0.015462018,0.021065056,0.007857894,0.081017725,-0.000396665,-0.3074742,0.02028618,0.07037545,0.048929438,0.065452665,0.048192967,0.011144737,-0.09278719,0.008617589,-0.017847499,0.009542779,0.03640893,0.07306826,-0.039358966,-0.0046897447,0.032846235,0.059392586,-0.050422687,0.045846954,-0.019936793,0.0059435028,0.049781397,0.18020721,-0.04633807,0.053447887,0.008819065,0.0063078096,-0.000364295,-0.00084595906,0.026460083,-0.043970186,-0.01062144,0.096798204,0.029828934,0.020925142,0.057587072,-0.00014328126,-0.018985523,0.011619387,-0.034529526,-0.011662715,-0.013330407,-0.037152883,-0.0015337664,0.00731611,0.03171014,-0.008791078,-0.05598366,-0.0035483136,-0.030346386,-0.05272621,-0.050662883,-0.025806747,-0.0027387817,0.03995339,-0.02852235,0.0018139189,0.048113838,-0.05879055,0.010792592,0.026156757,-0.016638683,0.0059629534,0.03564547,-0.008262379]	{"terms": [{"type": "Royalty Structure", "location": "Section 3.1", "confidence": 0.95, "description": "Licensee shall pay royalties based on Net Sales of Licensed Products according to a tiered structure, with different rates for different product categories and geographic regions."}, {"type": "Payment Terms", "location": "Section 6.2", "confidence": 0.9, "description": "Licensee shall pay royalties quarterly, with the first payment due within 45 days of quarter end. Minimum guarantee payments are also due quarterly in advance."}, {"type": "Manufacturing Requirements", "location": "Section 5.1", "confidence": 0.88, "description": "All Licensed Products must be manufactured in accordance with ISO 9001:2015 quality management systems, ISO/TS 16949 automotive quality standards, IPC-A-610 electronic assembly standards, JEDEC standards for semiconductor reliability testing, and RoHS compliance for all electronic components."}, {"type": "Licensed Technology", "location": "Section 1", "confidence": 0.95, "description": "Licensor grants Licensee non-exclusive rights to manufacture, use, and distribute products incorporating Licensor's patented technologies, including semiconductor and microprocessor patents, circuit design and interface patents, and trade secrets and know-how."}, {"type": "Termination Conditions", "location": "Section 10.1 and 10.2", "confidence": 0.9, "description": "The agreement may be terminated by either party upon material breach, insolvency, bankruptcy, or assignment for benefit of creditors. Upon termination, Licensee may continue selling existing finished goods inventory for 24 months and complete products in various stages of production within 12 months."}, {"type": "Financial Obligations", "location": "Section 4.1 and 4.3", "confidence": 0.95, "description": "Licensee shall pay an initial license fee of $1,250,000 within 45 days of agreement execution. Licensee shall also pay annual support and updates fees of $185,000, and volume milestone bonuses based on cumulative sales achievements."}, {"type": "Performance Requirements", "location": "Section 5.4", "confidence": 0.9, "description": "Licensee shall maintain quality metrics, including defect rate below 50 PPM for all licensed components, customer return rate below 0.1% within 12 months of sale, and compliance with Licensor's electrical and mechanical specifications within ±2% tolerance."}, {"type": "Territory and Scope", "location": "Section 2.2", "confidence": 0.95, "description": "The agreement grants Licensee non-exclusive rights to manufacture, use, and distribute products incorporating Licensor's patented technologies within the United States, Canada, Mexico, European Union, United Kingdom, Australia, New Zealand, Japan, South Korea, and Taiwan."}]}	2025-11-12 15:34:04.364561
4453dfb2-8f6f-4d6a-a07f-06a7aa20fbc7	8f321b79-1ab4-4b02-8806-26db3a21a8ff	insights	[object Object]	[-0.011873694,0.00089998415,0.0028070868,-0.059635602,-0.02447449,-0.015775645,0.017935269,-0.011989422,0.0734046,0.015531356,-0.029456673,-0.08811156,-0.015957853,-0.015907444,0.02769116,-0.05499292,0.016150614,0.04646411,-0.052464467,-0.02699958,0.06872933,-0.019758677,-0.0515321,-0.022943297,-0.04449015,0.0803655,-0.041657507,0.027382117,-0.017584816,-0.13283364,-0.04852705,-0.019810883,0.0059173726,0.04705836,0.036612973,0.0062365825,-0.0075611803,0.042464,-0.066186465,0.03732114,0.05968933,0.014256779,-0.07846066,-0.03212781,0.01885349,-0.0073624593,-0.033728626,0.0011507573,0.0402161,-0.01970507,-0.0881681,-0.0146860555,0.015809922,0.02052696,0.032248102,0.063455924,0.042151272,0.014335432,-0.00525382,-0.016472114,0.029445613,-0.041668806,-0.120930396,0.040556494,0.035660412,0.004969631,0.010544651,-0.03355139,-0.024484396,-0.0049196025,0.019810019,0.040177733,0.007832037,0.036673415,0.0066483673,-0.04156422,-0.008264546,-0.042470433,0.008711306,0.0008908634,-0.040320378,-0.017547796,0.015594784,0.043805663,-0.00078828103,-0.028231924,-0.060815573,-0.09706951,0.04411882,0.03901103,-0.07245892,-0.008147211,0.07877244,0.014341981,-0.0259951,0.00082258164,0.013280836,0.0002038648,-0.0076133446,0.38893145,-0.06663887,0.0074392487,0.06381576,-0.039028477,-0.019653808,0.004074432,-0.01652089,-0.102739096,-0.016523879,0.056675933,-0.019215113,-0.048836384,-0.0030907078,-0.044499893,-0.019207854,-0.018867956,0.043879423,0.006900614,-0.0074079284,0.021592455,0.03015129,0.007895207,-0.018684743,0.04749698,0.006161397,-0.06260755,0.0075976737,0.12676673,0.017472144,0.050619062,0.023027226,0.029922571,-0.085749336,0.0053528473,0.012552611,0.003573785,-0.0117843645,0.013564713,0.010531001,0.012351272,-0.057124488,-0.056565862,-0.02346937,-0.008673141,-0.0095210895,0.081518345,-0.018147834,0.030352684,-0.020396572,-0.015467482,0.0015167699,0.07680113,-0.017804531,-0.07467293,0.0054766284,-0.0026313106,0.022737194,0.004180443,0.050568648,-0.029305287,-0.04667332,0.017266847,-0.0138800545,0.093019605,-0.057761252,-0.0574788,0.02256621,-0.017501138,0.029027272,-0.030295977,0.014002,0.07124106,-0.08657683,0.042453375,0.058343448,-0.012985096,-0.023280084,0.009019841,0.009608072,0.011949109,0.0028138633,-0.017899405,-0.060495116,-6.791353e-05,0.035406757,-0.015521631,0.004625518,-0.036491323,0.0014807858,0.008487391,-0.04373144,0.016760595,-0.027587576,-0.03270951,-0.098231494,-0.04096649,0.03990513,-0.039549693,0.014885319,0.0030529308,0.07992937,0.004566605,-0.03528147,0.02963165,0.0022509915,-0.01403596,0.037252806,-0.03481838,0.015709411,0.025014607,-0.08216597,-0.043148246,0.012375162,0.030239163,0.0023600783,-0.023438994,0.003594233,0.057372503,0.005698606,-0.030922458,-0.05694207,-0.059453238,-0.036753878,-0.30797485,0.021555748,0.020105114,-0.03390628,-0.03663515,0.00979087,0.017506927,0.014657011,-0.005443102,0.039956894,0.005384147,-0.08745356,0.0069078463,-0.011945395,-0.06243401,0.014097948,-0.02164918,-0.039361503,-0.03663265,0.009902569,-0.02368534,-0.01652778,0.031000748,-0.039300915,-0.012911869,-0.100019336,0.20994958,0.037517175,0.033141315,0.056386575,0.055288233,0.008436912,-0.025414053,-0.091902174,0.018970577,0.066776074,-0.03935013,0.07294091,-0.009180844,-0.0025572844,0.012636425,0.0334926,0.017824573,0.01299612,0.021787131,-0.03639417,0.010682066,-0.021082798,-0.020565346,0.0517378,-0.016582513,-0.010615515,0.05323266,0.046902496,0.014703753,-0.014225806,0.011852761,-0.036115848,0.022846313,0.05984027,0.018347932,-0.024770333,-0.031523563,0.0015625628,0.04161475,-0.009819316,0.015003264,-0.091773115,0.0025385562,-0.03561331,-0.015026385,0.07225043,0.042178333,0.026138546,0.021456787,0.05165667,0.04024535,0.018293047,0.025567971,0.010314939,-0.0009246886,0.003757587,-0.015150907,0.06324803,0.04715815,0.007225745,0.045929044,0.0020502154,0.026411654,-0.026251147,-0.0057751527,-0.026377676,0.04693779,-0.003987983,0.02464223,-0.041066747,-0.31949514,0.005258554,0.05299031,0.051465183,0.018901646,0.069050156,0.027640695,-0.03817893,-0.0082040075,-0.028098164,-0.0021518758,0.04338296,0.069428146,-0.0021762138,-0.014748113,0.064080365,0.01149422,-0.04215115,0.04220247,-0.021207038,0.026424326,0.07204864,0.17074299,-0.056417443,0.049185246,-0.017366527,-0.028180232,-0.006957791,0.020119881,0.0048723603,-0.024894696,0.033409253,0.07897891,0.0071359575,0.018436328,0.02385067,0.031360418,-0.006496382,0.032644026,-0.023075227,-0.008730163,0.0031285582,-0.024556225,0.014355755,0.06220648,0.03006301,0.01748438,-0.04953456,0.0075406767,-0.012470166,-0.0283781,-0.0499571,-0.018125579,0.026738506,0.03622109,-0.020622466,-0.05903852,0.031783048,-0.034875985,-0.03702854,0.03964683,0.0059811897,0.034686133,0.0060391566,0.0012919374]	{"insights": [{"type": "opportunity", "title": "Opportunity to Increase Sales and Revenue", "description": "By meeting performance requirements and complying with manufacturing requirements, Licensee may increase sales and revenue by producing high-quality products that meet customer needs and expectations."}]}	2025-11-12 15:34:04.587358
5c5761f3-8e8a-4b5b-8218-4e947755a702	46d0e351-4296-45bb-a680-9ee65497967b	summary	This document outlines a Plant Variety License & Royalty Agreement between Green Innovation Genetics LLC (Licensor) and Heritage Gardens Nursery & Landscaping (Licensee). The agreement grants exclusive regional plant variety rights to the Licensee for a period of eight years, with automatic renewal for successive four-year periods. The Licensee must pay royalties based on net plant sales, with varying rates depending on the type of plant and sales volume.	[-0.052990615,-0.05195359,0.024336694,-0.05492571,0.03426233,0.03829121,-0.021608517,-0.021904113,-0.040431432,0.10006089,-0.051761154,0.016272677,0.039884,-0.005950196,-0.043035906,-0.0052409936,0.0070330435,-0.006884573,-0.03244087,-0.007826058,0.026460156,-0.0069754682,-0.038114022,-0.00651492,-0.05837177,-0.0022297155,-0.07302581,0.0028715038,-0.037666168,-0.16126095,-0.024183195,0.040194165,-0.018166123,-0.0016154386,-0.008277544,-0.015068642,-0.056166414,-0.0076591102,-0.0013370499,0.021379484,0.08234382,-0.015759671,-0.034669045,0.02312595,-0.038421277,0.0040623597,7.438243e-05,0.020689337,-0.009621671,-0.004604024,0.028213847,-0.036913726,-0.00035756137,0.053634185,-0.05298102,0.06326707,0.092262715,0.05609881,-0.0077682296,0.026012164,0.013363056,0.018290116,-0.21837413,0.028303465,-0.036132053,0.007211359,-0.05195273,-0.065425664,-0.0050699287,-0.024556551,0.055685777,0.028869387,-0.051206328,-0.021923272,0.037214126,0.072733074,-0.020238403,-0.019173844,-0.028990084,-0.036691073,-0.03150932,-0.022435533,-0.01686011,-0.06001422,-0.025014529,0.005887088,-0.04060579,0.028354375,0.07699977,0.08217094,0.05347688,-0.061194398,0.013413477,-0.031473808,-0.080266386,0.009642779,0.031638026,-0.026181849,0.010754443,0.33996177,-0.02231958,0.031719577,-0.036287453,0.0029561543,-0.010635079,0.0004433642,-0.08186431,-0.0006813401,-0.008356901,0.004550871,0.0119871115,0.025105625,0.087721236,0.0038425424,-0.05802623,0.025862405,-0.04186858,0.04861747,0.05044127,-0.0060614287,-0.007310211,0.040195264,0.0025678857,0.05040122,-0.04318087,0.035971846,0.009245742,0.040920228,0.044580393,0.07392149,0.037020367,0.06407396,-0.015949598,-0.012690959,-0.014444103,0.002986807,-0.0076249097,0.0047796774,0.031603474,0.021899642,-0.005671306,-0.029588444,-0.011811181,-0.023379646,0.006210908,0.039192036,0.0039622714,0.06840547,-0.028206494,0.08300328,-0.027648585,0.062464472,0.0030774479,-0.013730471,0.021308608,0.093224935,0.07625009,-0.0056455135,0.019083127,-0.004141548,0.020964095,0.05136343,0.022232683,0.09206194,-0.0015557983,-0.13395019,0.022271685,-0.023888513,0.024952881,0.028517082,0.0007650808,0.041139882,0.07288714,-0.022783471,0.031932097,-0.03900635,-0.050280605,0.024823472,0.043560926,0.009036986,0.019344993,-0.0039553265,-0.017647542,-0.002871524,0.010657466,-0.012998631,-0.022922143,0.025077347,-0.04074164,0.03331619,-0.004661103,-0.019703886,-0.031977024,0.08839342,0.015476939,-0.07061199,-0.08545483,0.026834568,0.11693618,-0.025537571,-0.056045555,0.038771965,0.03354119,-0.018733686,-0.017176658,0.0069424636,0.050843116,0.019900823,0.05250159,0.014357209,-0.05673873,0.0500626,0.00061847206,0.010050591,-0.043546267,0.07606003,-0.016765699,0.031239845,0.07006622,0.08496818,-0.0035263058,-0.010141427,0.013727018,-0.3017876,-0.028761884,-0.0558871,0.032238603,0.03682438,0.011334025,-0.052969005,-0.02247808,-0.054186963,-0.008359556,0.058682404,0.00931972,0.009016162,0.023288125,-0.0054948763,-0.019443914,0.10001251,0.013129784,-0.0051049665,-0.03248054,0.01113976,-0.03634394,0.049958427,-0.03110108,0.062052775,-0.01777352,0.10062532,-0.07293826,-0.016970744,-0.028758813,0.02012744,0.00091632106,-0.037468825,-0.049132727,0.025766853,0.01834039,-0.049706466,0.012651889,-0.013942676,-0.009085524,0.011335334,0.017968824,-0.05072538,-0.047194324,0.0039159223,0.008070307,-0.025919806,0.07068363,0.06759286,0.0007626982,-0.00926106,0.013138903,0.04239368,0.05877705,0.0090618385,-0.034583382,-0.053271405,0.07741588,-0.009492259,0.011265521,0.059980173,-0.03593692,0.071088396,-0.056459658,0.03212708,-0.086496696,-0.0065013636,-0.0022351542,0.016907508,-0.037229136,0.029277427,-0.01223301,-0.008125605,-0.055867184,-0.0048291273,0.047882475,-0.018173547,-0.051496208,-0.0017631245,0.013450002,-0.022533732,-0.05020135,0.020557415,0.06930163,-0.058850348,0.022965398,-0.01353602,-0.005855443,0.014990884,-0.03270774,0.016041277,0.049186185,-0.027195565,-0.00817868,0.008570933,-0.010030467,-0.2923716,0.024196193,-0.028995348,0.039879274,-0.007336699,0.028146278,-0.0026185333,0.011564351,-0.035306886,-0.059121214,0.044434085,0.0033584638,-0.0069367196,-0.01733602,0.013592979,0.0037554358,0.09874356,-0.019165032,-0.017397858,-0.060356315,0.06569731,0.01649633,0.10837097,0.035349064,-0.020778421,-0.036478773,-0.04659764,0.034753192,0.012445284,-0.06580919,-0.050166477,0.022026224,0.073080964,-0.0777963,0.034609903,0.06460299,-0.04452689,-0.020577166,-0.03577232,-0.0029295336,-0.05147267,-0.06262168,-0.06188886,0.0035739515,-0.0035001119,-0.049975514,0.0115326,-0.021901999,-0.06441717,0.026969569,0.04695042,-0.015621346,-0.04019136,0.015776489,-0.024490435,-0.030176235,0.039968275,0.00840081,0.020321622,-0.0444512,0.017362455,-0.04934258,-0.041017935,0.11332525,0.011927438]	{"type": "summary"}	2025-11-12 15:36:38.518927
5619b179-7d7c-4e1e-ac8f-918a119cad3a	46d0e351-4296-45bb-a680-9ee65497967b	key_terms	[object Object], [object Object], [object Object], [object Object], [object Object], [object Object], [object Object], [object Object]	[-0.029917,0.010987811,-0.02382003,-0.061188996,0.014624095,-0.00058916677,0.02532213,-0.03843175,0.051462095,0.04100082,-0.02875065,-0.08338046,0.0062819785,-0.0055786488,0.032632545,-0.048051354,0.009598552,0.0607168,-0.08215711,-0.05672461,0.066856034,-0.017484061,-0.048984837,-0.03942319,-0.022723785,0.09761533,0.018220022,-0.0051568085,-0.036149222,-0.16640866,-0.04658365,-0.008414659,-0.01492506,0.028178914,0.020346968,0.017105382,-0.035115868,0.05646424,-0.07629423,0.026530989,0.055580504,-0.004666739,-0.036581688,-0.0601175,-0.0022288181,-0.017897947,-0.04863786,-0.017862258,0.036157183,-0.046498027,-0.07740227,-0.041764606,-0.0421569,0.05305529,0.024652982,0.04214629,0.044218145,0.047905833,-0.017685281,-0.008308193,0.0505556,-0.0018819355,-0.08148973,0.022040136,0.016469738,0.031764984,-0.008076806,-0.026056342,-0.011696514,0.04961493,0.04212531,0.04081058,0.02747504,0.020835932,0.019829746,-0.019216105,-0.035634708,-0.06912418,0.014050619,-0.005806741,-0.036622413,-0.007986283,0.0075921896,0.046868786,-0.008352877,-0.009519124,-0.056342445,-0.0947896,0.014106495,0.06868067,-0.05078709,0.019788243,0.063183434,0.025454415,-0.03410491,-0.017067818,-0.00083573716,-0.05074973,0.021976175,0.36882514,-0.08194472,-0.006157381,0.07370184,-0.031687148,-0.026119256,-0.017343072,-0.012132663,-0.094902985,0.012440363,0.058262404,-0.017435743,-0.064908475,0.032550503,-0.06601702,-0.066719815,-0.046608135,0.05213939,-0.027362848,0.0076824357,-0.008298543,0.010604633,0.011917416,0.035709344,0.024043353,-0.018812606,-0.076978445,0.008864149,0.12597567,0.05827304,0.0754369,0.010102635,0.1021886,-0.061479077,0.002718549,0.008881441,0.021520868,0.0024104689,-0.03197842,0.034504894,0.020508997,-0.04121871,-0.05047906,-0.00807553,-0.03137091,-0.0062890686,0.105400056,-0.046594813,0.02973203,-0.0041234526,0.043172043,0.016922014,0.08997135,0.031346284,-0.040496394,0.00083216955,-0.013534927,0.028431801,-0.0008303692,0.00354938,-0.015719716,-0.08906025,0.01792753,0.02062306,0.13254446,-0.06596746,-0.07543938,0.043838974,-0.020601619,0.0014131707,-0.024242332,-0.013187898,0.061149757,-0.06353913,0.024059206,0.057530444,0.023279943,-0.040468007,-0.012804531,0.0031173355,0.019944595,-0.021030255,-0.001217073,-0.049850058,0.037015066,0.047967643,0.030948201,-0.018281046,-0.033155266,-0.014171544,0.018736083,-0.010153218,0.014036103,-0.053305708,-0.03412274,-0.06789396,-0.048959758,0.026725058,-0.04774828,0.0016826252,-0.0068552285,0.09180729,0.005655985,-0.020669403,0.02557489,-0.011076421,-0.00779846,-0.006079689,-0.031833097,0.045965705,0.0087792175,-0.059230663,-0.023151014,0.014961968,0.02183771,-0.010505906,-0.015970264,0.02258985,0.041594192,-0.0041340888,-0.012155353,-0.056780607,-0.041355416,-0.04913164,-0.24758087,0.019636212,0.058624946,-0.038971152,-0.06776015,-0.0037790139,0.011993083,0.030474426,-0.011886876,0.017604174,-0.008837065,-0.068903185,-0.011050413,0.0017934546,-0.023384595,0.008665375,-0.029449405,-0.018833434,-0.07081523,0.028711826,-0.024277039,0.012482956,0.0362485,-0.036630627,-0.04800316,-0.03283225,0.19355382,0.014289806,0.012972047,0.07425716,0.05833281,0.032264616,0.006739806,-0.024912693,0.060430847,0.036074467,-0.017221687,0.029400352,0.021030555,-0.023370672,0.009454852,0.04467961,0.038449388,-0.04353083,0.03691935,-0.041428585,-0.0036865894,-0.028973734,0.007270557,0.020169685,-0.029109472,-0.039439872,0.056238964,0.00372726,-0.013324681,-0.009786537,-0.0021447516,-0.01766433,-0.020357685,0.09155146,0.02829088,-0.05236269,-0.06139067,0.033471867,0.02526662,-0.028719349,0.025219405,-0.020040449,-0.040859398,-0.04670629,-0.060033523,0.047520418,0.037746597,-0.018171983,0.010867564,0.04676861,0.016740808,0.04092023,0.024474747,0.0024983236,0.028467566,0.024904255,-0.023889054,0.02616694,0.061572235,-0.0048373193,0.038990993,-0.0021647958,0.009257482,-0.006774457,0.022792764,-0.015462018,0.021065056,0.007857894,0.081017725,-0.000396665,-0.3074742,0.02028618,0.07037545,0.048929438,0.065452665,0.048192967,0.011144737,-0.09278719,0.008617589,-0.017847499,0.009542779,0.03640893,0.07306826,-0.039358966,-0.0046897447,0.032846235,0.059392586,-0.050422687,0.045846954,-0.019936793,0.0059435028,0.049781397,0.18020721,-0.04633807,0.053447887,0.008819065,0.0063078096,-0.000364295,-0.00084595906,0.026460083,-0.043970186,-0.01062144,0.096798204,0.029828934,0.020925142,0.057587072,-0.00014328126,-0.018985523,0.011619387,-0.034529526,-0.011662715,-0.013330407,-0.037152883,-0.0015337664,0.00731611,0.03171014,-0.008791078,-0.05598366,-0.0035483136,-0.030346386,-0.05272621,-0.050662883,-0.025806747,-0.0027387817,0.03995339,-0.02852235,0.0018139189,0.048113838,-0.05879055,0.010792592,0.026156757,-0.016638683,0.0059629534,0.03564547,-0.008262379]	{"terms": [{"type": "Royalty Structure", "location": "Section 3.1", "confidence": 0.95, "description": "The Licensee must pay royalties based on net plant sales, with varying rates depending on the type of plant and sales volume. For example, for Tier 1 - Ornamental Trees & Shrubs, the royalty rate is $1.25 per unit for 1-gallon containers, with a discount rate of $1.10 per unit for 5,000+ units annually."}, {"type": "Payment Terms", "location": "Section 6.2", "confidence": 0.9, "description": "The Licensee must pay royalties within 45 days of the quarter end, with a late payment charge of 1.25% per month. The Licensee must also pay an annual certification and inspection fee of $12,500."}, {"type": "Manufacturing Requirements", "location": "Section 5.1", "confidence": 0.88, "description": "The Licensee must meet minimum annual production levels, with a minimum of 15,000 total plants across all varieties in Years 1-2, and a minimum of 35,000 total plants with 20% year-over-year growth in Years 3-4."}, {"type": "Licensed Technology & Patents", "location": "Section 1.2", "confidence": 0.85, "description": "The Licensor grants the Licensee access to proprietary propagation methods covered by US Patent 11,456,892 and US Patent 11,234,567, as well as trade secret 'GreenGrow™ Nutrient Formula and Application Methods'."}, {"type": "Termination & Post-Termination", "location": "Section 9.1", "confidence": 0.8, "description": "Either party may terminate the agreement upon 180 days written notice for material breach, or upon 12 months notice for convenience. Upon termination, the Licensee may sell existing finished inventory for 18 months subject to continued royalty payments."}, {"type": "Financial Obligations", "location": "Section 4.1", "confidence": 0.85, "description": "The Licensee must pay an initial license fee of $125,000, as well as an annual certification and inspection fee of $12,500. The Licensee must also guarantee minimum annual royalty payments totaling $85,000."}, {"type": "Performance Requirements", "location": "Section 5.1", "confidence": 0.88, "description": "The Licensee must meet minimum annual production levels, with a minimum of 15,000 total plants across all varieties in Years 1-2, and a minimum of 35,000 total plants with 20% year-over-year growth in Years 3-4."}, {"type": "Territory & Scope", "location": "Section 2.1", "confidence": 0.9, "description": "The agreement grants exclusive regional plant variety rights to the Licensee within the following geographic region: Oregon, Washington, Northern California (counties north of Fresno), and Idaho."}]}	2025-11-12 15:36:38.664372
88e1f2b9-8a86-47b4-92a7-5abc1649b6a1	46d0e351-4296-45bb-a680-9ee65497967b	insights	[object Object]	[-0.011873694,0.00089998415,0.0028070868,-0.059635602,-0.02447449,-0.015775645,0.017935269,-0.011989422,0.0734046,0.015531356,-0.029456673,-0.08811156,-0.015957853,-0.015907444,0.02769116,-0.05499292,0.016150614,0.04646411,-0.052464467,-0.02699958,0.06872933,-0.019758677,-0.0515321,-0.022943297,-0.04449015,0.0803655,-0.041657507,0.027382117,-0.017584816,-0.13283364,-0.04852705,-0.019810883,0.0059173726,0.04705836,0.036612973,0.0062365825,-0.0075611803,0.042464,-0.066186465,0.03732114,0.05968933,0.014256779,-0.07846066,-0.03212781,0.01885349,-0.0073624593,-0.033728626,0.0011507573,0.0402161,-0.01970507,-0.0881681,-0.0146860555,0.015809922,0.02052696,0.032248102,0.063455924,0.042151272,0.014335432,-0.00525382,-0.016472114,0.029445613,-0.041668806,-0.120930396,0.040556494,0.035660412,0.004969631,0.010544651,-0.03355139,-0.024484396,-0.0049196025,0.019810019,0.040177733,0.007832037,0.036673415,0.0066483673,-0.04156422,-0.008264546,-0.042470433,0.008711306,0.0008908634,-0.040320378,-0.017547796,0.015594784,0.043805663,-0.00078828103,-0.028231924,-0.060815573,-0.09706951,0.04411882,0.03901103,-0.07245892,-0.008147211,0.07877244,0.014341981,-0.0259951,0.00082258164,0.013280836,0.0002038648,-0.0076133446,0.38893145,-0.06663887,0.0074392487,0.06381576,-0.039028477,-0.019653808,0.004074432,-0.01652089,-0.102739096,-0.016523879,0.056675933,-0.019215113,-0.048836384,-0.0030907078,-0.044499893,-0.019207854,-0.018867956,0.043879423,0.006900614,-0.0074079284,0.021592455,0.03015129,0.007895207,-0.018684743,0.04749698,0.006161397,-0.06260755,0.0075976737,0.12676673,0.017472144,0.050619062,0.023027226,0.029922571,-0.085749336,0.0053528473,0.012552611,0.003573785,-0.0117843645,0.013564713,0.010531001,0.012351272,-0.057124488,-0.056565862,-0.02346937,-0.008673141,-0.0095210895,0.081518345,-0.018147834,0.030352684,-0.020396572,-0.015467482,0.0015167699,0.07680113,-0.017804531,-0.07467293,0.0054766284,-0.0026313106,0.022737194,0.004180443,0.050568648,-0.029305287,-0.04667332,0.017266847,-0.0138800545,0.093019605,-0.057761252,-0.0574788,0.02256621,-0.017501138,0.029027272,-0.030295977,0.014002,0.07124106,-0.08657683,0.042453375,0.058343448,-0.012985096,-0.023280084,0.009019841,0.009608072,0.011949109,0.0028138633,-0.017899405,-0.060495116,-6.791353e-05,0.035406757,-0.015521631,0.004625518,-0.036491323,0.0014807858,0.008487391,-0.04373144,0.016760595,-0.027587576,-0.03270951,-0.098231494,-0.04096649,0.03990513,-0.039549693,0.014885319,0.0030529308,0.07992937,0.004566605,-0.03528147,0.02963165,0.0022509915,-0.01403596,0.037252806,-0.03481838,0.015709411,0.025014607,-0.08216597,-0.043148246,0.012375162,0.030239163,0.0023600783,-0.023438994,0.003594233,0.057372503,0.005698606,-0.030922458,-0.05694207,-0.059453238,-0.036753878,-0.30797485,0.021555748,0.020105114,-0.03390628,-0.03663515,0.00979087,0.017506927,0.014657011,-0.005443102,0.039956894,0.005384147,-0.08745356,0.0069078463,-0.011945395,-0.06243401,0.014097948,-0.02164918,-0.039361503,-0.03663265,0.009902569,-0.02368534,-0.01652778,0.031000748,-0.039300915,-0.012911869,-0.100019336,0.20994958,0.037517175,0.033141315,0.056386575,0.055288233,0.008436912,-0.025414053,-0.091902174,0.018970577,0.066776074,-0.03935013,0.07294091,-0.009180844,-0.0025572844,0.012636425,0.0334926,0.017824573,0.01299612,0.021787131,-0.03639417,0.010682066,-0.021082798,-0.020565346,0.0517378,-0.016582513,-0.010615515,0.05323266,0.046902496,0.014703753,-0.014225806,0.011852761,-0.036115848,0.022846313,0.05984027,0.018347932,-0.024770333,-0.031523563,0.0015625628,0.04161475,-0.009819316,0.015003264,-0.091773115,0.0025385562,-0.03561331,-0.015026385,0.07225043,0.042178333,0.026138546,0.021456787,0.05165667,0.04024535,0.018293047,0.025567971,0.010314939,-0.0009246886,0.003757587,-0.015150907,0.06324803,0.04715815,0.007225745,0.045929044,0.0020502154,0.026411654,-0.026251147,-0.0057751527,-0.026377676,0.04693779,-0.003987983,0.02464223,-0.041066747,-0.31949514,0.005258554,0.05299031,0.051465183,0.018901646,0.069050156,0.027640695,-0.03817893,-0.0082040075,-0.028098164,-0.0021518758,0.04338296,0.069428146,-0.0021762138,-0.014748113,0.064080365,0.01149422,-0.04215115,0.04220247,-0.021207038,0.026424326,0.07204864,0.17074299,-0.056417443,0.049185246,-0.017366527,-0.028180232,-0.006957791,0.020119881,0.0048723603,-0.024894696,0.033409253,0.07897891,0.0071359575,0.018436328,0.02385067,0.031360418,-0.006496382,0.032644026,-0.023075227,-0.008730163,0.0031285582,-0.024556225,0.014355755,0.06220648,0.03006301,0.01748438,-0.04953456,0.0075406767,-0.012470166,-0.0283781,-0.0499571,-0.018125579,0.026738506,0.03622109,-0.020622466,-0.05903852,0.031783048,-0.034875985,-0.03702854,0.03964683,0.0059811897,0.034686133,0.0060391566,0.0012919374]	{"insights": [{"type": "opportunity", "title": "Increased Sales Potential", "description": "The Licensee has the opportunity to increase sales by meeting minimum production levels and quality standards, and by taking advantage of the exclusive regional plant variety rights granted by the agreement."}]}	2025-11-12 15:36:38.797066
\.


--
-- Data for Name: contract_graph_edges; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_graph_edges (id, contract_id, extraction_run_id, source_node_id, target_node_id, relationship_type, properties, confidence, created_at) FROM stdin;
\.


--
-- Data for Name: contract_graph_nodes; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_graph_nodes (id, contract_id, extraction_run_id, node_type, label, properties, confidence, source_document_id, source_text, embedding, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: contract_obligations; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_obligations (id, contract_id, obligation_type, description, due_date, responsible, status, priority, completion_date, notes, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: contract_royalty_calculations; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_royalty_calculations (id, contract_id, name, period_start, period_end, status, total_sales_amount, total_royalty, currency, sales_count, breakdown, chart_data, calculated_by, approved_by, approved_at, rejected_by, rejected_at, rejection_reason, comments, created_at, updated_at) FROM stdin;
dc47ba27-bc00-446a-80b5-c96813c1b4a2	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	Calculation 11/12/2025	\N	\N	pending_approval	106347000.00	7168412.00	USD	20	"[{\\"saleId\\":\\"d94a3f5f-e49e-4d7c-aaa8-9ea80f09ce12\\",\\"productName\\":\\"High-Temp Polymer Coating HT-200\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"United States\\",\\"quantity\\":64000,\\"saleAmount\\":7680000,\\"royaltyAmount\\":\\"499200\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"1a03a986-afdb-4776-a7d1-d2ac05a68334\\",\\"productName\\":\\"Composite Bearing CB-500\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"Mexico\\",\\"quantity\\":82000,\\"saleAmount\\":6560000,\\"royaltyAmount\\":\\"426400\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"57035850-2c1d-40f8-b5fd-fc036eef1edf\\",\\"productName\\":\\"Aerospace Composite AC-1000\\",\\"category\\":\\"Aerospace-grade composite materials\\",\\"territory\\":\\"United States\\",\\"quantity\\":4200,\\"saleAmount\\":5376000,\\"royaltyAmount\\":\\"387072.00000000006\\",\\"ruleApplied\\":\\"Industrial & Aerospace Components Royalty Rate Structure\\",\\"explanation\\":\\"7.2% of gross sales\\",\\"tierRate\\":7.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"cb923af0-ea03-4f82-848a-16ae68b21389\\",\\"productName\\":\\"Industrial Bearing IB-750\\",\\"category\\":\\"Industrial machinery parts\\",\\"territory\\":\\"United States\\",\\"quantity\\":22000,\\"saleAmount\\":5830000,\\"royaltyAmount\\":\\"419760.00000000006\\",\\"ruleApplied\\":\\"Industrial & Aerospace Components Royalty Rate Structure\\",\\"explanation\\":\\"7.2% of gross sales\\",\\"tierRate\\":7.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"3e640420-9d5c-4e13-929d-c0f7523af239\\",\\"productName\\":\\"Smart Material Component SM-100\\",\\"category\\":\\"Industrial machinery parts\\",\\"territory\\":\\"United States\\",\\"quantity\\":32000,\\"saleAmount\\":6240000,\\"royaltyAmount\\":\\"449280.00000000006\\",\\"ruleApplied\\":\\"Industrial & Aerospace Components Royalty Rate Structure\\",\\"explanation\\":\\"7.2% of gross sales\\",\\"tierRate\\":7.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"ce988fcc-62db-4f13-9c8a-a13ccd9ec769\\",\\"productName\\":\\"Precision Molded Component PM-350\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"Canada\\",\\"quantity\\":68000,\\"saleAmount\\":6256000,\\"royaltyAmount\\":\\"406640\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"afec807f-1f20-4065-8eaf-d6f8935c3dad\\",\\"productName\\":\\"High-Temp Polymer Coating HT-200\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"United States\\",\\"quantity\\":55000,\\"saleAmount\\":6710000,\\"royaltyAmount\\":\\"436150\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"1cb614e3-205e-45e3-b020-79cf75468414\\",\\"productName\\":\\"Composite Bearing CB-500\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"United States\\",\\"quantity\\":75000,\\"saleAmount\\":6150000,\\"royaltyAmount\\":\\"399750\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"79b7e744-16e8-42de-b3cd-bcd983838471\\",\\"productName\\":\\"Aerospace Composite AC-1000\\",\\"category\\":\\"Aerospace-grade composite materials\\",\\"territory\\":\\"United States\\",\\"quantity\\":2800,\\"saleAmount\\":3780000,\\"royaltyAmount\\":\\"272160.00000000006\\",\\"ruleApplied\\":\\"Industrial & Aerospace Components Royalty Rate Structure\\",\\"explanation\\":\\"7.2% of gross sales\\",\\"tierRate\\":7.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"98249528-1320-441b-aa72-39cec1567888\\",\\"productName\\":\\"Industrial Bearing IB-750\\",\\"category\\":\\"Industrial machinery parts\\",\\"territory\\":\\"Mexico\\",\\"quantity\\":18000,\\"saleAmount\\":4500000,\\"royaltyAmount\\":\\"324000.00000000006\\",\\"ruleApplied\\":\\"Industrial & Aerospace Components Royalty Rate Structure\\",\\"explanation\\":\\"7.2% of gross sales\\",\\"tierRate\\":7.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"7f48e1f9-38ea-4c50-bfa6-ae5b26c22a8b\\",\\"productName\\":\\"Smart Material Component SM-100\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"United States\\",\\"quantity\\":35000,\\"saleAmount\\":6475000,\\"royaltyAmount\\":\\"420875\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"0485f463-ea2d-4435-b27d-bfe33de06c39\\",\\"productName\\":\\"Precision Molded Component PM-350\\",\\"category\\":\\"Industrial machinery parts\\",\\"territory\\":\\"Canada\\",\\"quantity\\":25000,\\"saleAmount\\":2875000,\\"royaltyAmount\\":\\"207000.00000000003\\",\\"ruleApplied\\":\\"Industrial & Aerospace Components Royalty Rate Structure\\",\\"explanation\\":\\"7.2% of gross sales\\",\\"tierRate\\":7.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"7b163ffe-0bc9-43c5-a05b-e6b8880d4ba3\\",\\"productName\\":\\"High-Temp Polymer Coating HT-200\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"United States\\",\\"quantity\\":48000,\\"saleAmount\\":6000000,\\"royaltyAmount\\":\\"390000\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"4c5cce3d-b15b-4648-89df-be7f42525a38\\",\\"productName\\":\\"Composite Bearing CB-500\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"United States\\",\\"quantity\\":62000,\\"saleAmount\\":5270000,\\"royaltyAmount\\":\\"342550\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"fc881b3a-455c-4899-b868-18db7ff31cde\\",\\"productName\\":\\"Aerospace Composite AC-1000\\",\\"category\\":\\"Aerospace-grade composite materials\\",\\"territory\\":\\"United States\\",\\"quantity\\":3500,\\"saleAmount\\":4200000,\\"royaltyAmount\\":\\"302400.00000000006\\",\\"ruleApplied\\":\\"Industrial & Aerospace Components Royalty Rate Structure\\",\\"explanation\\":\\"7.2% of gross sales\\",\\"tierRate\\":7.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"d32d5461-303d-48d6-91e0-fce1a4776082\\",\\"productName\\":\\"Industrial Bearing IB-750\\",\\"category\\":\\"Industrial machinery parts\\",\\"territory\\":\\"United States\\",\\"quantity\\":15000,\\"saleAmount\\":3750000,\\"royaltyAmount\\":\\"270000.00000000006\\",\\"ruleApplied\\":\\"Industrial & Aerospace Components Royalty Rate Structure\\",\\"explanation\\":\\"7.2% of gross sales\\",\\"tierRate\\":7.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"2a1e6a68-7db7-4c43-8674-c3e81f18a40d\\",\\"productName\\":\\"Smart Material Component SM-100\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"Mexico\\",\\"quantity\\":28000,\\"saleAmount\\":5180000,\\"royaltyAmount\\":\\"336700\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"81b906c3-5f6f-4fa7-9734-17e35e166269\\",\\"productName\\":\\"Precision Molded Component PM-350\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"Canada\\",\\"quantity\\":52000,\\"saleAmount\\":4940000,\\"royaltyAmount\\":\\"321100\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"d9b447d4-b224-4a3c-b49f-d1ee8ac048c4\\",\\"productName\\":\\"High-Temp Polymer Coating HT-200\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"United States\\",\\"quantity\\":38000,\\"saleAmount\\":4750000,\\"royaltyAmount\\":\\"308750\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"c891769b-e001-4628-aa9b-a15f4ab7ad99\\",\\"productName\\":\\"Composite Bearing CB-500\\",\\"category\\":\\"Automotive transmission components\\",\\"territory\\":\\"United States\\",\\"quantity\\":45000,\\"saleAmount\\":3825000,\\"royaltyAmount\\":\\"248625\\",\\"ruleApplied\\":\\"Automotive Components Royalty Rate Structure\\",\\"explanation\\":\\"6.5% of gross sales\\",\\"tierRate\\":6.5,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1}]"	"{\\"minimumGuarantee\\":125000,\\"calculatedRoyalty\\":7168412,\\"rulesApplied\\":[\\"Automotive Components Royalty Rate Structure\\",\\"Industrial & Aerospace Components Royalty Rate Structure\\"]}"	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	\N	\N	\N	\N	\N	\N	2025-11-12 15:33:11.134406	2025-11-12 15:33:11.134406
8bfa672a-3d6c-4a05-b434-e6f5c46b78c5	8f321b79-1ab4-4b02-8806-26db3a21a8ff	Calculation 11/12/2025	\N	\N	pending_approval	285072500.00	183175.00	USD	20	"[{\\"saleId\\":\\"da5d6136-8caa-4797-be8e-234f89b8e842\\",\\"productName\\":\\"Medical Monitor MM300\\",\\"category\\":\\"Medical Device Electronics\\",\\"territory\\":\\"Japan\\",\\"quantity\\":12000,\\"saleAmount\\":18000000,\\"royaltyAmount\\":\\"36000\\",\\"ruleApplied\\":\\"Tier 3 Market Adjustment\\",\\"explanation\\":\\"0.2% of gross sales\\",\\"tierRate\\":0.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"bf99be26-1c73-4903-aa79-d39e992ce164\\",\\"productName\\":\\"Auto Infotainment AI800\\",\\"category\\":\\"Automotive Electronics\\",\\"territory\\":\\"European Union\\",\\"quantity\\":18000,\\"saleAmount\\":17595000,\\"royaltyAmount\\":\\"17595\\",\\"ruleApplied\\":\\"Tier 2 Market Adjustment\\",\\"explanation\\":\\"0.1% of gross sales\\",\\"tierRate\\":0.1,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"d8b1e3c9-af61-4cb3-b311-9f3476394186\\",\\"productName\\":\\"Fitness Tracker FT500\\",\\"category\\":\\"Wearable Electronics\\",\\"territory\\":\\"South Korea\\",\\"quantity\\":140000,\\"saleAmount\\":26600000,\\"royaltyAmount\\":\\"53200\\",\\"ruleApplied\\":\\"Tier 3 Market Adjustment\\",\\"explanation\\":\\"0.2% of gross sales\\",\\"tierRate\\":0.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"a3c16886-b0ef-4d79-b4ca-697ce8ab963f\\",\\"productName\\":\\"Smart Home Hub SH200\\",\\"category\\":\\"Smart Home Devices\\",\\"territory\\":\\"European Union\\",\\"quantity\\":95000,\\"saleAmount\\":12825000,\\"royaltyAmount\\":\\"12825\\",\\"ruleApplied\\":\\"Tier 2 Market Adjustment\\",\\"explanation\\":\\"0.1% of gross sales\\",\\"tierRate\\":0.1,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"bd7267da-2a5b-478b-a10a-e000d6500140\\",\\"productName\\":\\"Premium Tablet T10\\",\\"category\\":\\"Tablets and E-Readers\\",\\"territory\\":\\"Japan\\",\\"quantity\\":35000,\\"saleAmount\\":16800000,\\"royaltyAmount\\":\\"33600\\",\\"ruleApplied\\":\\"Tier 3 Market Adjustment\\",\\"explanation\\":\\"0.2% of gross sales\\",\\"tierRate\\":0.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"e16ae15e-babb-4f25-89c2-d35728370eb3\\",\\"productName\\":\\"Power Management IC PM500\\",\\"category\\":\\"Power Management ICs\\",\\"territory\\":\\"European Union\\",\\"quantity\\":1800000,\\"saleAmount\\":2610000,\\"royaltyAmount\\":\\"2610\\",\\"ruleApplied\\":\\"Tier 2 Market Adjustment\\",\\"explanation\\":\\"0.1% of gross sales\\",\\"tierRate\\":0.1,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"bbd7de79-f054-4a8a-8c47-1f8f7d1a60e2\\",\\"productName\\":\\"ARM Processor A72\\",\\"category\\":\\"ARM-Compatible Processors\\",\\"territory\\":\\"Japan\\",\\"quantity\\":850000,\\"saleAmount\\":2422500,\\"royaltyAmount\\":\\"4845\\",\\"ruleApplied\\":\\"Tier 3 Market Adjustment\\",\\"explanation\\":\\"0.2% of gross sales\\",\\"tierRate\\":0.2,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"0e635951-9f76-4a60-864b-63c4865e696b\\",\\"productName\\":\\"Premium Tablet T10\\",\\"category\\":\\"Tablets and E-Readers\\",\\"territory\\":\\"European Union\\",\\"quantity\\":50000,\\"saleAmount\\":22500000,\\"royaltyAmount\\":\\"22500\\",\\"ruleApplied\\":\\"Tier 2 Market Adjustment\\",\\"explanation\\":\\"0.1% of gross sales\\",\\"tierRate\\":0.1,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1}]"	"{\\"minimumGuarantee\\":null,\\"calculatedRoyalty\\":183175,\\"rulesApplied\\":[\\"Tier 3 Market Adjustment\\",\\"Tier 2 Market Adjustment\\"]}"	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	\N	\N	\N	\N	\N	\N	2025-11-12 15:35:54.013257	2025-11-12 15:35:54.013257
9a79c31d-4ec1-4367-9d25-39923cd24fb8	46d0e351-4296-45bb-a680-9ee65497967b	Calculation 11/12/2025	\N	\N	pending_approval	384000.00	85000.00	USD	15	"[{\\"saleId\\":\\"411b8a4d-f6bb-4bef-a206-590e2187d37f\\",\\"productName\\":\\"Pacific Sunset Rose\\",\\"category\\":\\"Flowering Shrubs\\",\\"territory\\":\\"Primary\\",\\"quantity\\":250,\\"saleAmount\\":5000,\\"royaltyAmount\\":\\"37.5\\",\\"ruleApplied\\":\\"Perennials & Roses Royalty Structure\\",\\"explanation\\":\\"0.75% of gross sales\\",\\"tierRate\\":0.75,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"969f398d-a738-4811-8432-76e4b17ca68c\\",\\"productName\\":\\"Emerald Crown Hosta\\",\\"category\\":\\"Perennials\\",\\"territory\\":\\"Secondary\\",\\"quantity\\":750,\\"saleAmount\\":20000,\\"royaltyAmount\\":\\"150\\",\\"ruleApplied\\":\\"Perennials & Roses Royalty Structure\\",\\"explanation\\":\\"0.75% of gross sales\\",\\"tierRate\\":0.75,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"332e4927-6958-4381-b0ec-a3af508c88c1\\",\\"productName\\":\\"Golden Spire Juniper\\",\\"category\\":\\"Ornamental Shrubs\\",\\"territory\\":\\"Primary\\",\\"quantity\\":1600,\\"saleAmount\\":8000,\\"royaltyAmount\\":\\"100\\",\\"ruleApplied\\":\\"Ornamental Trees & Shrubs Royalty Structure\\",\\"explanation\\":\\"1.25% of gross sales\\",\\"tierRate\\":1.25,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"f293a77a-6023-44d8-85f8-af96e607a620\\",\\"productName\\":\\"Aurora Flame Maple\\",\\"category\\":\\"Ornamental Trees\\",\\"territory\\":\\"Primary\\",\\"quantity\\":2000,\\"saleAmount\\":35000,\\"royaltyAmount\\":\\"437.5\\",\\"ruleApplied\\":\\"Ornamental Trees & Shrubs Royalty Structure\\",\\"explanation\\":\\"1.25% of gross sales\\",\\"tierRate\\":1.25,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"b3e8d2e1-f30d-4b9d-b4bb-daa4a7381d9f\\",\\"productName\\":\\"Golden Spire Juniper\\",\\"category\\":\\"Ornamental Shrubs\\",\\"territory\\":\\"Secondary\\",\\"quantity\\":1800,\\"saleAmount\\":28000,\\"royaltyAmount\\":\\"350\\",\\"ruleApplied\\":\\"Ornamental Trees & Shrubs Royalty Structure\\",\\"explanation\\":\\"1.25% of gross sales\\",\\"tierRate\\":1.25,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"75e29720-bd56-475c-8293-46cfdb4ee400\\",\\"productName\\":\\"Emerald Crown Hosta\\",\\"category\\":\\"Perennials\\",\\"territory\\":\\"Primary\\",\\"quantity\\":900,\\"saleAmount\\":18000,\\"royaltyAmount\\":\\"135\\",\\"ruleApplied\\":\\"Perennials & Roses Royalty Structure\\",\\"explanation\\":\\"0.75% of gross sales\\",\\"tierRate\\":0.75,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"04736c11-e14d-4a67-a7cb-d9172657c0ed\\",\\"productName\\":\\"Pacific Sunset Rose\\",\\"category\\":\\"Flowering Shrubs\\",\\"territory\\":\\"Secondary\\",\\"quantity\\":1000,\\"saleAmount\\":16000,\\"royaltyAmount\\":\\"120\\",\\"ruleApplied\\":\\"Perennials & Roses Royalty Structure\\",\\"explanation\\":\\"0.75% of gross sales\\",\\"tierRate\\":0.75,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"a21eb4a5-0aad-4dcd-9223-1022f89b8168\\",\\"productName\\":\\"Emerald Crown Hosta\\",\\"category\\":\\"Perennials\\",\\"territory\\":\\"Primary\\",\\"quantity\\":1500,\\"saleAmount\\":12000,\\"royaltyAmount\\":\\"90\\",\\"ruleApplied\\":\\"Perennials & Roses Royalty Structure\\",\\"explanation\\":\\"0.75% of gross sales\\",\\"tierRate\\":0.75,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"eff99248-2279-4886-941f-d38c94ba824a\\",\\"productName\\":\\"Cascade Blue Hydrangea\\",\\"category\\":\\"Flowering Shrubs\\",\\"territory\\":\\"Secondary\\",\\"quantity\\":1200,\\"saleAmount\\":18000,\\"royaltyAmount\\":\\"405\\",\\"ruleApplied\\":\\"Flowering Shrubs Royalty Structure\\",\\"explanation\\":\\"2.25% of gross sales\\",\\"tierRate\\":2.25,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"0b659b6a-2710-4e67-83d8-0be2e3d526e0\\",\\"productName\\":\\"Golden Spire Juniper\\",\\"category\\":\\"Ornamental Shrubs\\",\\"territory\\":\\"Primary\\",\\"quantity\\":800,\\"saleAmount\\":22000,\\"royaltyAmount\\":\\"275\\",\\"ruleApplied\\":\\"Ornamental Trees & Shrubs Royalty Structure\\",\\"explanation\\":\\"1.25% of gross sales\\",\\"tierRate\\":1.25,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"aedaef70-ae36-4d82-b34e-f93387e57973\\",\\"productName\\":\\"Aurora Flame Maple\\",\\"category\\":\\"Ornamental Trees\\",\\"territory\\":\\"Secondary\\",\\"quantity\\":3000,\\"saleAmount\\":15000,\\"royaltyAmount\\":\\"187.5\\",\\"ruleApplied\\":\\"Ornamental Trees & Shrubs Royalty Structure\\",\\"explanation\\":\\"1.25% of gross sales\\",\\"tierRate\\":1.25,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"b9b0d3c0-b469-4d74-b625-89c20a0e7548\\",\\"productName\\":\\"Pacific Sunset Rose\\",\\"category\\":\\"Flowering Shrubs\\",\\"territory\\":\\"Primary\\",\\"quantity\\":3000,\\"saleAmount\\":12000,\\"royaltyAmount\\":\\"90\\",\\"ruleApplied\\":\\"Perennials & Roses Royalty Structure\\",\\"explanation\\":\\"0.75% of gross sales\\",\\"tierRate\\":0.75,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"2be37310-e76c-41bd-85fd-83b05753c602\\",\\"productName\\":\\"Cascade Blue Hydrangea\\",\\"category\\":\\"Flowering Shrubs\\",\\"territory\\":\\"Primary\\",\\"quantity\\":20000,\\"saleAmount\\":120000,\\"royaltyAmount\\":\\"1739.9999999999998\\",\\"ruleApplied\\":\\"Flowering Shrubs Royalty Structure\\",\\"explanation\\":\\"1.45% of gross sales\\",\\"tierRate\\":1.45,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"e6c1426e-e596-430f-8c04-26985ecb77aa\\",\\"productName\\":\\"Aurora Flame Maple\\",\\"category\\":\\"Ornamental Trees\\",\\"territory\\":\\"Primary\\",\\"quantity\\":1100,\\"saleAmount\\":25000,\\"royaltyAmount\\":\\"312.5\\",\\"ruleApplied\\":\\"Ornamental Trees & Shrubs Royalty Structure\\",\\"explanation\\":\\"1.25% of gross sales\\",\\"tierRate\\":1.25,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1},{\\"saleId\\":\\"1bf4c94a-5b98-4b99-9dd3-2eb3bd9046c4\\",\\"productName\\":\\"Aurora Flame Maple\\",\\"category\\":\\"Ornamental Trees\\",\\"territory\\":\\"Primary\\",\\"quantity\\":6200,\\"saleAmount\\":30000,\\"royaltyAmount\\":\\"330.00000000000006\\",\\"ruleApplied\\":\\"Ornamental Trees & Shrubs Royalty Structure\\",\\"explanation\\":\\"1.1% of gross sales\\",\\"tierRate\\":1.1,\\"seasonalMultiplier\\":1,\\"territoryMultiplier\\":1}]"	"{\\"minimumGuarantee\\":85000,\\"calculatedRoyalty\\":4760,\\"rulesApplied\\":[\\"Perennials & Roses Royalty Structure\\",\\"Ornamental Trees & Shrubs Royalty Structure\\",\\"Flowering Shrubs Royalty Structure\\"]}"	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	\N	\N	\N	\N	\N	\N	2025-11-12 15:37:19.853852	2025-11-12 15:37:19.853852
\.


--
-- Data for Name: contract_versions; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contract_versions (id, contract_id, version_number, editor_id, change_summary, metadata_snapshot, file_reference, approval_state, created_at) FROM stdin;
\.


--
-- Data for Name: contracts; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.contracts (id, file_name, original_name, file_size, file_type, file_path, contract_type, priority, status, uploaded_by, notes, processing_started_at, processing_completed_at, created_at, updated_at, contract_number, display_name, effective_start, effective_end, renewal_terms, governing_law, counterparty_name, contract_owner_id, approval_state, current_version, organization_name, use_erp_matching) FROM stdin;
46d0e351-4296-45bb-a680-9ee65497967b	1e5ef2bc-2f72-4d7e-90f0-c7a115cf3b8a.pdf	Plant Variety License & Royalty Agreement.pdf	443464	application/pdf	/home/runner/workspace/uploads/1e5ef2bc-2f72-4d7e-90f0-c7a115cf3b8a.pdf	licensing	normal	analyzed	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	\N	\N	2025-11-12 15:36:38.809	2025-11-12 15:36:32.035334	2025-11-12 15:36:38.809	CNT-2025-003	CNT-2025-003 Plant Variety License & Royalty Agreement.pdf	2024-02-12 00:00:00	2032-02-12 00:00:00	\N	\N	Green Innovation Genetics LLC	\N	draft	1	\N	f
8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	f9cdb4dc-2023-4927-892e-24dd3c041d68.pdf	Technology License & Royalty Agreement - Manufacturing.pdf	421738	application/pdf	/home/runner/workspace/uploads/f9cdb4dc-2023-4927-892e-24dd3c041d68.pdf	licensing	normal	analyzed	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	\N	\N	2025-11-12 15:32:47.065	2025-11-12 15:32:41.324099	2025-11-12 15:32:47.065	CNT-2025-001	CNT-2025-001 Technology License & Royalty Agreement - Manufacturing.pdf	2024-01-08 00:00:00	2034-01-08 00:00:00	\N	\N	Advanced Materials Technology Corp.	\N	draft	1	\N	f
8f321b79-1ab4-4b02-8806-26db3a21a8ff	fdc5d187-4989-4d1e-a9c1-64e773f231b5.pdf	Electronics Patent License & Component Royalty Agreement.pdf	456378	application/pdf	/home/runner/workspace/uploads/fdc5d187-4989-4d1e-a9c1-64e773f231b5.pdf	licensing	normal	analyzed	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	\N	\N	2025-11-12 15:34:04.6	2025-11-12 15:33:53.043097	2025-11-12 15:34:04.6	CNT-2025-002	CNT-2025-002 Electronics Patent License & Component Royalty Agreement.pdf	\N	\N	\N	\N	Not specified	\N	draft	1	\N	f
\.


--
-- Data for Name: data_import_jobs; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.data_import_jobs (id, mapping_id, customer_id, job_name, upload_meta, status, records_total, records_processed, records_failed, error_log, created_by, started_at, completed_at, created_at) FROM stdin;
\.


--
-- Data for Name: demo_requests; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.demo_requests (id, email, plan_tier, source, status, notes, created_at, updated_at) FROM stdin;
6267180d-2c6d-408a-bfcd-e9bb5455642c	narasimha.kadimi@cimpleit.com	licenseiq	pricing_section	new	\N	2025-10-27 16:19:49.376618	2025-10-27 16:19:49.376618
\.


--
-- Data for Name: early_access_signups; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.early_access_signups (id, email, name, company, source, status, notes, created_at, updated_at) FROM stdin;
8c6144a0-d3cd-4555-9312-5724232f3aef	narasimha.kadimi@cimplet.com	Narasimha Kadimi	sdfdsf	landing_page	new	\N	2025-10-27 16:18:29.33402	2025-10-27 16:18:29.33402
b7030efe-0c9a-4ace-ae94-3baaaa24db8c	test@example.com	John Doe	Test Company	landing_page	new	\N	2025-11-11 15:14:32.231259	2025-11-11 15:14:32.231259
68dec378-03d8-4e7f-a43f-c1b62b6de743	verification@test.com	Test User 2	Verification Corp	landing_page	new	\N	2025-11-11 15:16:02.089959	2025-11-11 15:16:02.089959
7952ede6-c2d7-4c4b-802a-e1e3b99c668b	customer@example.com	Sarah Johnson	TechCorp Inc	landing_page	new	\N	2025-11-11 15:22:03.358833	2025-11-11 15:22:03.358833
68e4cab9-408c-48f9-90b3-1494c2388848	kmlnrao2008@gmail.com	Narasimha Rao Kadimi	Cimpleit	landing_page	new	\N	2025-11-11 15:24:27.011795	2025-11-11 15:24:27.011795
5e871e43-2150-44c6-bbe5-8f91d9f90ce6	test-zoho@example.com	Test User	Test Corp	landing_page	new	\N	2025-11-11 15:44:56.988328	2025-11-11 15:44:56.988328
fd052500-e8b6-48c2-b245-320175b117bc	kmlnrao2008@gmail.com	Narasimha Rao Kadimi	Cimpleit	landing_page	new	\N	2025-11-11 15:45:37.996965	2025-11-11 15:45:37.996965
cc6778dd-83b0-416a-b028-eac7ed6400d7	narasimha.kadimi@cimplet.com	Maha Laxmi Nara Kadimi	Cimpleit	landing_page	new	\N	2025-11-11 15:48:06.949433	2025-11-11 15:48:06.949433
94abea14-f043-491e-860f-aa84aec91a7d	kmlnrao2008@gmail.com	Maha Laxmi Nara Kadimi	Cimpleit	landing_page	new	\N	2025-11-11 15:50:15.553264	2025-11-11 15:50:15.553264
4854aee9-8c59-4496-a16f-037da2c7b13b	narasimha.kadimi@cimpleit.com	Maha Laxmi Nara Kadimi	Cimpleit	landing_page	new	\N	2025-11-11 15:51:15.202749	2025-11-11 15:51:15.202749
391b9dee-e51d-438a-a18a-b2164be3be78	chaits@cimpleit.com	Chaitnya	Cimpleit	landing_page	new	\N	2025-11-11 18:15:34.95742	2025-11-11 18:15:34.95742
3bcc0e02-d80c-4352-b1cc-557531739c41	neelesh.gamini@cimpleit.com	Neelesh	Cimpleit	landing_page	new	\N	2025-11-11 23:12:57.789074	2025-11-11 23:12:57.789074
7d68d78a-9e2b-4f6b-89de-3cccd377538a	amulya.yadamreddy@cimpleit.com	Amulya	Amulya	landing_page	new	\N	2025-11-11 23:13:45.617973	2025-11-11 23:13:45.617973
\.


--
-- Data for Name: erp_entities; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.erp_entities (id, system_id, name, technical_name, entity_type, description, sample_data, status, created_by, created_at, updated_at) FROM stdin;
15f06edb-779e-4768-aaf1-00ac996a8f6b	010df980-10ab-4577-a980-3ab4afc9669f	Customers	HZ_PARTIES	master_data	Customer master data from Trading Community Architecture	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
d7f21bac-c06f-4499-805c-8b319ed9fee8	010df980-10ab-4577-a980-3ab4afc9669f	Customer Accounts	HZ_CUST_ACCOUNTS	master_data	Customer account information	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
91296cc1-c2e9-438d-9fed-9b00aee17db7	010df980-10ab-4577-a980-3ab4afc9669f	Items	MTL_SYSTEM_ITEMS_B	master_data	Inventory items master data	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
dd63ac70-efcc-47f1-8b7b-1570b035652e	010df980-10ab-4577-a980-3ab4afc9669f	Suppliers	PO_VENDORS	master_data	Supplier/vendor master data	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
c5bf21ec-6afe-4966-8bce-3bfea771eaa4	010df980-10ab-4577-a980-3ab4afc9669f	Payment Terms	AP_TERMS	master_data	Payment terms definitions	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
9abad548-038b-4624-ba05-a8a3b282aa11	010df980-10ab-4577-a980-3ab4afc9669f	Employees	PER_ALL_PEOPLE_F	master_data	Employee and person records	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
6add9b09-b9b5-4908-bc32-a8937043492a	010df980-10ab-4577-a980-3ab4afc9669f	GL Accounts	GL_CODE_COMBINATIONS	master_data	Chart of accounts combinations	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
a968e83d-a2d4-4fad-9fe7-e4784bd366be	010df980-10ab-4577-a980-3ab4afc9669f	Locations	HR_LOCATIONS_ALL	master_data	Physical locations	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
3a9f436b-b7e3-41a2-bc23-e3645e32592e	010df980-10ab-4577-a980-3ab4afc9669f	Units of Measure	MTL_UNITS_OF_MEASURE	master_data	Unit of measure conversions	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
0c5d67e3-9214-42d6-b905-fe89523ef359	010df980-10ab-4577-a980-3ab4afc9669f	Currencies	FND_CURRENCIES	master_data	Currency definitions	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
7a1b1c08-f437-452b-b2e0-447b3069f1fd	010df980-10ab-4577-a980-3ab4afc9669f	Sales Orders	OE_ORDER_HEADERS_ALL	transactional	Sales order headers	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
b48c0676-daa1-4464-83a1-f6299c0f2d01	010df980-10ab-4577-a980-3ab4afc9669f	Invoices	RA_CUSTOMER_TRX_ALL	transactional	AR invoices	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	010df980-10ab-4577-a980-3ab4afc9669f	Purchase Orders	PO_HEADERS_ALL	transactional	Purchase order headers	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
05adbbb6-c482-4714-b1fc-2515e6fa8904	010df980-10ab-4577-a980-3ab4afc9669f	Journal Entries	GL_JE_HEADERS	transactional	General ledger journal entries	\N	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:12:09.655664	2025-11-04 21:12:09.655664
\.


--
-- Data for Name: erp_fields; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.erp_fields (id, entity_id, field_name, data_type, constraints, sample_values, description, is_primary_key, is_required, created_at, updated_at) FROM stdin;
a502d8e9-8704-4234-9798-c6f3ab799ff2	15f06edb-779e-4768-aaf1-00ac996a8f6b	PARTY_ID	NUMBER	\N	12345, 67890	Unique party identifier	t	t	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
65734f22-3b8e-4f23-be3b-3ae4fdfa4eb6	15f06edb-779e-4768-aaf1-00ac996a8f6b	PARTY_NUMBER	VARCHAR2	\N	CUST-001, CUST-002	Party number	f	t	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
679973df-5e55-48f5-8f9f-2efff9727ae6	15f06edb-779e-4768-aaf1-00ac996a8f6b	PARTY_NAME	VARCHAR2	\N	Acme Corp, Global Industries	Customer name	f	t	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
2f6b7f5b-0b86-4e68-90da-ca2f0ffb105a	15f06edb-779e-4768-aaf1-00ac996a8f6b	PARTY_TYPE	VARCHAR2	\N	ORGANIZATION, PERSON	Party type code	f	t	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
c000d97a-42b9-40d2-9fa9-0e77d9d26c39	15f06edb-779e-4768-aaf1-00ac996a8f6b	EMAIL_ADDRESS	VARCHAR2	\N	contact@acme.com, info@global.com	Primary email address	f	f	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
07eaab3c-caf1-42ea-ac88-5795609d590b	15f06edb-779e-4768-aaf1-00ac996a8f6b	PHONE_NUMBER	VARCHAR2	\N	+1-555-0100, +1-555-0200	Primary phone number	f	f	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
29b63a32-4e4e-496e-a53e-ed66d42b2536	15f06edb-779e-4768-aaf1-00ac996a8f6b	TAX_REFERENCE	VARCHAR2	\N	12-3456789, 98-7654321	Tax identification number	f	f	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
94c57756-4838-4bea-bf6d-c0a1b543a335	15f06edb-779e-4768-aaf1-00ac996a8f6b	CREDIT_RATING	VARCHAR2	\N	EXCELLENT, GOOD, FAIR	Customer credit rating	f	f	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
b798c695-c9ee-40b3-ab87-a8ad39774a6f	15f06edb-779e-4768-aaf1-00ac996a8f6b	CUSTOMER_CLASS_CODE	VARCHAR2	\N	WHOLESALE, RETAIL	Customer classification	f	f	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
340002cb-c8f1-4170-8a9f-0a227f9a8aa0	15f06edb-779e-4768-aaf1-00ac996a8f6b	STATUS	VARCHAR2	\N	A, I	Active status	f	t	2025-11-04 21:12:27.342012	2025-11-04 21:12:27.342012
b13645c9-b34a-4381-abd3-2b5281de7d41	dd63ac70-efcc-47f1-8b7b-1570b035652e	VENDOR_ID	NUMBER	\N	\N	Unique supplier/vendor identifier	t	t	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
bd3e746d-d40d-419a-b2fe-598e441d7c27	dd63ac70-efcc-47f1-8b7b-1570b035652e	VENDOR_NAME	VARCHAR2(240)	\N	\N	Supplier/vendor name	f	t	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
8972cf7e-17dd-4725-9355-c99d19f3f37e	dd63ac70-efcc-47f1-8b7b-1570b035652e	VENDOR_NUMBER	VARCHAR2(30)	\N	\N	Unique supplier/vendor number	f	t	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
27ddeb51-58dc-4e0c-aba3-5710be2a6c69	dd63ac70-efcc-47f1-8b7b-1570b035652e	VENDOR_TYPE	VARCHAR2(25)	\N	\N	Type of supplier (Standard, One-Time, etc.)	f	f	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
785f570a-4151-4e22-8ff2-ff8e46668cca	dd63ac70-efcc-47f1-8b7b-1570b035652e	TAX_ID	VARCHAR2(30)	\N	\N	Tax identification number	f	f	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
4cc69a09-763d-4772-a4b4-02f4d92063e6	dd63ac70-efcc-47f1-8b7b-1570b035652e	PAYMENT_TERMS_ID	NUMBER	\N	\N	Default payment terms	f	f	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
92612060-33b3-4e09-b6c4-6192f0513e34	dd63ac70-efcc-47f1-8b7b-1570b035652e	CURRENCY_CODE	VARCHAR2(15)	\N	\N	Default currency code	f	f	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
2b605dff-6116-41b1-a035-2d5b015b1c74	dd63ac70-efcc-47f1-8b7b-1570b035652e	PAYMENT_METHOD	VARCHAR2(25)	\N	\N	Preferred payment method	f	f	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
64371a6f-e839-4718-8b97-cc6f44818d28	dd63ac70-efcc-47f1-8b7b-1570b035652e	VENDOR_SITE_CODE	VARCHAR2(15)	\N	\N	Supplier site code	f	f	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
fe399d5c-0374-4070-b893-590dbe4901f1	dd63ac70-efcc-47f1-8b7b-1570b035652e	STATUS	VARCHAR2(25)	\N	\N	Supplier status (Active, Inactive, Hold)	f	t	2025-11-05 20:39:15.617274	2025-11-05 20:39:15.617274
eab02081-8ec6-4c3f-9249-f7759123b8d7	91296cc1-c2e9-438d-9fed-9b00aee17db7	INVENTORY_ITEM_ID	NUMBER	\N	\N	Unique item identifier	t	t	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
035c14d9-4ec1-4fa6-bd3a-266c29438011	91296cc1-c2e9-438d-9fed-9b00aee17db7	ITEM_NUMBER	VARCHAR2(40)	\N	\N	Item number/code	f	t	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
5f4c72f6-ceee-4438-b337-82409303975e	91296cc1-c2e9-438d-9fed-9b00aee17db7	DESCRIPTION	VARCHAR2(240)	\N	\N	Item description	f	t	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
cc914da3-8776-41ca-8e81-d3853c68983d	91296cc1-c2e9-438d-9fed-9b00aee17db7	PRIMARY_UOM_CODE	VARCHAR2(3)	\N	\N	Primary unit of measure	f	t	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
29660753-63cc-46c1-9ed0-027de84b2b4f	91296cc1-c2e9-438d-9fed-9b00aee17db7	ITEM_TYPE	VARCHAR2(30)	\N	\N	Type of item (Purchased, Manufactured, etc.)	f	f	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
93883272-dc7c-40f6-8e78-3c280af9772d	91296cc1-c2e9-438d-9fed-9b00aee17db7	CATEGORY_SET_NAME	VARCHAR2(30)	\N	\N	Item category set	f	f	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
026753aa-3f4a-473a-95a0-4db0139b504e	91296cc1-c2e9-438d-9fed-9b00aee17db7	LIST_PRICE	NUMBER	\N	\N	Standard list price	f	f	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
802facf2-785c-4358-9a1f-626b820afa58	91296cc1-c2e9-438d-9fed-9b00aee17db7	UNIT_WEIGHT	NUMBER	\N	\N	Weight per unit	f	f	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
ccc1d966-7d33-4cef-bb4a-2788767720ed	91296cc1-c2e9-438d-9fed-9b00aee17db7	INVENTORY_ASSET_FLAG	VARCHAR2(1)	\N	\N	Asset tracked (Y/N)	f	f	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
8a068dfe-2dd2-402c-94f5-dbe22e837160	91296cc1-c2e9-438d-9fed-9b00aee17db7	STATUS	VARCHAR2(15)	\N	\N	Item status (Active, Inactive, Obsolete)	f	t	2025-11-05 20:40:02.361177	2025-11-05 20:40:02.361177
6dc679ba-9e94-40a7-9588-18d76efd6ca7	b48c0676-daa1-4464-83a1-f6299c0f2d01	CUSTOMER_TRX_ID	NUMBER	\N	\N	Unique invoice transaction identifier	t	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
8bbe644f-9e5f-4049-877f-89908e723603	b48c0676-daa1-4464-83a1-f6299c0f2d01	TRX_NUMBER	VARCHAR2(20)	\N	\N	Invoice number	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
a435876a-55e2-4af8-9243-4ca3862a803c	b48c0676-daa1-4464-83a1-f6299c0f2d01	TRX_DATE	DATE	\N	\N	Transaction date	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
3e4bd79e-d5d3-458c-9954-6bc30f0a57ba	b48c0676-daa1-4464-83a1-f6299c0f2d01	BILL_TO_CUSTOMER_ID	NUMBER	\N	\N	Bill-to customer identifier	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
699a934b-7dc7-4d11-a147-059cf7110614	b48c0676-daa1-4464-83a1-f6299c0f2d01	INVOICE_CURRENCY_CODE	VARCHAR2(15)	\N	\N	Invoice currency code	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
3e792333-6a59-4478-8291-829fde384c61	b48c0676-daa1-4464-83a1-f6299c0f2d01	EXCHANGE_RATE	NUMBER	\N	\N	Currency exchange rate	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
03838349-f11f-41a1-a118-63c717faa383	b48c0676-daa1-4464-83a1-f6299c0f2d01	TOTAL_AMOUNT	NUMBER	\N	\N	Total invoice amount	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
68d3354d-8726-4b10-ac7d-b23af440fdb7	b48c0676-daa1-4464-83a1-f6299c0f2d01	STATUS_TRX	VARCHAR2(30)	\N	\N	Transaction status	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
b36e47ab-280e-44ba-92ef-7fc90e5803d8	7a1b1c08-f437-452b-b2e0-447b3069f1fd	HEADER_ID	NUMBER	\N	\N	Unique sales order header identifier	t	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
2d7f358d-f81b-4aa5-939e-bee8996014fd	7a1b1c08-f437-452b-b2e0-447b3069f1fd	ORDER_NUMBER	NUMBER	\N	\N	Sales order number	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
baebf443-f0eb-44ba-8b17-198273633d78	7a1b1c08-f437-452b-b2e0-447b3069f1fd	ORDERED_DATE	DATE	\N	\N	Order date	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
b4b683ac-8e24-4e3b-97e7-4b751186c88a	7a1b1c08-f437-452b-b2e0-447b3069f1fd	SOLD_TO_ORG_ID	NUMBER	\N	\N	Sold-to organization ID	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
9804c35a-90a1-4d22-b58f-ad68b429e8b2	7a1b1c08-f437-452b-b2e0-447b3069f1fd	SHIP_TO_ORG_ID	NUMBER	\N	\N	Ship-to organization ID	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
ec93257a-2a93-411c-8bcf-5aa29ff72ed7	7a1b1c08-f437-452b-b2e0-447b3069f1fd	ORDER_CATEGORY_CODE	VARCHAR2(30)	\N	\N	Order category	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
3c15b943-64bd-4abb-b7a6-3b4ada422d90	7a1b1c08-f437-452b-b2e0-447b3069f1fd	TRANSACTIONAL_CURR_CODE	VARCHAR2(15)	\N	\N	Transaction currency	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
f8d6538e-1627-47ae-a38e-c800985704c9	7a1b1c08-f437-452b-b2e0-447b3069f1fd	FLOW_STATUS_CODE	VARCHAR2(30)	\N	\N	Order flow status	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
b008caba-c044-4663-a253-2af7515d6b17	4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	PO_HEADER_ID	NUMBER	\N	\N	Unique purchase order header identifier	t	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
8c65d644-a017-4e75-b0f7-b3d75e51f4f2	4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	SEGMENT1	VARCHAR2(20)	\N	\N	Purchase order number	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
dd0d8b68-d887-4f19-9309-fbbe8d17dc7e	4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	TYPE_LOOKUP_CODE	VARCHAR2(25)	\N	\N	PO type (Standard, Blanket, etc.)	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
74e5d81f-da64-4476-8959-594a810ea63a	4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	VENDOR_ID	NUMBER	\N	\N	Supplier/vendor identifier	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
6889563c-aee9-423e-b140-e848b6105ece	4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	VENDOR_SITE_ID	NUMBER	\N	\N	Supplier site identifier	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
1690face-26c3-451e-8634-91e0bfaf2714	4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	CURRENCY_CODE	VARCHAR2(15)	\N	\N	PO currency code	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
66d16be8-7a56-457a-9b27-7d20c91cec5d	4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	RATE	NUMBER	\N	\N	Exchange rate	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
4ff353f5-da99-4872-8105-b0b44e7501ce	4e3c7a47-29fc-4dc5-b07c-9e0b1c5a1d20	AUTHORIZATION_STATUS	VARCHAR2(25)	\N	\N	Approval status	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
64d0507d-2814-459f-a76b-f8c6b36f70ab	6add9b09-b9b5-4908-bc32-a8937043492a	CODE_COMBINATION_ID	NUMBER	\N	\N	Unique GL account combination ID	t	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
e3cb8608-d1b1-4ac0-8dc5-7a82ca43646e	6add9b09-b9b5-4908-bc32-a8937043492a	SEGMENT1	VARCHAR2(25)	\N	\N	Company segment	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
9b488a05-375a-41c3-8f80-95a2a48601a2	6add9b09-b9b5-4908-bc32-a8937043492a	SEGMENT2	VARCHAR2(25)	\N	\N	Department segment	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
8e92ddca-7890-450b-91e9-a74d6455f044	6add9b09-b9b5-4908-bc32-a8937043492a	SEGMENT3	VARCHAR2(25)	\N	\N	Account segment	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
97192afb-a712-46dd-a184-91527da28e08	6add9b09-b9b5-4908-bc32-a8937043492a	SEGMENT4	VARCHAR2(25)	\N	\N	Sub-account segment	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
8edcc1a6-dd4d-4d4e-9399-77140ea79f67	6add9b09-b9b5-4908-bc32-a8937043492a	SEGMENT5	VARCHAR2(25)	\N	\N	Product segment	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
92133282-93e5-487a-95b3-59cb6d9f5eb1	6add9b09-b9b5-4908-bc32-a8937043492a	CONCATENATED_SEGMENTS	VARCHAR2(240)	\N	\N	Full account string	f	f	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
ef753cc6-4ff3-4e7d-9409-c071b3a2a966	6add9b09-b9b5-4908-bc32-a8937043492a	ENABLED_FLAG	VARCHAR2(1)	\N	\N	Account enabled (Y/N)	f	t	2025-11-05 20:47:40.475826	2025-11-05 20:47:40.475826
38065559-87b2-4ee4-9cf0-33622ceb62d0	d7f21bac-c06f-4499-805c-8b319ed9fee8	CUST_ACCOUNT_ID	NUMBER	\N	\N	Unique customer account identifier	t	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
b33f92a6-6527-4521-ab98-84733a68cd95	d7f21bac-c06f-4499-805c-8b319ed9fee8	ACCOUNT_NUMBER	VARCHAR2(30)	\N	\N	Customer account number	f	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
e2d59969-5ddd-4741-a915-10e80e77c09c	d7f21bac-c06f-4499-805c-8b319ed9fee8	PARTY_ID	NUMBER	\N	\N	Party identifier	f	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
79e5e550-8b83-486e-a322-8919f2f830f7	d7f21bac-c06f-4499-805c-8b319ed9fee8	ACCOUNT_NAME	VARCHAR2(240)	\N	\N	Account name	f	f	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
59daaaea-df26-4bee-ae1c-1ee66d149f82	d7f21bac-c06f-4499-805c-8b319ed9fee8	CUSTOMER_TYPE	VARCHAR2(30)	\N	\N	Customer type	f	f	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
f2e52620-4d58-4ca7-a561-35ba37a87947	d7f21bac-c06f-4499-805c-8b319ed9fee8	CUSTOMER_CLASS_CODE	VARCHAR2(30)	\N	\N	Customer classification	f	f	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
1402b25d-d0d9-47d8-9e86-18c5e012821a	d7f21bac-c06f-4499-805c-8b319ed9fee8	STATUS	VARCHAR2(1)	\N	\N	Account status (A/I)	f	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
9cb6fcb4-a631-4411-8b03-8ce2ebd0dc45	9abad548-038b-4624-ba05-a8a3b282aa11	PERSON_ID	NUMBER	\N	\N	Unique person identifier	t	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
4974928c-cea1-4f98-8846-07d173052ba6	9abad548-038b-4624-ba05-a8a3b282aa11	EMPLOYEE_NUMBER	VARCHAR2(30)	\N	\N	Employee number	f	f	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
14ef6a3d-6530-4534-a9bf-4d5c345ae1c1	9abad548-038b-4624-ba05-a8a3b282aa11	FULL_NAME	VARCHAR2(240)	\N	\N	Full name	f	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
908b5dde-fa99-40c8-bf75-ef745f7fb8b2	9abad548-038b-4624-ba05-a8a3b282aa11	FIRST_NAME	VARCHAR2(150)	\N	\N	First name	f	f	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
3c087eca-0864-49a2-bd64-4252643da561	9abad548-038b-4624-ba05-a8a3b282aa11	LAST_NAME	VARCHAR2(150)	\N	\N	Last name	f	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
b32666c1-e3b2-4ecf-95a9-091cdfe56c54	9abad548-038b-4624-ba05-a8a3b282aa11	EMAIL_ADDRESS	VARCHAR2(240)	\N	\N	Email address	f	f	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
66d4be74-ccf0-4873-844f-24b6f3e0852b	9abad548-038b-4624-ba05-a8a3b282aa11	EFFECTIVE_START_DATE	DATE	\N	\N	Effective start date	f	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
2772e233-7c40-4a39-acaa-fcb1a2430493	9abad548-038b-4624-ba05-a8a3b282aa11	EFFECTIVE_END_DATE	DATE	\N	\N	Effective end date	f	t	2025-11-05 20:47:49.80572	2025-11-05 20:47:49.80572
0060b34e-9a2c-405f-8fc7-1697742b8e31	a968e83d-a2d4-4fad-9fe7-e4784bd366be	LOCATION_ID	NUMBER	\N	\N	Unique location identifier	t	t	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
35da2193-593e-4696-8184-0f9e4459c333	a968e83d-a2d4-4fad-9fe7-e4784bd366be	LOCATION_CODE	VARCHAR2(60)	\N	\N	Location code	f	t	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
1bf89502-413b-4714-9649-62a61598bb79	a968e83d-a2d4-4fad-9fe7-e4784bd366be	DESCRIPTION	VARCHAR2(240)	\N	\N	Location description	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
69794049-6c35-4f6d-a6c9-5f96be3e8fb0	a968e83d-a2d4-4fad-9fe7-e4784bd366be	ADDRESS_LINE_1	VARCHAR2(240)	\N	\N	Address line 1	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
75ea42f4-0c2b-4311-995f-29a9b5aa3f5a	a968e83d-a2d4-4fad-9fe7-e4784bd366be	CITY	VARCHAR2(60)	\N	\N	City	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
b2882dd2-e784-40c9-9aaa-a64c29b00299	a968e83d-a2d4-4fad-9fe7-e4784bd366be	POSTAL_CODE	VARCHAR2(30)	\N	\N	Postal/ZIP code	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
e0c84ccc-4d64-4413-8014-bafb7ccee13e	a968e83d-a2d4-4fad-9fe7-e4784bd366be	COUNTRY	VARCHAR2(60)	\N	\N	Country	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
1abf884c-1de3-48fe-a536-de2ea315e1b3	a968e83d-a2d4-4fad-9fe7-e4784bd366be	INACTIVE_DATE	DATE	\N	\N	Inactive date	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
3156d270-5907-4ea8-9bc3-0e28a3106324	0c5d67e3-9214-42d6-b905-fe89523ef359	CURRENCY_CODE	VARCHAR2(15)	\N	\N	Currency code (USD, EUR, etc.)	t	t	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
a1e07211-30e0-475f-8ffa-6209ac31e402	0c5d67e3-9214-42d6-b905-fe89523ef359	NAME	VARCHAR2(240)	\N	\N	Currency name	f	t	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
503d6b37-d2bf-4dba-a98a-c90cade66ff6	0c5d67e3-9214-42d6-b905-fe89523ef359	DESCRIPTION	VARCHAR2(240)	\N	\N	Currency description	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
6a2dc3e4-1f4d-4fda-8415-3b77886032dd	0c5d67e3-9214-42d6-b905-fe89523ef359	PRECISION	NUMBER	\N	\N	Decimal precision	f	t	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
d77066ac-4e9b-47a0-90cf-bd29c7155e5e	0c5d67e3-9214-42d6-b905-fe89523ef359	MINIMUM_ACCOUNTABLE_UNIT	NUMBER	\N	\N	Minimum accountable unit	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
182d0c92-bd48-4c13-8ce9-928a10a79c04	0c5d67e3-9214-42d6-b905-fe89523ef359	ENABLED_FLAG	VARCHAR2(1)	\N	\N	Enabled (Y/N)	f	t	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
02de2e43-c3da-4c9e-8aa7-f2a77cb907be	0c5d67e3-9214-42d6-b905-fe89523ef359	START_DATE_ACTIVE	DATE	\N	\N	Start date active	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
0c879f0e-c57f-4571-9e44-2db048cde8b5	0c5d67e3-9214-42d6-b905-fe89523ef359	END_DATE_ACTIVE	DATE	\N	\N	End date active	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
aef9b900-00ff-42a3-ad92-33609aff6308	3a9f436b-b7e3-41a2-bc23-e3645e32592e	UOM_CODE	VARCHAR2(3)	\N	\N	Unit of measure code	t	t	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
16ab86dd-09dd-4384-8445-7ea4fed0987a	3a9f436b-b7e3-41a2-bc23-e3645e32592e	UNIT_OF_MEASURE	VARCHAR2(25)	\N	\N	Unit of measure name	f	t	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
4a5f8188-0f21-46da-a24d-5dffe71be721	3a9f436b-b7e3-41a2-bc23-e3645e32592e	UOM_CLASS	VARCHAR2(10)	\N	\N	UOM class (Weight, Volume, etc.)	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
111836ae-3587-4488-9296-deab3b0898c4	3a9f436b-b7e3-41a2-bc23-e3645e32592e	BASE_UOM_FLAG	VARCHAR2(1)	\N	\N	Is base UOM (Y/N)	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
960e7b16-5a8d-476d-8aaa-b142eaa71084	3a9f436b-b7e3-41a2-bc23-e3645e32592e	DISABLE_DATE	DATE	\N	\N	Disable date	f	f	2025-11-05 20:50:01.160188	2025-11-05 20:50:01.160188
70f2e906-e5cb-4e7b-a453-5a24706ecfbf	c5bf21ec-6afe-4966-8bce-3bfea771eaa4	TERM_ID	NUMBER	\N	\N	Unique payment term identifier	t	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
827a3285-6e41-486e-81e8-ca6bb02d1a79	c5bf21ec-6afe-4966-8bce-3bfea771eaa4	NAME	VARCHAR2(50)	\N	\N	Payment term name	f	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
75913a15-4ed9-42c9-be3a-03d40534b087	c5bf21ec-6afe-4966-8bce-3bfea771eaa4	DESCRIPTION	VARCHAR2(240)	\N	\N	Payment term description	f	f	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
fff8eebd-2981-4d72-b4d0-c0205430f32e	c5bf21ec-6afe-4966-8bce-3bfea771eaa4	DUE_DAYS	NUMBER	\N	\N	Payment due in days	f	f	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
3892b58c-d920-421c-8e2e-cc7ac6eae0e7	c5bf21ec-6afe-4966-8bce-3bfea771eaa4	DISCOUNT_DAYS	NUMBER	\N	\N	Discount available days	f	f	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
38a42888-8a06-47ce-9103-cd7a944a0dd6	c5bf21ec-6afe-4966-8bce-3bfea771eaa4	DISCOUNT_PERCENT	NUMBER	\N	\N	Discount percentage	f	f	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
c4aaad54-19e5-4a89-af02-9f3cbc5c97b4	c5bf21ec-6afe-4966-8bce-3bfea771eaa4	ENABLED_FLAG	VARCHAR2(1)	\N	\N	Enabled (Y/N)	f	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
5c3e18eb-95a7-48fe-ac1e-1527a8a61846	05adbbb6-c482-4714-b1fc-2515e6fa8904	JE_HEADER_ID	NUMBER	\N	\N	Unique journal entry header identifier	t	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
957f19c0-3a70-42c0-9ec4-fe61fc30d228	05adbbb6-c482-4714-b1fc-2515e6fa8904	JE_BATCH_ID	NUMBER	\N	\N	Journal entry batch identifier	f	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
e74838b1-d999-4918-8c8c-6f9934ace9c0	05adbbb6-c482-4714-b1fc-2515e6fa8904	NAME	VARCHAR2(100)	\N	\N	Journal entry name	f	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
9aa9b809-5328-4c9c-a46c-d707d51fff27	05adbbb6-c482-4714-b1fc-2515e6fa8904	DESCRIPTION	VARCHAR2(240)	\N	\N	Journal entry description	f	f	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
d350b283-42b1-42aa-b209-c6fd0d6cc665	05adbbb6-c482-4714-b1fc-2515e6fa8904	PERIOD_NAME	VARCHAR2(15)	\N	\N	GL period name	f	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
306d4080-3337-4e8f-a30b-bc6a661a6351	05adbbb6-c482-4714-b1fc-2515e6fa8904	POSTED_DATE	DATE	\N	\N	Posted date	f	f	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
2fdc8a0c-5984-4385-9e25-55addff7f540	05adbbb6-c482-4714-b1fc-2515e6fa8904	CURRENCY_CODE	VARCHAR2(15)	\N	\N	Currency code	f	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
344c73d3-ffa5-47b9-a848-3ae2ece9d9dc	05adbbb6-c482-4714-b1fc-2515e6fa8904	STATUS	VARCHAR2(1)	\N	\N	Status (P=Posted, U=Unposted)	f	t	2025-11-05 20:50:11.464887	2025-11-05 20:50:11.464887
\.


--
-- Data for Name: erp_systems; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.erp_systems (id, name, vendor, version, description, category, status, created_by, created_at, updated_at) FROM stdin;
010df980-10ab-4577-a980-3ab4afc9669f	Oracle EBS	Oracle	R12.2	Oracle E-Business Suite - Enterprise Resource Planning system	enterprise	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:11:53.709005	2025-11-04 21:11:53.709005
8870a21c-e12d-467e-ad76-de8f77e5579d	SAP S/4HANA	SAP	2023	SAP S/4HANA - Next-generation ERP suite	enterprise	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:11:53.709005	2025-11-04 21:11:53.709005
b83e2981-c615-47d0-9ec4-d362872fe4e8	NetSuite	Oracle	2024.1	NetSuite Cloud ERP - Unified business management suite	cloud	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:11:53.709005	2025-11-04 21:11:53.709005
75ca185b-b2a3-4a15-a940-02d9e20b78bc	Microsoft Dynamics 365	Microsoft	Finance & Operations	Dynamics 365 ERP platform	enterprise	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:11:53.709005	2025-11-04 21:11:53.709005
bd52443e-578e-474c-b04f-ad65e3d18f24	Workday	Workday	Update 46	Workday Financial Management	cloud	active	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-04 21:11:53.709005	2025-11-04 21:11:53.709005
\.


--
-- Data for Name: extraction_runs; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.extraction_runs (id, contract_id, run_type, status, overall_confidence, nodes_extracted, edges_extracted, rules_extracted, validation_results, ai_model, processing_time, error_log, triggered_by, created_at, completed_at) FROM stdin;
\.


--
-- Data for Name: financial_analysis; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.financial_analysis (id, contract_id, total_value, currency, payment_schedule, royalty_structure, revenue_projections, cost_impact, currency_risk, payment_terms, penalty_clauses, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: human_review_tasks; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.human_review_tasks (id, contract_id, extraction_run_id, task_type, priority, status, target_id, target_type, original_data, suggested_correction, confidence, review_notes, assigned_to, reviewed_by, reviewed_at, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: imported_erp_records; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.imported_erp_records (id, job_id, mapping_id, customer_id, source_record, target_record, embedding, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: licenseiq_entities; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.licenseiq_entities (id, name, technical_name, description, category, created_at, updated_at) FROM stdin;
lic-sales-data	Sales Data	sales_data	Transactional sales records used for royalty calculations and revenue analysis	Transactional	2025-11-06 02:54:12.573101	2025-11-06 02:54:12.573101
lic-contract-terms	Contract Terms	contract_terms	Key terms and conditions extracted from contracts including parties, dates, and obligations	Master Data	2025-11-06 02:54:12.573101	2025-11-06 02:54:12.573101
lic-royalty-rules	Royalty Rules	royalty_rules	Payment calculation rules including rates, tiers, and conditional logic	Rules	2025-11-06 02:54:12.573101	2025-11-06 02:54:12.573101
lic-payments	Payments	payments	Calculated payment records with breakdowns and transaction details	Transactional	2025-11-06 02:54:12.573101	2025-11-06 02:54:12.573101
lic-products	Products	products	Product and item master data for licensing and royalty tracking	Master Data	2025-11-06 02:54:12.573101	2025-11-06 02:54:12.573101
2948b6e4-c3ee-430b-a181-9e2970e81505	Customers/Parties	customers_parties	Customer and party master data	Master Data	2025-11-06 22:07:39.915208	2025-11-06 22:07:39.915208
3a8e3cfb-e3b1-460a-9567-e7ab62be0c00	Items	items	Item master data	Master Data	2025-11-06 22:07:39.945787	2025-11-06 22:07:39.945787
f9ae69a5-7fc7-4b31-8096-0742c2c35991	Item Category	item_category	Item categories	Master Data	2025-11-06 22:07:39.973932	2025-11-06 22:07:39.973932
60522d4a-d393-4d69-b728-8caec273ebeb	Item Class	item_class	Item classifications	Master Data	2025-11-06 22:07:39.999294	2025-11-06 22:07:39.999294
cd1becd4-fc55-4fd8-908a-491661ddfb95	Item Catalog	item_catalog	Item catalog	Master Data	2025-11-06 22:07:40.025889	2025-11-06 22:07:40.025889
e326e98c-70f2-4bc2-a44b-bc5d57a755c0	Item Structures	item_structures	Item structures and hierarchies	Master Data	2025-11-06 22:07:40.052734	2025-11-06 22:07:40.052734
d8a5768f-b33c-469f-8b39-1055e3de0628	Customer Sites	customer_sites	Customer site locations	Master Data	2025-11-06 22:07:40.080235	2025-11-06 22:07:40.080235
93737de6-4714-4af1-86a0-0fbac1517f22	Customer Site Uses	customer_site_uses	Customer site uses	Master Data	2025-11-06 22:07:40.109471	2025-11-06 22:07:40.109471
223227dc-23b4-4ade-98b2-d76e1d3e379a	Suppliers/Vendors	suppliers_vendors	Supplier and vendor master data	Master Data	2025-11-06 22:07:40.136705	2025-11-06 22:07:40.136705
0d99a727-9d93-44a1-91e7-fad14dd5ff28	Supplier Sites	supplier_sites	Supplier site locations	Master Data	2025-11-06 22:07:40.163554	2025-11-06 22:07:40.163554
15d63ead-a068-4609-9a2c-0508571f7dc0	Payment Terms	payment_terms	Payment terms master data	Master Data	2025-11-06 22:07:40.190055	2025-11-06 22:07:40.190055
e309d11d-a60f-43ed-abf0-a7883822efbc	Chart of Accounts	chart_of_accounts	General ledger accounts	Master Data	2025-11-06 22:07:40.272013	2025-11-06 22:07:40.272013
b0b42d29-9027-4300-b9bf-84461003b64d	Sales Reps	sales_reps	Sales representatives	Master Data	2025-11-06 22:07:40.298717	2025-11-06 22:07:40.298717
028b76f4-43e6-4ee1-a7f2-f3e678b35ad2	Employee Master	employee_master	Employee master data	Master Data	2025-11-06 22:07:40.325822	2025-11-06 22:07:40.325822
269ecbac-5b9f-4b93-8699-4c69a3f0431b	Sales Orders	sales_orders	Sales order headers	Transactions	2025-11-06 22:07:40.352059	2025-11-06 22:07:40.352059
c11f3d4c-8767-4333-b287-874cf3ccf117	Sales Order Lines	sales_order_lines	Sales order line items	Transactions	2025-11-06 22:07:40.378675	2025-11-06 22:07:40.378675
b597d8b0-25c0-4026-ba1b-9a25ad148f40	AR Invoices	ar_invoices	Accounts receivable invoice headers	Transactions	2025-11-06 22:07:40.405873	2025-11-06 22:07:40.405873
bc08ed69-0bcc-4785-975b-8bcc0fd19c15	AR Invoice Lines	ar_invoice_lines	Accounts receivable invoice lines	Transactions	2025-11-06 22:07:40.43261	2025-11-06 22:07:40.43261
d049d404-086b-4b89-b62e-ce249e7f8be4	AP Invoices	ap_invoices	Accounts payable invoice headers	Transactions	2025-11-06 22:07:40.460683	2025-11-06 22:07:40.460683
ad5852cf-89df-4943-b381-3bd1d2a2ba56	AP Invoice Lines	ap_invoice_lines	Accounts payable invoice lines	Transactions	2025-11-06 22:07:40.487341	2025-11-06 22:07:40.487341
6953eed2-9705-45d8-8628-c18bd4c7e51f	AP Invoice Payments	ap_invoice_payments	Accounts payable payments	Transactions	2025-11-06 22:07:40.513825	2025-11-06 22:07:40.513825
2c31bc3b-aeb4-4a3d-b709-819e5e7f693f	Purchase Orders	purchase_orders	Purchase order headers	Transactions	2025-11-06 22:07:40.540123	2025-11-06 22:07:40.540123
b7503f02-a1c0-4162-995b-8cf823d293b1	Purchase Order Lines	purchase_order_lines	Purchase order line items	Transactions	2025-11-06 22:07:40.566275	2025-11-06 22:07:40.566275
a95e4564-d045-40e5-bfd4-ecaddc30cd44	Companies	companies	Top-level company entities in the organizational hierarchy	Organization Hierarchy	2025-11-10 23:57:26.579293	2025-11-10 23:57:26.579293
10e4b05b-5608-42ba-842f-7eb76ab6857f	Business Units	business_units	Business units within companies	Organization Hierarchy	2025-11-10 23:57:26.603679	2025-11-10 23:57:26.603679
0fccce1d-0fd9-4849-a4d3-330ec3f6e80e	Locations	locations	Physical or logical locations within business units	Organization Hierarchy	2025-11-10 23:57:26.625694	2025-11-10 23:57:26.625694
1180d528-178d-4144-aabd-1647c60a6e5b	Organizations	organizations	Organization hierarchy	Master Data	2025-11-10 23:57:26.646727	2025-11-10 23:57:26.646727
c0f9fc27-6769-44ab-9b6d-0d4b33417196	Business Units (Template)	business_units_template	Business unit template data	Master Data	2025-11-10 23:57:26.668926	2025-11-10 23:57:26.668926
\.


--
-- Data for Name: licenseiq_entity_records; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.licenseiq_entity_records (id, entity_id, record_data, created_by, created_at, updated_at, grp_id, org_id, loc_id) FROM stdin;
\.


--
-- Data for Name: licenseiq_fields; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.licenseiq_fields (id, entity_id, field_name, data_type, description, is_required, default_value, validation_rules, created_at, updated_at) FROM stdin;
a35ba362-a4ef-4057-90d8-409bbab66456	lic-sales-data	transactionId	string	Unique transaction identifier	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
64676810-47c9-4bbf-80ea-a51cf79e911b	lic-sales-data	transactionDate	date	Date of the sales transaction	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
dfa28a4b-c201-4aa1-b426-cf5f5c64475e	lic-sales-data	productCode	string	Product or SKU code	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
463e5ff7-335b-4d5a-8ae2-eae26bc721b3	lic-sales-data	productName	string	Product description or name	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
7d15b220-a100-4c38-aeb9-35c8e9f5119f	lic-sales-data	quantity	number	Quantity sold	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
51b65f9f-3387-4ee1-9ca7-fc767d5cd9c9	lic-sales-data	grossAmount	number	Gross sales amount	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
7695e072-e3ff-407f-9403-8cad98fe084c	lic-sales-data	netAmount	number	Net sales amount after deductions	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
879bcbfe-ae5f-41ca-8654-c297d302d1b9	lic-sales-data	currency	string	Currency code (USD, EUR, etc.)	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
b903b426-58f1-4607-aaed-1a01416f39b6	lic-sales-data	territory	string	Sales territory or region	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
15e8983c-b5cb-4e59-9b53-6aa6e824822c	lic-sales-data	category	string	Product category	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
e2f8f7a2-628d-49d9-acd7-37c82ce8701f	lic-sales-data	customerName	string	Customer or end-user name	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
71e4d3cc-4860-49a7-832d-973f4b2c6fd0	lic-sales-data	salesChannel	string	Sales channel (Direct, Retail, Online)	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
699bb0a1-8282-457f-9743-dd4dd9af84a0	lic-contract-terms	contractNumber	string	Unique contract identifier	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
a3be9c03-a5f2-47fb-8bb9-32f8b82e9488	lic-contract-terms	displayName	string	User-friendly contract name	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
cf469a8d-e338-4da4-941a-011af780b96a	lic-contract-terms	organizationName	string	Your organization name	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
cb0d28c1-d611-4c97-9a9e-78a29139c933	lic-contract-terms	counterpartyName	string	Other party in the contract	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
65d80eba-d2f8-4e8c-a505-2a644586383b	lic-contract-terms	effectiveStart	date	Contract effective start date	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
d6ba8427-4ba5-4bf6-b072-decf37f9548b	lic-contract-terms	effectiveEnd	date	Contract expiration date	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
7b95dd3c-9268-49cb-9160-3fec9ca645ca	lic-contract-terms	contractType	string	Type of contract (License, Service, etc.)	t	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
7f424405-dfaa-4ac9-a763-cb0d17ec0dcd	lic-contract-terms	governingLaw	string	Jurisdiction or governing law	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
e6420a47-4d83-406f-849a-ea0aa186c6e4	lic-contract-terms	renewalTerms	string	Renewal terms and conditions	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
1771e32f-73b0-4104-beaa-0f536e725227	lic-contract-terms	paymentTerms	string	Payment terms description	f	\N	\N	2025-11-06 02:54:28.619742	2025-11-06 02:54:28.619742
2240537a-cc75-4333-8457-2e7c22270a8c	lic-royalty-rules	ruleName	string	Descriptive name for the rule	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
ded49b92-e2d9-4303-a0a3-e1f660053ea4	lic-royalty-rules	ruleType	string	Type of rule (Flat Rate, Tiered, Conditional)	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
c68aee9c-7860-476d-9eee-212f2245644e	lic-royalty-rules	baseRate	number	Base royalty rate or percentage	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
cd9d05cb-8902-4cb3-a5a6-f27de4654a31	lic-royalty-rules	minimumPayment	number	Minimum guaranteed payment	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
d68b8a43-f859-495c-a7f3-b129e169da8f	lic-royalty-rules	maximumCap	number	Maximum payment cap	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
d6764962-3f84-4c35-a019-ac98d523fc15	lic-royalty-rules	territory	string	Applicable territory or region	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
9cb3de66-760d-4a6b-a5e6-d27baa6a26fc	lic-royalty-rules	productCategory	string	Applicable product category	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
072eebcb-3b34-4273-b1ad-6b2166823bd5	lic-royalty-rules	effectiveDate	date	Rule effective start date	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
c791459c-9184-44ef-a946-4b80c17249a0	lic-royalty-rules	expirationDate	date	Rule expiration date	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
9a6145c6-b74a-42fe-be89-3b27bbab6bc1	lic-royalty-rules	calculationFormula	string	Formula or logic for calculation	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
ffc84588-4350-4561-ab8a-6dec00170f0c	lic-payments	paymentId	string	Unique payment identifier	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
75d27d35-da61-4919-a58a-68878dc999bf	lic-payments	calculationDate	date	Date payment was calculated	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
842d08d1-56b3-44ab-9303-d328d0feee4b	lic-payments	periodStart	date	Payment period start date	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
160fcab4-3c6d-46aa-a71e-1c62895d49bd	lic-payments	periodEnd	date	Payment period end date	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
d0083c96-69bb-482a-bf23-56df70df4bc2	lic-payments	totalAmount	number	Total payment amount	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
42e5a6f3-514f-41af-9691-040e63bb0d25	lic-payments	currency	string	Payment currency	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
ceb8139a-c940-4f9f-bf47-07471bc5ab64	lic-payments	status	string	Payment status (Pending, Approved, Paid)	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
a83672fa-ade1-4042-a1a3-6e462994da41	lic-payments	transactionCount	number	Number of transactions included	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
20b934c6-5143-4d0a-8de0-caa45a992470	lic-payments	appliedRules	string	Rules used in calculation	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
4f0b77b1-40f1-4fef-bafe-2e70c35b7a79	lic-payments	notes	string	Additional notes or comments	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
2eeb5ea1-c3f9-4817-bce5-57ba24f3ef04	lic-products	productCode	string	Unique product identifier or SKU	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
1ddfc6fb-69be-45be-9594-ae81408d290c	lic-products	productName	string	Product name or description	t	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
07b293cb-a273-40a4-bc8f-d82e410990d8	lic-products	category	string	Product category	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
c49f0b39-4ade-4ea9-b95f-a234b710a619	lic-products	subcategory	string	Product subcategory	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
e66f0f58-7bb0-43ad-8b20-103110fa73c6	lic-products	unitOfMeasure	string	Unit of measure (Each, Kg, Liter)	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
8056be6f-d310-45a5-b953-4295e45e94d6	lic-products	listPrice	number	Standard list price	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
2ffa1694-8106-4287-85c2-f36af89cdf8d	lic-products	royaltyRate	number	Default royalty rate for this product	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
2163244d-dd28-4b8f-ac31-62d751ea64e5	lic-products	isActive	boolean	Whether product is currently active	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
cb8b6a1e-ce1e-4bea-9ac2-07025e9c3063	lic-products	manufacturer	string	Product manufacturer	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
a1e4c729-2a85-4561-938c-a464f98cc8d1	lic-products	territory	string	Geographic territory for product	f	\N	\N	2025-11-06 02:54:36.717685	2025-11-06 02:54:36.717685
df74b20a-5bd5-4d7b-acfe-96fdc5bf9c23	2948b6e4-c3ee-430b-a181-9e2970e81505	customerCode	text	Unique customer code	t	\N	\N	2025-11-10 21:23:04.749064	2025-11-10 21:23:04.749064
16ca251f-b0a3-4e54-9995-3d7cc99c4dc7	2948b6e4-c3ee-430b-a181-9e2970e81505	customerName	text	Customer full name	t	\N	\N	2025-11-10 21:23:04.773352	2025-11-10 21:23:04.773352
8dfd108d-f38b-497e-bc8c-a427838a2cd9	2948b6e4-c3ee-430b-a181-9e2970e81505	email	text	Customer email address	f	\N	\N	2025-11-10 21:23:04.797623	2025-11-10 21:23:04.797623
9ffe9340-7ab9-42f4-a3e0-db6cc1e1bcf3	2948b6e4-c3ee-430b-a181-9e2970e81505	phone	text	Contact phone number	f	\N	\N	2025-11-10 21:23:04.823123	2025-11-10 21:23:04.823123
488d798c-f21a-4180-bcb2-c82148146fc8	2948b6e4-c3ee-430b-a181-9e2970e81505	category	text	Customer category	f	\N	\N	2025-11-10 21:23:04.848505	2025-11-10 21:23:04.848505
75d4c117-e025-4912-95fd-c7f5b460d531	2948b6e4-c3ee-430b-a181-9e2970e81505	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:04.877637	2025-11-10 21:23:04.877637
09797222-3843-4ca4-a13a-9d8006d15b71	3a8e3cfb-e3b1-460a-9567-e7ab62be0c00	itemCode	text	Unique item code	t	\N	\N	2025-11-10 21:23:04.926225	2025-11-10 21:23:04.926225
763cd9fa-af8e-4c7f-8354-087870607035	3a8e3cfb-e3b1-460a-9567-e7ab62be0c00	itemName	text	Item description	t	\N	\N	2025-11-10 21:23:04.951868	2025-11-10 21:23:04.951868
76598e69-b3b0-46b1-9f3f-cbf373717b20	3a8e3cfb-e3b1-460a-9567-e7ab62be0c00	category	text	Item category	f	\N	\N	2025-11-10 21:23:04.976422	2025-11-10 21:23:04.976422
ed83ed4d-8519-47a6-8300-ed15337345cb	3a8e3cfb-e3b1-460a-9567-e7ab62be0c00	unitPrice	number	Standard unit price	f	\N	\N	2025-11-10 21:23:05.000699	2025-11-10 21:23:05.000699
ce2451b1-66af-4b53-9f70-e119a21bd0cf	3a8e3cfb-e3b1-460a-9567-e7ab62be0c00	uom	text	Unit of measure	f	\N	\N	2025-11-10 21:23:05.036269	2025-11-10 21:23:05.036269
d2190693-71ff-40b0-afdf-b6b486e0ff3e	3a8e3cfb-e3b1-460a-9567-e7ab62be0c00	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:05.060382	2025-11-10 21:23:05.060382
5eb8f230-929a-433c-a38d-62efec81b658	f9ae69a5-7fc7-4b31-8096-0742c2c35991	categoryCode	text	Category code	t	\N	\N	2025-11-10 21:23:05.111576	2025-11-10 21:23:05.111576
ed3e24f0-1060-4a34-bba4-85e4a898a46d	f9ae69a5-7fc7-4b31-8096-0742c2c35991	categoryName	text	Category name	t	\N	\N	2025-11-10 21:23:05.139344	2025-11-10 21:23:05.139344
6b79b574-5e57-4c63-bb6f-ec7802976f01	f9ae69a5-7fc7-4b31-8096-0742c2c35991	parentCategory	text	Parent category code	f	\N	\N	2025-11-10 21:23:05.163722	2025-11-10 21:23:05.163722
e14d806b-79c3-4158-8230-b97159565976	f9ae69a5-7fc7-4b31-8096-0742c2c35991	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:05.187941	2025-11-10 21:23:05.187941
5115ca71-9ec1-4c84-a227-cbd372970960	60522d4a-d393-4d69-b728-8caec273ebeb	classCode	text	Class code	t	\N	\N	2025-11-10 21:23:05.235796	2025-11-10 21:23:05.235796
9b32de83-c646-40a9-b5e3-9946bf32b916	60522d4a-d393-4d69-b728-8caec273ebeb	className	text	Class name	t	\N	\N	2025-11-10 21:23:05.260682	2025-11-10 21:23:05.260682
5698735f-0bd4-40e4-bfa5-46e2828adeac	60522d4a-d393-4d69-b728-8caec273ebeb	description	text	Class description	f	\N	\N	2025-11-10 21:23:05.285108	2025-11-10 21:23:05.285108
71c1ba11-642b-4276-908c-2a43a103e8a0	60522d4a-d393-4d69-b728-8caec273ebeb	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:05.309454	2025-11-10 21:23:05.309454
b4d6a63f-2c2a-4f48-8adf-aeca711c3f15	cd1becd4-fc55-4fd8-908a-491661ddfb95	catalogCode	text	Catalog code	t	\N	\N	2025-11-10 21:23:05.357536	2025-11-10 21:23:05.357536
8e4758fd-bc54-4e26-a212-af5dc7d435b5	cd1becd4-fc55-4fd8-908a-491661ddfb95	catalogName	text	Catalog name	t	\N	\N	2025-11-10 21:23:05.383998	2025-11-10 21:23:05.383998
9e9eddbd-8ee4-45d2-8b41-8698985a082e	cd1becd4-fc55-4fd8-908a-491661ddfb95	effectiveDate	date	Effective from date	f	\N	\N	2025-11-10 21:23:05.412015	2025-11-10 21:23:05.412015
20197f87-be90-40e8-9f25-09dea7530865	cd1becd4-fc55-4fd8-908a-491661ddfb95	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:05.437873	2025-11-10 21:23:05.437873
d0441a71-1354-41a3-b9d9-8be9cbcd7d9d	e326e98c-70f2-4bc2-a44b-bc5d57a755c0	structureCode	text	Structure code	t	\N	\N	2025-11-10 21:23:05.486363	2025-11-10 21:23:05.486363
d845d36c-a397-493e-b8e5-1bb1fe77a698	e326e98c-70f2-4bc2-a44b-bc5d57a755c0	parentItem	text	Parent item code	t	\N	\N	2025-11-10 21:23:05.510369	2025-11-10 21:23:05.510369
0a2912a9-4bb7-445a-a0ef-b98a1d7c8532	e326e98c-70f2-4bc2-a44b-bc5d57a755c0	childItem	text	Child item code	t	\N	\N	2025-11-10 21:23:05.53439	2025-11-10 21:23:05.53439
a219f55a-c3ac-452a-89be-eb45fc7ced91	e326e98c-70f2-4bc2-a44b-bc5d57a755c0	quantity	number	Component quantity	t	\N	\N	2025-11-10 21:23:05.558548	2025-11-10 21:23:05.558548
f0059951-68ba-4709-a344-14be9492aea5	e326e98c-70f2-4bc2-a44b-bc5d57a755c0	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:05.58301	2025-11-10 21:23:05.58301
cfcf9318-2da8-45f6-baaf-9d0938796a3f	d8a5768f-b33c-469f-8b39-1055e3de0628	siteCode	text	Site code	t	\N	\N	2025-11-10 21:23:05.631367	2025-11-10 21:23:05.631367
903990db-dbe5-46a2-aada-b450bcdea1e2	d8a5768f-b33c-469f-8b39-1055e3de0628	customerCode	text	Customer code	t	\N	\N	2025-11-10 21:23:05.655036	2025-11-10 21:23:05.655036
1bd31565-57b1-4f63-9bc8-8f179e1504ef	d8a5768f-b33c-469f-8b39-1055e3de0628	siteName	text	Site name	t	\N	\N	2025-11-10 21:23:05.680088	2025-11-10 21:23:05.680088
d2ef62f6-28e0-4529-b14e-8bce7b56cb2d	d8a5768f-b33c-469f-8b39-1055e3de0628	address	text	Site address	f	\N	\N	2025-11-10 21:23:05.710032	2025-11-10 21:23:05.710032
2da84073-619c-4870-9a88-ff54371c665c	d8a5768f-b33c-469f-8b39-1055e3de0628	city	text	City	f	\N	\N	2025-11-10 21:23:05.73423	2025-11-10 21:23:05.73423
77ea8cd0-1f53-4e2d-8d23-9e344c40188e	d8a5768f-b33c-469f-8b39-1055e3de0628	country	text	Country	f	\N	\N	2025-11-10 21:23:05.758323	2025-11-10 21:23:05.758323
9bbaf645-74ba-4053-9510-6315f977c7d4	d8a5768f-b33c-469f-8b39-1055e3de0628	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:05.783723	2025-11-10 21:23:05.783723
df11710b-6f88-4506-92fc-f8dca34eb138	93737de6-4714-4af1-86a0-0fbac1517f22	siteUseCode	text	Site use code	t	\N	\N	2025-11-10 21:23:05.831986	2025-11-10 21:23:05.831986
2480a854-2437-465e-8b0b-8db7a4d1c89c	93737de6-4714-4af1-86a0-0fbac1517f22	siteCode	text	Site code	t	\N	\N	2025-11-10 21:23:05.855957	2025-11-10 21:23:05.855957
dec5c578-d2c1-4dfc-bdc3-e6f867843db7	93737de6-4714-4af1-86a0-0fbac1517f22	useType	text	Use type (Bill-To, Ship-To)	t	\N	\N	2025-11-10 21:23:05.879957	2025-11-10 21:23:05.879957
19b33b3d-d187-449d-8c81-ec3bf010deef	93737de6-4714-4af1-86a0-0fbac1517f22	isPrimary	boolean	Primary site flag	f	\N	\N	2025-11-10 21:23:05.903957	2025-11-10 21:23:05.903957
c1678c39-3b08-4318-b8ba-108212e9c4d5	93737de6-4714-4af1-86a0-0fbac1517f22	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:05.928625	2025-11-10 21:23:05.928625
7904d297-c803-46e6-b638-ad3d052fbf80	223227dc-23b4-4ade-98b2-d76e1d3e379a	supplierCode	text	Supplier code	t	\N	\N	2025-11-10 21:23:05.975783	2025-11-10 21:23:05.975783
33f2c3a5-399f-4e10-979e-15a045b01099	223227dc-23b4-4ade-98b2-d76e1d3e379a	supplierName	text	Supplier name	t	\N	\N	2025-11-10 21:23:06.002003	2025-11-10 21:23:06.002003
d964d21a-87d7-4eda-87f1-8757bdd778f1	223227dc-23b4-4ade-98b2-d76e1d3e379a	email	text	Contact email	f	\N	\N	2025-11-10 21:23:06.026812	2025-11-10 21:23:06.026812
66dcbe43-4d22-4861-92fe-e0f794a44820	223227dc-23b4-4ade-98b2-d76e1d3e379a	phone	text	Contact phone	f	\N	\N	2025-11-10 21:23:06.054038	2025-11-10 21:23:06.054038
362a4ef1-19b6-405b-9bb5-613895e06102	223227dc-23b4-4ade-98b2-d76e1d3e379a	category	text	Supplier category	f	\N	\N	2025-11-10 21:23:06.078466	2025-11-10 21:23:06.078466
2e8d1665-07fb-4626-8d95-d8b148d6eb56	223227dc-23b4-4ade-98b2-d76e1d3e379a	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:06.103879	2025-11-10 21:23:06.103879
b62703c1-e539-49c0-b701-398938caa3f6	0d99a727-9d93-44a1-91e7-fad14dd5ff28	siteCode	text	Site code	t	\N	\N	2025-11-10 21:23:06.179263	2025-11-10 21:23:06.179263
c36b41e7-7b49-43b0-9727-4192788cc698	0d99a727-9d93-44a1-91e7-fad14dd5ff28	supplierCode	text	Supplier code	t	\N	\N	2025-11-10 21:23:06.205053	2025-11-10 21:23:06.205053
c570b6a1-633d-4ea6-9adb-bd195d3c8197	0d99a727-9d93-44a1-91e7-fad14dd5ff28	siteName	text	Site name	t	\N	\N	2025-11-10 21:23:06.234292	2025-11-10 21:23:06.234292
e7ce2cb9-1edb-4209-9d37-af7c92e9559b	0d99a727-9d93-44a1-91e7-fad14dd5ff28	address	text	Site address	f	\N	\N	2025-11-10 21:23:06.259609	2025-11-10 21:23:06.259609
d6dc48ea-dd95-4d89-92d7-c37712075dbd	0d99a727-9d93-44a1-91e7-fad14dd5ff28	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:06.306075	2025-11-10 21:23:06.306075
c33044d3-2f2b-4cea-bcfd-c2862def5994	15d63ead-a068-4609-9a2c-0508571f7dc0	code	text	Terms code	t	\N	\N	2025-11-10 21:23:06.358673	2025-11-10 21:23:06.358673
0e3bf039-04f8-47ea-b65a-93dc5c067a20	15d63ead-a068-4609-9a2c-0508571f7dc0	name	text	Terms name	t	\N	\N	2025-11-10 21:23:06.383048	2025-11-10 21:23:06.383048
2341830e-69c0-4706-b18e-6d399db359dc	15d63ead-a068-4609-9a2c-0508571f7dc0	dueDays	number	Due in days	t	\N	\N	2025-11-10 21:23:06.407818	2025-11-10 21:23:06.407818
25ef048f-4afd-4a05-b22c-506e1d28d194	15d63ead-a068-4609-9a2c-0508571f7dc0	discountPercent	number	Discount percentage	f	\N	\N	2025-11-10 21:23:06.430969	2025-11-10 21:23:06.430969
f5f1aa4c-f91e-4ef4-89fa-9e218c2500fc	15d63ead-a068-4609-9a2c-0508571f7dc0	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:06.455664	2025-11-10 21:23:06.455664
326cc2bd-4a3e-457c-b25a-f7f2e112d83b	e309d11d-a60f-43ed-abf0-a7883822efbc	accountCode	text	GL account code	t	\N	\N	2025-11-10 21:23:06.661213	2025-11-10 21:23:06.661213
e147d166-be4e-4926-b145-ba43111287db	e309d11d-a60f-43ed-abf0-a7883822efbc	accountName	text	Account name	t	\N	\N	2025-11-10 21:23:06.690065	2025-11-10 21:23:06.690065
ddc795e1-ae93-47b7-99c5-ee8a7b2f49bd	e309d11d-a60f-43ed-abf0-a7883822efbc	accountType	text	Account type (Asset/Liability/Revenue/Expense)	t	\N	\N	2025-11-10 21:23:06.71674	2025-11-10 21:23:06.71674
ba497342-6619-4f8f-90af-ee1297a861c8	e309d11d-a60f-43ed-abf0-a7883822efbc	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:06.740137	2025-11-10 21:23:06.740137
522752ed-54b3-4af4-90e5-05f341ddebe2	b0b42d29-9027-4300-b9bf-84461003b64d	repCode	text	Sales rep code	t	\N	\N	2025-11-10 21:23:06.790835	2025-11-10 21:23:06.790835
6a8a3b27-1be3-4a39-9f8a-7886544d7b9b	b0b42d29-9027-4300-b9bf-84461003b64d	repName	text	Sales rep name	t	\N	\N	2025-11-10 21:23:06.81714	2025-11-10 21:23:06.81714
f4675ee4-8036-408a-9378-f9f6779c1855	b0b42d29-9027-4300-b9bf-84461003b64d	email	text	Email address	f	\N	\N	2025-11-10 21:23:06.841201	2025-11-10 21:23:06.841201
f4af9136-ccc2-41c6-bb62-d3d16be8e78c	b0b42d29-9027-4300-b9bf-84461003b64d	territory	text	Sales territory	f	\N	\N	2025-11-10 21:23:06.865707	2025-11-10 21:23:06.865707
36bb367b-0f8a-4667-9d53-fe96953cb401	b0b42d29-9027-4300-b9bf-84461003b64d	isActive	boolean	Active status	t	\N	\N	2025-11-10 21:23:06.889603	2025-11-10 21:23:06.889603
e25d9a86-00b8-4354-8d84-a5f7312de836	028b76f4-43e6-4ee1-a7f2-f3e678b35ad2	empCode	text	Employee code	t	\N	\N	2025-11-10 21:23:06.937223	2025-11-10 21:23:06.937223
5c1da512-cbcc-4810-890e-d7db03054648	028b76f4-43e6-4ee1-a7f2-f3e678b35ad2	empName	text	Employee name	t	\N	\N	2025-11-10 21:23:06.961494	2025-11-10 21:23:06.961494
5cbd5ad4-2e39-4076-905c-c5c3b74a9ef2	028b76f4-43e6-4ee1-a7f2-f3e678b35ad2	department	text	Department	f	\N	\N	2025-11-10 21:23:06.985422	2025-11-10 21:23:06.985422
3a0013e5-ed42-4483-ae99-c7fa6a728b9d	028b76f4-43e6-4ee1-a7f2-f3e678b35ad2	position	text	Job position	f	\N	\N	2025-11-10 21:23:07.010201	2025-11-10 21:23:07.010201
afe7578d-e01e-4154-9667-a2dd689a0f26	028b76f4-43e6-4ee1-a7f2-f3e678b35ad2	hireDate	date	Hire date	f	\N	\N	2025-11-10 21:23:07.036851	2025-11-10 21:23:07.036851
6a19d184-f98e-4e70-a6d4-0945c5c1eb21	269ecbac-5b9f-4b93-8699-4c69a3f0431b	orderNumber	text	Sales order number	t	\N	\N	2025-11-10 21:23:07.087679	2025-11-10 21:23:07.087679
b322584f-363d-4acd-a6de-697bc5456189	269ecbac-5b9f-4b93-8699-4c69a3f0431b	customerCode	text	Customer code	t	\N	\N	2025-11-10 21:23:07.113022	2025-11-10 21:23:07.113022
21dae1f5-5afc-42d9-a061-c07fb1cec603	269ecbac-5b9f-4b93-8699-4c69a3f0431b	orderDate	date	Order date	t	\N	\N	2025-11-10 21:23:07.138063	2025-11-10 21:23:07.138063
ea6778de-7a42-48dd-ae11-af1c7a6ea6e0	269ecbac-5b9f-4b93-8699-4c69a3f0431b	totalAmount	number	Order total amount	t	\N	\N	2025-11-10 21:23:07.16365	2025-11-10 21:23:07.16365
b45ae9c5-cf9e-4b74-a385-9b8c2df2b2ff	269ecbac-5b9f-4b93-8699-4c69a3f0431b	status	text	Order status	t	\N	\N	2025-11-10 21:23:07.187496	2025-11-10 21:23:07.187496
79491a45-65ff-47f7-9a2e-870a796f1841	269ecbac-5b9f-4b93-8699-4c69a3f0431b	salesRep	text	Sales rep code	f	\N	\N	2025-11-10 21:23:07.216645	2025-11-10 21:23:07.216645
0a1153e8-ad13-44ba-824b-5ffb40318780	c11f3d4c-8767-4333-b287-874cf3ccf117	lineNumber	number	Line number	t	\N	\N	2025-11-10 21:23:07.26958	2025-11-10 21:23:07.26958
4f53b322-3425-4ec7-92b6-59f54331790e	c11f3d4c-8767-4333-b287-874cf3ccf117	orderNumber	text	Sales order number	t	\N	\N	2025-11-10 21:23:07.293696	2025-11-10 21:23:07.293696
ae4ab485-68f7-423a-a6ab-0048b3e4fa84	c11f3d4c-8767-4333-b287-874cf3ccf117	itemCode	text	Item code	t	\N	\N	2025-11-10 21:23:07.320766	2025-11-10 21:23:07.320766
a4c8ed33-e2dd-43dd-a130-54c226e4b149	c11f3d4c-8767-4333-b287-874cf3ccf117	quantity	number	Ordered quantity	t	\N	\N	2025-11-10 21:23:07.345922	2025-11-10 21:23:07.345922
49dd7125-8450-4aac-addb-9280b0fc4723	c11f3d4c-8767-4333-b287-874cf3ccf117	unitPrice	number	Unit price	t	\N	\N	2025-11-10 21:23:07.371157	2025-11-10 21:23:07.371157
cceea8f0-9eda-403a-9caa-57f04e7684be	c11f3d4c-8767-4333-b287-874cf3ccf117	lineTotal	number	Line total	t	\N	\N	2025-11-10 21:23:07.395698	2025-11-10 21:23:07.395698
d5c3bdf9-e2e6-4183-95a4-b7d8c3e45d70	b597d8b0-25c0-4026-ba1b-9a25ad148f40	invoiceNumber	text	Invoice number	t	\N	\N	2025-11-10 21:23:07.443387	2025-11-10 21:23:07.443387
7649dad5-cda1-4022-8fd3-ef29b156c99d	b597d8b0-25c0-4026-ba1b-9a25ad148f40	customerCode	text	Customer code	t	\N	\N	2025-11-10 21:23:07.467106	2025-11-10 21:23:07.467106
d3caab5e-1ac7-41d8-9301-2fa331c97a6c	b597d8b0-25c0-4026-ba1b-9a25ad148f40	invoiceDate	date	Invoice date	t	\N	\N	2025-11-10 21:23:07.493151	2025-11-10 21:23:07.493151
f66a9e7b-6516-4e6f-98f1-879091a21f6d	b597d8b0-25c0-4026-ba1b-9a25ad148f40	amount	number	Invoice amount	t	\N	\N	2025-11-10 21:23:07.517588	2025-11-10 21:23:07.517588
7bbef5f9-b19c-4b77-943c-e79727c51fbd	b597d8b0-25c0-4026-ba1b-9a25ad148f40	status	text	Invoice status	t	\N	\N	2025-11-10 21:23:07.542449	2025-11-10 21:23:07.542449
99b1dc6f-dd8e-49a5-bbe2-aae9b5f6c246	b597d8b0-25c0-4026-ba1b-9a25ad148f40	dueDate	date	Payment due date	f	\N	\N	2025-11-10 21:23:07.566993	2025-11-10 21:23:07.566993
ff9f9ff9-eb3d-4007-8c4a-0efc1030a4be	bc08ed69-0bcc-4785-975b-8bcc0fd19c15	lineNumber	number	Line number	t	\N	\N	2025-11-10 21:23:07.617195	2025-11-10 21:23:07.617195
63a80c83-7c8b-4e3a-8656-cc761aa75526	bc08ed69-0bcc-4785-975b-8bcc0fd19c15	invoiceNumber	text	Invoice number	t	\N	\N	2025-11-10 21:23:07.642786	2025-11-10 21:23:07.642786
9278afb3-a89e-43be-b2d5-00cefcff14f6	bc08ed69-0bcc-4785-975b-8bcc0fd19c15	description	text	Line description	t	\N	\N	2025-11-10 21:23:07.667455	2025-11-10 21:23:07.667455
6d681fa6-ae54-41fa-8bf8-8ee08e2d2b32	bc08ed69-0bcc-4785-975b-8bcc0fd19c15	amount	number	Line amount	t	\N	\N	2025-11-10 21:23:07.691515	2025-11-10 21:23:07.691515
acad61ef-5e7f-4106-9816-59ddf2b43adb	bc08ed69-0bcc-4785-975b-8bcc0fd19c15	quantity	number	Quantity	t	\N	\N	2025-11-10 21:23:07.716715	2025-11-10 21:23:07.716715
5df8ff5a-ec60-46a1-808d-4ac11d4f002a	d049d404-086b-4b89-b62e-ce249e7f8be4	invoiceNumber	text	AP invoice number	t	\N	\N	2025-11-10 21:23:07.766398	2025-11-10 21:23:07.766398
02cfa251-e9e9-4dd2-86f6-36b782be64fa	d049d404-086b-4b89-b62e-ce249e7f8be4	supplierCode	text	Supplier code	t	\N	\N	2025-11-10 21:23:07.79766	2025-11-10 21:23:07.79766
5f86883a-b133-4134-9c40-cd163dee2845	d049d404-086b-4b89-b62e-ce249e7f8be4	invoiceDate	date	Invoice date	t	\N	\N	2025-11-10 21:23:07.826075	2025-11-10 21:23:07.826075
f9fe6bcf-1118-4b49-aa04-9b9079eeaf35	d049d404-086b-4b89-b62e-ce249e7f8be4	amount	number	Invoice amount	t	\N	\N	2025-11-10 21:23:07.849004	2025-11-10 21:23:07.849004
8ae8e09f-012c-44a9-9ae2-efca3cc0b162	d049d404-086b-4b89-b62e-ce249e7f8be4	status	text	Status	t	\N	\N	2025-11-10 21:23:07.875614	2025-11-10 21:23:07.875614
e97eec69-3083-4ed0-b235-42640f986db4	d049d404-086b-4b89-b62e-ce249e7f8be4	dueDate	date	Payment due date	f	\N	\N	2025-11-10 21:23:07.898925	2025-11-10 21:23:07.898925
7cde8bb7-5ed2-4859-9c94-aed11b9138fb	ad5852cf-89df-4943-b381-3bd1d2a2ba56	lineNumber	number	Line number	t	\N	\N	2025-11-10 21:23:07.946389	2025-11-10 21:23:07.946389
fa7479c1-9587-4d58-8728-dd4fcc4a1bc3	ad5852cf-89df-4943-b381-3bd1d2a2ba56	invoiceNumber	text	AP invoice number	t	\N	\N	2025-11-10 21:23:07.971539	2025-11-10 21:23:07.971539
40e7a684-76c5-4308-ac34-0b7b8c8f0fa5	ad5852cf-89df-4943-b381-3bd1d2a2ba56	description	text	Line description	t	\N	\N	2025-11-10 21:23:07.99537	2025-11-10 21:23:07.99537
ab615c8a-1847-4a97-b801-ab791eea37a0	ad5852cf-89df-4943-b381-3bd1d2a2ba56	amount	number	Line amount	t	\N	\N	2025-11-10 21:23:08.019705	2025-11-10 21:23:08.019705
77e12ef5-9765-4a5c-bc9b-c6f31edcf25b	ad5852cf-89df-4943-b381-3bd1d2a2ba56	quantity	number	Quantity	t	\N	\N	2025-11-10 21:23:08.043779	2025-11-10 21:23:08.043779
06d9f081-eff1-4b0d-a869-398809e9abe4	6953eed2-9705-45d8-8628-c18bd4c7e51f	paymentNumber	text	Payment number	t	\N	\N	2025-11-10 21:23:08.092047	2025-11-10 21:23:08.092047
9fec81ff-e779-4518-bedd-b24d7c970b3a	6953eed2-9705-45d8-8628-c18bd4c7e51f	invoiceNumber	text	AP invoice number	t	\N	\N	2025-11-10 21:23:08.11621	2025-11-10 21:23:08.11621
5ec9559d-df70-4f63-b817-294632e9627f	6953eed2-9705-45d8-8628-c18bd4c7e51f	paymentDate	date	Payment date	t	\N	\N	2025-11-10 21:23:08.141205	2025-11-10 21:23:08.141205
e572c27d-e864-48e9-bd80-9111a38593e5	6953eed2-9705-45d8-8628-c18bd4c7e51f	paymentAmount	number	Payment amount	t	\N	\N	2025-11-10 21:23:08.166238	2025-11-10 21:23:08.166238
f52275ba-2b3f-48fc-875d-ab8d41e47c70	6953eed2-9705-45d8-8628-c18bd4c7e51f	paymentMethod	text	Payment method	f	\N	\N	2025-11-10 21:23:08.193247	2025-11-10 21:23:08.193247
6d350db5-d42d-4054-b3e2-6d527616b5a6	2c31bc3b-aeb4-4a3d-b709-819e5e7f693f	poNumber	text	PO number	t	\N	\N	2025-11-10 21:23:08.24809	2025-11-10 21:23:08.24809
c5f995a2-5209-42b6-88dd-e61a215926fc	2c31bc3b-aeb4-4a3d-b709-819e5e7f693f	supplierCode	text	Supplier code	t	\N	\N	2025-11-10 21:23:08.279106	2025-11-10 21:23:08.279106
1fc7c323-1432-416f-ab60-58747827fc64	2c31bc3b-aeb4-4a3d-b709-819e5e7f693f	orderDate	date	Order date	t	\N	\N	2025-11-10 21:23:08.30621	2025-11-10 21:23:08.30621
06e4559c-fa0e-4142-9a1d-fade7dd475cc	2c31bc3b-aeb4-4a3d-b709-819e5e7f693f	totalAmount	number	Total amount	t	\N	\N	2025-11-10 21:23:08.331946	2025-11-10 21:23:08.331946
df229bf9-4d1d-421c-944e-906b33b4747c	2c31bc3b-aeb4-4a3d-b709-819e5e7f693f	status	text	PO status	t	\N	\N	2025-11-10 21:23:08.356636	2025-11-10 21:23:08.356636
f12ecbc4-1ecd-474b-b3ab-a52f2dab9adf	b7503f02-a1c0-4162-995b-8cf823d293b1	lineNumber	number	Line number	t	\N	\N	2025-11-10 21:23:08.407486	2025-11-10 21:23:08.407486
fbfffe9b-77bb-4210-8cb8-b7c2180b1a20	b7503f02-a1c0-4162-995b-8cf823d293b1	poNumber	text	PO number	t	\N	\N	2025-11-10 21:23:08.431874	2025-11-10 21:23:08.431874
0dba5ba2-2369-4f2c-adc9-21bd94e74366	b7503f02-a1c0-4162-995b-8cf823d293b1	itemCode	text	Item code	t	\N	\N	2025-11-10 21:23:08.456857	2025-11-10 21:23:08.456857
a2cb1384-462e-4cd7-933e-911f48163783	b7503f02-a1c0-4162-995b-8cf823d293b1	quantity	number	Order quantity	t	\N	\N	2025-11-10 21:23:08.4802	2025-11-10 21:23:08.4802
b75ae682-c7b0-4fad-9cc4-41bc2c6b1fab	b7503f02-a1c0-4162-995b-8cf823d293b1	unitPrice	number	Unit price	t	\N	\N	2025-11-10 21:23:08.5058	2025-11-10 21:23:08.5058
1f9a5a1a-7515-47a9-b80c-0060d756bbbd	b7503f02-a1c0-4162-995b-8cf823d293b1	lineTotal	number	Line total	t	\N	\N	2025-11-10 21:23:08.535377	2025-11-10 21:23:08.535377
\.


--
-- Data for Name: locations; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.locations (loc_id, company_id, org_id, loc_name, loc_descr, address1, contact_person, contact_email, contact_phone, contact_preference, status, created_by, creation_date, last_updated_by, last_update_date) FROM stdin;
loc-001	cmp-001	org-001	New York Office	East Coast headquarters	350 Fifth Avenue, New York, NY	David Lee	david.lee@acmecorp.com	+1-212-555-0150	\N	A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407
loc-002	cmp-001	org-001	Los Angeles Office	West Coast sales center	1999 Avenue of the Stars, Los Angeles, CA	Jessica Martinez	jessica.m@acmecorp.com	+1-310-555-0160	\N	A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-10 22:24:13.625407
391f5e20-4161-4480-abca-a5b2a8f959f8	eeca99c0-de3e-4d69-8599-8ff6f1dc9dcc	7e06ef6e-0dfb-4068-8e93-17c770b7d053	Frisco	\N	\N	\N	\N	\N	\N	A	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-11 23:23:11.729601	993f41bb-59e7-4a3a-ad0d-25e808ca81c7	2025-11-11 23:23:11.729601
\.


--
-- Data for Name: market_benchmarks; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.market_benchmarks (id, contract_type, industry, benchmark_data, average_value, standard_terms, risk_factors, last_updated, created_at) FROM stdin;
\.


--
-- Data for Name: master_data_mappings; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.master_data_mappings (id, mapping_name, erp_system, entity_type, source_schema, target_schema, mapping_results, status, ai_model, created_by, notes, created_at, updated_at, customer_id) FROM stdin;
\.


--
-- Data for Name: navigation_permissions; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.navigation_permissions (id, item_key, item_name, href, icon_name, default_roles, is_active, sort_order, created_at, updated_at) FROM stdin;
e635693f-4475-41fa-8cbf-ccfa2d0bf414	configuration	Configuration	/configuration	Sparkles	["admin", "owner"]	t	20	2025-11-10 21:55:36.30616	2025-11-10 21:55:36.30616
730153ea-354e-4af6-ae87-ab98c5b2c6e3	dashboard	Dashboard	/	BarChart3	["user", "analyst", "admin", "owner"]	t	1	2025-11-06 22:28:44.516653	2025-11-10 21:56:38.305
8abb1a62-82a6-4231-9ec4-0cf96a717da5	contracts	Contracts	/contracts	File	["user", "analyst", "admin", "owner"]	t	2	2025-11-06 22:28:44.547198	2025-11-10 21:56:38.328
a8aad591-9262-4f19-9455-3270ca951a01	upload	Upload	/upload	Upload	["user", "analyst", "admin", "owner"]	t	3	2025-11-06 22:28:44.573039	2025-11-10 21:56:38.352
c7f0d610-81a4-4760-95fa-861a84ebd61a	sales-data	Sales Data	/sales-upload	Receipt	["analyst", "admin", "owner"]	t	4	2025-11-06 22:28:44.59951	2025-11-10 21:56:38.375
5a102caf-642f-4139-a7ab-d4c403050c40	master-data-mapping	Master Data Mapping	/master-data-mapping	Database	["admin", "owner"]	t	6	2025-11-06 22:28:44.650252	2025-11-10 21:56:38.421
8dc24fbc-314f-4d05-b928-41329ad1906d	erp-catalog	ERP Catalog	/erp-catalog	Database	["admin", "owner"]	t	7	2025-11-06 22:28:44.674821	2025-11-10 21:56:38.444
683904bb-33ff-4191-a572-d84c495f5171	licenseiq-schema	LicenseIQ Schema	/licenseiq-schema	Layers	["admin", "owner"]	t	8	2025-11-06 22:28:44.699514	2025-11-10 21:56:38.466
cc71d842-fd1c-43bb-aa6c-78a1231f601f	data-management	Data Management	/data-management	Table	["analyst", "admin", "owner"]	t	9	2025-11-06 22:28:44.7241	2025-11-10 21:56:38.49
7309c911-fb3a-4169-8a61-9d9faea6f6f4	master-data	Master Data	/master-data	Building2	["admin", "owner", "editor"]	t	10	2025-11-10 21:56:38.524272	2025-11-10 21:56:38.524272
8c0736a8-40a1-4ae2-9f64-0ae3e77cd606	erp-import	ERP Data Import	/erp-import	Upload	["admin", "owner"]	t	11	2025-11-06 22:28:44.748847	2025-11-10 21:56:38.539
9e34edee-4147-4a5a-aedb-81b8023a7731	rag-dashboard	RAG Dashboard	/rag-dashboard	Sparkles	["admin", "owner"]	t	13	2025-11-06 22:28:44.797926	2025-11-10 21:56:38.585
5d63bd56-fe32-467a-98ce-80da98295847	analytics	Analytics	/analytics	TrendingUp	["analyst", "admin", "owner"]	t	14	2025-11-06 22:28:44.822554	2025-11-10 21:56:38.607
e3e9bf68-6840-4c9b-bd8f-e2271d3940c8	reports	Reports	/reports	FileText	["analyst", "admin", "owner"]	t	15	2025-11-06 22:28:44.847155	2025-11-10 21:56:38.631
f6675ec4-c678-4d83-953d-d497319a7df8	lead-management	Lead Management	/admin/leads	Mail	["admin", "owner"]	t	16	2025-11-06 22:28:44.8706	2025-11-10 21:56:38.654
d5cd36b2-540e-4d42-b789-cbf93b522609	review-queue	Review Queue	/review-queue	ClipboardCheck	["admin", "owner"]	t	17	2025-11-06 22:28:44.895326	2025-11-10 21:56:38.677
062089f0-7da5-4081-b7e4-8310534e92d2	user-management	User Management	/users	Users	["admin", "owner"]	t	18	2025-11-06 22:28:44.920025	2025-11-10 21:56:38.699
429fcd65-4277-4a86-a02b-6ef8fe8ba085	audit-trail	Audit Trail	/audit	History	["auditor", "admin", "owner"]	t	19	2025-11-06 22:28:44.943634	2025-11-10 21:56:38.724
f8ebc477-e337-450b-b106-d102e14ab2f6	royalty-calculator	License Fee Calculator	/calculations	Calculator	["analyst", "admin", "owner"]	t	5	2025-11-06 22:28:44.62542	2025-11-11 18:53:51.133274
e34c9c55-3dba-4781-8569-ea757f2be266	contract-qna	LIQ AI	/contract-qna	Brain	["user", "analyst", "admin", "owner"]	t	12	2025-11-06 22:28:44.773858	2025-11-11 18:53:52.277118
\.


--
-- Data for Name: performance_metrics; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.performance_metrics (id, contract_id, performance_score, milestone_completion, on_time_delivery, budget_variance, quality_score, client_satisfaction, renewal_probability, last_review_date, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: role_navigation_permissions; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.role_navigation_permissions (id, role, nav_item_key, is_enabled, created_at, updated_at) FROM stdin;
3d050b73-7147-47fd-860d-cd0db7d858d9	user	sales-data	f	2025-11-06 22:34:06.391778	2025-11-06 22:34:08.332
f3838a10-b8c9-4001-897c-75b9a6050ce0	admin	contracts	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
22006cb2-f2d9-4e08-bf80-c2c83343ac67	admin	upload	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
20b00e25-a720-45b7-9008-1a93ed312241	admin	sales-data	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
d20e2e6f-5df2-48e2-bfcc-2c6b931d3416	admin	royalty-calculator	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
2ccab61f-087a-4593-8118-8e3a8403b2bb	admin	master-data-mapping	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
2e22f3a1-b392-48e8-aa79-b78632ff3363	admin	erp-catalog	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
7a46051a-2815-4df3-868b-3c4c0b0d25aa	admin	licenseiq-schema	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
10fefac6-ef0a-4ca4-ac41-eb661f92104f	admin	data-management	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
3c425a1c-da40-420e-943f-9adf6362efa6	admin	erp-import	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
d61d51d3-223b-469f-a2b6-813d1c949bd8	admin	contract-qna	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
fd76bd58-acfc-42ea-a6fa-2a84d8be6c74	admin	rag-dashboard	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
61900e62-9230-4e2f-9637-88a197d2351e	admin	analytics	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
1b41b423-47b5-4e98-8b2f-9e64bd617880	admin	reports	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
295011dd-f2a2-484a-bd49-164daab8dcb2	admin	lead-management	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
214f89c1-a044-4e98-826d-5331b34f7eb0	admin	review-queue	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
d43d8596-97ec-4577-bf72-a00add3c30cb	admin	user-management	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
197700e0-5c98-4670-b40a-ce8bb354ff85	admin	audit-trail	t	2025-11-06 22:43:13.124319	2025-11-06 22:43:13.124319
4af21e63-965f-4203-9625-29152faedb6d	admin	dashboard	t	2025-11-06 22:43:13.124319	2025-11-06 22:45:45.605
96ad76d3-84b1-4cf1-adfd-0f62c49c93e7	admin	configuration	t	2025-11-10 21:55:36.30616	2025-11-10 21:55:36.30616
bef48a07-b860-488e-a4e9-28915b7a8cd3	admin	dashboard	t	2025-11-10 21:56:38.758515	2025-11-10 21:56:38.758515
2347e77d-276e-4aa5-81cc-792d958da252	admin	contracts	t	2025-11-10 21:56:38.782294	2025-11-10 21:56:38.782294
47e67a3d-f5f5-4042-8eed-d895ba0ee7bd	admin	upload	t	2025-11-10 21:56:38.804596	2025-11-10 21:56:38.804596
57b51f94-b003-4d69-a432-04fdf4704c51	admin	sales-data	t	2025-11-10 21:56:38.826965	2025-11-10 21:56:38.826965
7db00989-ed46-4366-bbe0-2ed8a712838f	admin	royalty-calculator	t	2025-11-10 21:56:38.849085	2025-11-10 21:56:38.849085
6fa7db44-1c67-4517-a4bc-745c886a4e92	admin	master-data-mapping	t	2025-11-10 21:56:38.871592	2025-11-10 21:56:38.871592
9c5685a7-12d3-41f7-b1db-bbc8e35938f9	admin	erp-catalog	t	2025-11-10 21:56:38.895169	2025-11-10 21:56:38.895169
fdcf0022-2282-4ab9-ae17-ab4b8ee8b39d	admin	licenseiq-schema	t	2025-11-10 21:56:38.918007	2025-11-10 21:56:38.918007
d757b5e9-ae27-4736-9db1-18bf42e717d6	admin	data-management	t	2025-11-10 21:56:38.939964	2025-11-10 21:56:38.939964
99d024b5-3482-47d5-a713-b443bf4de3fb	admin	master-data	t	2025-11-10 21:56:38.962098	2025-11-10 21:56:38.962098
ebccbf37-b6be-443d-b8f3-f620b4e31ec8	admin	erp-import	t	2025-11-10 21:56:38.984122	2025-11-10 21:56:38.984122
e301fc3e-d318-4470-8549-33d4bd327cbf	admin	contract-qna	t	2025-11-10 21:56:39.007021	2025-11-10 21:56:39.007021
bb343020-5593-4ca0-8e79-c492b14506f4	admin	rag-dashboard	t	2025-11-10 21:56:39.029077	2025-11-10 21:56:39.029077
1257369b-a609-4e7b-b455-5cbcf20a5c49	admin	analytics	t	2025-11-10 21:56:39.051031	2025-11-10 21:56:39.051031
c59d4fdd-ddf0-4a75-b032-c4afe749140d	admin	reports	t	2025-11-10 21:56:39.07324	2025-11-10 21:56:39.07324
63d98774-75bd-40e2-8748-de2f24194c95	admin	lead-management	t	2025-11-10 21:56:39.095614	2025-11-10 21:56:39.095614
af9b651c-f0cf-4609-b9a0-ed085f72489e	admin	review-queue	t	2025-11-10 21:56:39.137125	2025-11-10 21:56:39.137125
e4ca7694-e122-44c0-8cfd-bca4dcc12ed7	admin	user-management	t	2025-11-10 21:56:39.160749	2025-11-10 21:56:39.160749
a5fbd751-df0e-4dca-8eeb-833b92c65d7d	admin	audit-trail	t	2025-11-10 21:56:39.183981	2025-11-10 21:56:39.183981
\.


--
-- Data for Name: roles; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.roles (id, role_name, display_name, description, is_system_role, is_active, created_at, updated_at) FROM stdin;
29628936-f431-4d65-9ca0-d723b90a7482	admin	Administrator	Full system access	t	t	2025-11-06 22:59:03.788598	2025-11-06 22:59:03.788598
b737629c-2425-458e-b6f3-5512888d9b03	owner	Owner	Business owner with full access	t	t	2025-11-06 22:59:03.788598	2025-11-06 22:59:03.788598
fdc7b228-e359-469f-8bf0-b9df8a234f7e	editor	Editor	Can edit contracts and data	t	t	2025-11-06 22:59:03.788598	2025-11-06 22:59:03.788598
40e87fe4-8304-465a-a899-49bdc248cc9f	viewer	Viewer	Read-only access	t	t	2025-11-06 22:59:03.788598	2025-11-06 22:59:03.788598
ed25e67c-96d0-43c3-9caf-0ea06383a3a4	auditor	Auditor	Access to audit trail and reports	t	t	2025-11-06 22:59:03.788598	2025-11-06 22:59:03.788598
\.


--
-- Data for Name: royalty_rules; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.royalty_rules (id, contract_id, rule_type, rule_name, description, product_categories, territories, container_sizes, seasonal_adjustments, territory_premiums, volume_tiers, base_rate, minimum_guarantee, calculation_formula, priority, is_active, confidence, source_section, source_text, created_at, updated_at, formula_definition, formula_version) FROM stdin;
f8dcdadc-c609-4a92-b423-9a420997c7a7	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	tiered	Automotive Components Royalty Rate Structure	Tiered royalty rate structure for automotive components	{"Automotive transmission components"}	{"United States",Canada,Mexico}	\N	\N	\N	[{"max": 5000000, "min": 0, "rate": 6.5}, {"max": 15000000, "min": 5000001, "rate": 5.8}, {"max": 50000000, "min": 15000001, "rate": 5.2}, {"min": 50000001, "rate": 4.8}]	6.50	\N	\N	1	t	1.00	3.1 Royalty Rate Structure	Licensee shall pay Licensor royalties based on Net Sales of Licensed Products according to the following tiered structure:	2025-11-12 15:32:46.309264	2025-11-12 15:32:46.309264	\N	\N
7977ea08-7312-4959-810c-cec408a86125	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	tiered	Industrial & Aerospace Components Royalty Rate Structure	Tiered royalty rate structure for industrial and aerospace components	{"Industrial machinery parts","Aerospace-grade composite materials"}	{"United States",Canada,Mexico}	\N	\N	\N	[{"max": 0, "min": 0, "rate": 7.2}, {"max": 0, "min": 0, "rate": 8.5}]	7.20	\N	\N	2	t	1.00	3.1 Royalty Rate Structure	Tier 2 - Industrial & Aerospace Components: Annual Net Sales Volume Royalty Rate Premium Multiplier	2025-11-12 15:32:46.332825	2025-11-12 15:32:46.332825	\N	\N
fe8c8703-81f1-4abb-83e9-84d0114f0735	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	minimum_guarantee	Minimum Annual Royalty Payments	Minimum annual royalty payments for licensed products	{General}	{"United States",Canada,Mexico}	\N	\N	\N	[]	0.00	125000.00	\N	3	t	1.00	3.1 Royalty Rate Structure	Licensee guarantees minimum annual royalty payments as specified in the rate structure, regardless of actual sales volume.	2025-11-12 15:32:46.356207	2025-11-12 15:32:46.356207	\N	\N
455897f9-c436-44e3-b9c9-6e588b33e94a	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	fixed_price	License Fee	One-time license fee for access to patent portfolio and initial technology transfer	{General}	{"United States",Canada,Mexico}	\N	\N	\N	[]	0.00	\N	\N	4	t	1.00	4.1 License Fee	Licensee shall pay a one-time, non-refundable license fee of $850,000 within 30 days of Agreement execution,	2025-11-12 15:32:46.379731	2025-11-12 15:32:46.379731	\N	\N
90511fbc-f5bc-4d1c-926c-59d5083b5f8a	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	fixed_price	Technology Transfer Fee	One-time technology transfer fee for comprehensive technology transfer	{General}	{"United States",Canada,Mexico}	\N	\N	\N	[]	0.00	\N	\N	5	t	1.00	4.2 Technology Transfer Fee	An additional $275,000 shall be paid for comprehensive technology transfer,	2025-11-12 15:32:46.40349	2025-11-12 15:32:46.40349	\N	\N
9cbad5c5-2a63-4a1b-a8bb-2478cc350345	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	milestone_payment	Milestone Payments	Milestone payments for achieving certain business milestones	{General}	{"United States",Canada,Mexico}	\N	\N	\N	[]	0.00	\N	\N	6	t	1.00	4.3 Milestone Payments	Milestone EventPayment AmountDue Date	2025-11-12 15:32:46.428028	2025-11-12 15:32:46.428028	\N	\N
df151760-f2ed-436c-93bf-81f679b2cac9	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	ARM-Compatible Processors	$3.25 per unit for M+ units/year	{"Smartphones and mobile communication devices"}	{US}	\N	\N	\N	[{"discountRate": 2.85, "volumeThreshold": 1000000}]	3.25	\N	\N	1	t	0.95	tiered	ARM-Compatible Processors: $3.25 per unit for M+ units/year	2025-11-12 15:34:02.776107	2025-11-12 15:34:02.776107	\N	\N
e5142239-0943-4cb3-8354-8fc6208fcf9b	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	Power Management ICs	$1.75 per unit for M+ units/year	{"Smartphones and mobile communication devices"}	{US}	\N	\N	\N	[{"discountRate": 1.45, "volumeThreshold": 1000000}]	1.75	\N	\N	1	t	0.95	tiered	Power Management ICs: $1.75 per unit for M+ units/year	2025-11-12 15:34:02.803265	2025-11-12 15:34:02.803265	\N	\N
0923f82b-3190-4202-8f9a-cffef2dbb19a	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	Memory Controllers	$2.15 per unit for 1.5M+ units/year	{"Smartphones and mobile communication devices"}	{US}	\N	\N	\N	[{"discountRate": 1.85, "volumeThreshold": 1500000}]	2.15	\N	\N	1	t	0.95	tiered	Memory Controllers: $2.15 per unit for 1.5M+ units/year	2025-11-12 15:34:02.82778	2025-11-12 15:34:02.82778	\N	\N
def12454-0d5d-4814-82a6-7c48f62dc619	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	Wireless Chipsets	$4.50 per unit for 50K+ units/year	{"Smartphones and mobile communication devices"}	{US}	\N	\N	\N	[{"discountRate": 3.95, "volumeThreshold": 50000}]	4.50	\N	\N	1	t	0.95	tiered	Wireless Chipsets: $4.50 per unit for 50K+ units/year	2025-11-12 15:34:02.85407	2025-11-12 15:34:02.85407	\N	\N
2d6cdffd-9b1c-4549-923f-f73d503a9650	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	AI Acceleration Units	$6.75 per unit for 25K+ units/year	{"Smartphones and mobile communication devices"}	{US}	\N	\N	\N	[{"discountRate": 5.95, "volumeThreshold": 25000}]	6.75	\N	\N	1	t	0.95	tiered	AI Acceleration Units: $6.75 per unit for 25K+ units/year	2025-11-12 15:34:02.877683	2025-11-12 15:34:02.877683	\N	\N
9ee898a9-e4fd-4618-a057-13d188d58287	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Tablets and E-Readers	2.5% of ASP	{"Tablets, laptops, and portable computing devices"}	{US}	\N	\N	\N	[]	2.50	\N	\N	1	t	0.95	percentage	Tablets and E-Readers: 2.5% of ASP	2025-11-12 15:34:02.901956	2025-11-12 15:34:02.901956	\N	\N
3cd8f31c-1573-4293-8ab5-1b18db1b7300	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Laptops and Ultrabooks	1.8% of ASP	{"Tablets, laptops, and portable computing devices"}	{US}	\N	\N	\N	[]	1.80	\N	\N	1	t	0.95	percentage	Laptops and Ultrabooks: 1.8% of ASP	2025-11-12 15:34:02.927565	2025-11-12 15:34:02.927565	\N	\N
0df4e5a2-f583-49de-86d5-2a7f5137fff5	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Smart Home Devices	3.2% of ASP	{"Smart home devices and IoT endpoints"}	{US}	\N	\N	\N	[]	3.20	\N	\N	1	t	0.95	percentage	Smart Home Devices: 3.2% of ASP	2025-11-12 15:34:02.954235	2025-11-12 15:34:02.954235	\N	\N
92ab508a-d05e-4cc5-bac3-2583ecf1396a	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Wearable Electronics	4.1% of ASP	{"Wearable electronics"}	{US}	\N	\N	\N	[]	4.10	\N	\N	1	t	0.95	percentage	Wearable Electronics: 4.1% of ASP	2025-11-12 15:34:02.977977	2025-11-12 15:34:02.977977	\N	\N
8bb64755-530b-4484-8cc6-81762c8b5266	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Automotive Electronics	2.875% of ASP	{"Automotive electronics and infotainment systems"}	{US}	\N	\N	\N	[]	2.88	\N	\N	1	t	0.95	percentage	Automotive Electronics: 2.875% of ASP	2025-11-12 15:34:03.001757	2025-11-12 15:34:03.001757	\N	\N
e28d2ce0-5e51-4cd8-8e78-e0b3eb91a107	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Medical Device Electronics	3.125% of ASP	{"Medical device electronics"}	{US}	\N	\N	\N	[]	3.13	\N	\N	1	t	0.95	percentage	Medical Device Electronics: 3.125% of ASP	2025-11-12 15:34:03.026724	2025-11-12 15:34:03.026724	\N	\N
e6d37cd8-fbbf-4840-9331-488684780989	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Industrial Automation	2.75% of ASP	{"Industrial automation and control systems"}	{US}	\N	\N	\N	[]	2.75	\N	\N	1	t	0.95	percentage	Industrial Automation: 2.75% of ASP	2025-11-12 15:34:03.051228	2025-11-12 15:34:03.051228	\N	\N
68a0784c-bf4c-4495-bc4b-94576c420510	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Server and Data Center	1.95% of ASP	{"Server and data center"}	{US}	\N	\N	\N	[]	1.95	\N	\N	1	t	0.95	percentage	Server and Data Center: 1.95% of ASP	2025-11-12 15:34:03.076675	2025-11-12 15:34:03.076675	\N	\N
29cce76e-313b-4240-b24a-b161923f2ae5	8f321b79-1ab4-4b02-8806-26db3a21a8ff	minimum_guarantee	Min Payment Year 3-4	Quarterly minimum	{All}	{US}	\N	\N	\N	\N	\N	1000000.00	\N	2	t	0.90	minimum_guarantee	Min Payment Year 3-4: Quarterly minimum	2025-11-12 15:34:03.126167	2025-11-12 15:34:03.126167	\N	\N
ab7e6194-316f-4abe-a819-541b991b9ba4	8f321b79-1ab4-4b02-8806-26db3a21a8ff	minimum_guarantee	Min Payment Year 5+	Quarterly minimum	{All}	{US}	\N	\N	\N	\N	\N	1625000.00	\N	2	t	0.90	minimum_guarantee	Min Payment Year 5+: Quarterly minimum	2025-11-12 15:34:03.15058	2025-11-12 15:34:03.15058	\N	\N
1a8ff104-2feb-4028-a89e-b78c508dc285	8f321b79-1ab4-4b02-8806-26db3a21a8ff	fixed_price	Initial License Fee	$1,250,000	{All}	{US}	\N	\N	\N	\N	\N	\N	\N	1	t	0.95	fixed_price	Initial License Fee: $1,250,000	2025-11-12 15:34:03.176685	2025-11-12 15:34:03.176685	\N	\N
0eff3d84-1612-4a18-9c90-058524c343e7	8f321b79-1ab4-4b02-8806-26db3a21a8ff	fixed_price	Technology Transfer Package	$850,000	{All}	{US}	\N	\N	\N	\N	\N	\N	\N	1	t	0.95	fixed_price	Technology Transfer Package: $850,000	2025-11-12 15:34:03.200847	2025-11-12 15:34:03.200847	\N	\N
d1196bfd-2fdd-4593-8603-77e615f5da2c	8f321b79-1ab4-4b02-8806-26db3a21a8ff	fixed_price	Annual Support and Updates	$185,000	{All}	{US}	\N	\N	\N	\N	\N	\N	\N	1	t	0.95	fixed_price	Annual Support and Updates: $185,000	2025-11-12 15:34:03.22533	2025-11-12 15:34:03.22533	\N	\N
6a1d5ea5-e4fe-4015-bd9e-ccb9fb478314	8f321b79-1ab4-4b02-8806-26db3a21a8ff	auto_renewal	License Term	Automatic renewal for successive three (3) year periods	{All}	{US}	\N	\N	\N	[]	\N	\N	\N	1	t	0.95	auto_renewal	License Term: Automatic renewal for successive three (3) year periods	2025-11-12 15:34:03.251031	2025-11-12 15:34:03.251031	\N	\N
540722f5-757e-4d79-809d-2c590912fe5d	8f321b79-1ab4-4b02-8806-26db3a21a8ff	escalation_clause	Geographic Royalty Adjustments	+10% premium on all royalty rates for Tier 2 Markets	{All}	{EU}	\N	\N	\N	[]	1.10	\N	\N	1	t	0.95	escalation_clause	Geographic Royalty Adjustments: +10% premium on all royalty rates for Tier 2 Markets	2025-11-12 15:34:03.276077	2025-11-12 15:34:03.276077	\N	\N
b96c525d-99a8-414d-b998-68b9dee9803e	8f321b79-1ab4-4b02-8806-26db3a21a8ff	escalation_clause	Geographic Royalty Adjustments	+20% premium on all royalty rates for Tier 3 Markets	{All}	{Japan}	\N	\N	\N	[]	1.20	\N	\N	1	t	0.95	escalation_clause	Geographic Royalty Adjustments: +20% premium on all royalty rates for Tier 3 Markets	2025-11-12 15:34:03.299689	2025-11-12 15:34:03.299689	\N	\N
2c05923e-167b-4590-b03e-af6eba7afe48	8f321b79-1ab4-4b02-8806-26db3a21a8ff	early_termination	Non-Renewal Notice	18 months written notice of non-renewal	{All}	{US}	\N	\N	\N	[]	\N	\N	\N	1	t	0.95	early_termination	Non-Renewal Notice: 18 months written notice of non-renewal	2025-11-12 15:34:03.323826	2025-11-12 15:34:03.323826	\N	\N
ca1e75a5-6589-461c-ba4f-55afb642fc14	8f321b79-1ab4-4b02-8806-26db3a21a8ff	license_scope	Licensed Product Categories	Rights granted apply to the following electronic device categories	{"Smartphones and mobile communication devices","Tablets, laptops, and portable computing devices","Smart home devices and IoT endpoints","Automotive electronics and infotainment systems","Industrial automation and control systems"}	{US}	\N	\N	\N	[]	\N	\N	\N	1	t	0.95	license_scope	Licensed Product Categories: Rights granted apply to the following electronic device categories	2025-11-12 15:34:03.347265	2025-11-12 15:34:03.347265	\N	\N
b9d484c7-dc4b-4789-b8ec-78255d052da5	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	Tablet and E-Reader Rate	2.5% of ASP, $3.50-$15.00 per unit	{"Tablets and E-Readers"}	{Global}	\N	\N	\N	[{"max": 1000000, "min": 0, "rate": 2.5}, {"max": 2000000, "min": 1000001, "rate": 2.5}, {"max": 3000000, "min": 2000001, "rate": 2.5}]	2.50	\N	\N	1	t	0.95	tiered	Tablet and E-Reader Rate: 2.5% of ASP, $3.50-$15.00 per unit	2025-11-12 15:34:03.370759	2025-11-12 15:34:03.370759	\N	\N
c56cb48e-20b7-4a72-9951-d6adf1f658cf	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	Laptop and Ultrabook Rate	1.8% of ASP, $8.00-$35.00 per unit	{"Laptops and Ultrabooks"}	{Global}	\N	\N	\N	[{"max": 1000000, "min": 0, "rate": 1.8}, {"max": 2000000, "min": 1000001, "rate": 1.8}, {"max": 3000000, "min": 2000001, "rate": 1.8}]	1.80	\N	\N	1	t	0.95	tiered	Laptop and Ultrabook Rate: 1.8% of ASP, $8.00-$35.00 per unit	2025-11-12 15:34:03.394383	2025-11-12 15:34:03.394383	\N	\N
324e11a7-44ab-4902-972e-26a706ed5ed4	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	Smart Home Device Rate	3.2% of ASP, $1.25-$8.50 per unit	{"Smart Home Devices"}	{Global}	\N	\N	\N	[{"max": 1000000, "min": 0, "rate": 3.2}, {"max": 2000000, "min": 1000001, "rate": 3.2}, {"max": 3000000, "min": 2000001, "rate": 3.2}]	3.20	\N	\N	1	t	0.95	tiered	Smart Home Device Rate: 3.2% of ASP, $1.25-$8.50 per unit	2025-11-12 15:34:03.419578	2025-11-12 15:34:03.419578	\N	\N
1920ff6d-2c3a-41cc-bb52-fdda435fd956	8f321b79-1ab4-4b02-8806-26db3a21a8ff	tiered	Wearable Electronics Rate	4.1% of ASP, $2.75-$12.00 per unit	{"Wearable Electronics"}	{Global}	\N	\N	\N	[{"max": 1000000, "min": 0, "rate": 4.1}, {"max": 2000000, "min": 1000001, "rate": 4.1}, {"max": 3000000, "min": 2000001, "rate": 4.1}]	4.10	\N	\N	1	t	0.95	tiered	Wearable Electronics Rate: 4.1% of ASP, $2.75-$12.00 per unit	2025-11-12 15:34:03.443905	2025-11-12 15:34:03.443905	\N	\N
1142d282-b830-4cf2-9694-31e0a57d5f3d	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Automotive Electronics Rate	2.875% of ASP	{"Automotive Electronics"}	{Global}	\N	\N	\N	[]	2.88	\N	\N	1	t	0.95	percentage	Automotive Electronics Rate: 2.875% of ASP	2025-11-12 15:34:03.467444	2025-11-12 15:34:03.467444	\N	\N
752055a0-64ed-44cf-afae-c097a26afda8	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Medical Device Electronics Rate	3.125% of ASP	{"Medical Device Electronics"}	{Global}	\N	\N	\N	[]	3.13	\N	\N	1	t	0.95	percentage	Medical Device Electronics Rate: 3.125% of ASP	2025-11-12 15:34:03.491537	2025-11-12 15:34:03.491537	\N	\N
3c0210c0-bf07-4eb3-aec2-a04c56e2314b	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Industrial Automation Rate	2.75% of ASP	{"Industrial Automation"}	{Global}	\N	\N	\N	[]	2.75	\N	\N	1	t	0.95	percentage	Industrial Automation Rate: 2.75% of ASP	2025-11-12 15:34:03.515242	2025-11-12 15:34:03.515242	\N	\N
165c021c-658e-4a6e-8241-b0b1707dbb33	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Server and Data Center Rate	1.95% of ASP	{"Server and Data Center"}	{Global}	\N	\N	\N	[]	1.95	\N	\N	1	t	0.95	percentage	Server and Data Center Rate: 1.95% of ASP	2025-11-12 15:34:03.538486	2025-11-12 15:34:03.538486	\N	\N
c275ccb1-a50d-4f41-abd0-88ee2b7e93d4	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Tier 2 Market Adjustment	+10% premium on all royalty rates	{}	{EU,UK,Australia}	\N	\N	\N	[]	0.10	\N	\N	1	t	0.95	percentage	Tier 2 Market Adjustment: +10% premium on all royalty rates	2025-11-12 15:34:03.562399	2025-11-12 15:34:03.562399	\N	\N
86a6f700-e259-4da0-bd32-0c4e44c35624	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Tier 3 Market Adjustment	+20% premium on all royalty rates	{}	{Japan,Korea,Taiwan}	\N	\N	\N	[]	0.20	\N	\N	1	t	0.95	percentage	Tier 3 Market Adjustment: +20% premium on all royalty rates	2025-11-12 15:34:03.584936	2025-11-12 15:34:03.584936	\N	\N
7bdb44ff-ea61-4b77-968d-b0be9fcf32a9	8f321b79-1ab4-4b02-8806-26db3a21a8ff	minimum_guarantee	Year 1-2 Min Guarantee	$2,500,000 quarterly minimum	{All}	{Global}	\N	\N	\N	\N	\N	2500000.00	\N	2	t	0.90	minimum_guarantee	Year 1-2 Min Guarantee: $2,500,000 quarterly minimum	2025-11-12 15:34:03.608419	2025-11-12 15:34:03.608419	\N	\N
8ee7f716-dadc-41bc-baf4-1d20addf4d1e	8f321b79-1ab4-4b02-8806-26db3a21a8ff	minimum_guarantee	Year 3-4 Min Guarantee	$4,000,000 quarterly minimum	{All}	{Global}	\N	\N	\N	\N	\N	4000000.00	\N	2	t	0.90	minimum_guarantee	Year 3-4 Min Guarantee: $4,000,000 quarterly minimum	2025-11-12 15:34:03.631627	2025-11-12 15:34:03.631627	\N	\N
87272a7e-0254-40d5-a2b1-f209256c4364	8f321b79-1ab4-4b02-8806-26db3a21a8ff	minimum_guarantee	Year 5+ Min Guarantee	$6,500,000 quarterly minimum	{All}	{Global}	\N	\N	\N	\N	\N	6500000.00	\N	2	t	0.90	minimum_guarantee	Year 5+ Min Guarantee: $6,500,000 quarterly minimum	2025-11-12 15:34:03.655348	2025-11-12 15:34:03.655348	\N	\N
f4c9403b-363c-4a8e-b78b-a6bea8dfef28	8f321b79-1ab4-4b02-8806-26db3a21a8ff	variable_price	Volume Milestone Bonus	$500,000-$2,500,000 bonus payments	{All}	{Global}	\N	\N	\N	[{"max": 25000000, "min": 5000000, "rate": 500000}, {"max": 100000000, "min": 25000001, "rate": 1250000}, {"max": null, "min": 100000001, "rate": 2500000}]	\N	\N	\N	1	t	0.95	variable_price	Volume Milestone Bonus: $500,000-$2,500,000 bonus payments	2025-11-12 15:34:03.679503	2025-11-12 15:34:03.679503	\N	\N
53abf081-be9e-44a0-97c3-1bf46086f24c	8f321b79-1ab4-4b02-8806-26db3a21a8ff	per_unit	Royalty Payment	Royalty payment due within 45 days of quarter end	{All}	{Global}	\N	\N	\N	[]	\N	\N	\N	1	t	0.95	per_unit	Royalty Payment: Royalty payment due within 45 days of quarter end	2025-11-12 15:34:03.703118	2025-11-12 15:34:03.703118	\N	\N
44352c0e-8e64-48e5-a347-f15ec7757542	8f321b79-1ab4-4b02-8806-26db3a21a8ff	per_time_period	Late Payment Penalty	1.75% per month plus collection costs	{All}	{Global}	\N	\N	\N	[]	0.02	\N	\N	1	t	0.95	per_time_period	Late Payment Penalty: 1.75% per month plus collection costs	2025-11-12 15:34:03.726742	2025-11-12 15:34:03.726742	\N	\N
2a58fb82-838e-4d55-b9df-38be9e163cb4	8f321b79-1ab4-4b02-8806-26db3a21a8ff	usage_based	Quality Metrics	Defect rate below 50 PPM, customer return rate below 0.1%	{All}	{Global}	\N	\N	\N	[]	\N	\N	\N	1	t	0.95	usage_based	Quality Metrics: Defect rate below 50 PPM, customer return rate below 0.1%	2025-11-12 15:34:03.750102	2025-11-12 15:34:03.750102	\N	\N
3cd16541-07e9-499c-8094-968826548ce2	8f321b79-1ab4-4b02-8806-26db3a21a8ff	auto_renewal	Annual Support and Updates	$185,000 annual fee	{All}	{Global}	\N	\N	\N	\N	\N	\N	\N	1	t	0.95	auto_renewal	Annual Support and Updates: $185,000 annual fee	2025-11-12 15:34:03.773117	2025-11-12 15:34:03.773117	\N	\N
62d06197-cc99-4d6e-b2cd-2ad557660363	8f321b79-1ab4-4b02-8806-26db3a21a8ff	escalation_clause	Royalty Rate Escalation	Royalty rate escalation based on cumulative sales achievements	{All}	{Global}	\N	\N	\N	[{"max": 25000000, "min": 5000000, "rate": 2.5}, {"max": 100000000, "min": 25000001, "rate": 1.8}, {"max": null, "min": 100000001, "rate": 3.2}]	\N	\N	\N	1	t	0.95	escalation_clause	Royalty Rate Escalation: Royalty rate escalation based on cumulative sales achievements	2025-11-12 15:34:03.796341	2025-11-12 15:34:03.796341	\N	\N
61304f08-d72d-411d-8d6e-84585df59439	8f321b79-1ab4-4b02-8806-26db3a21a8ff	early_termination	Termination Events	Material breach, insolvency, bankruptcy, patent invalidation, failure to meet minimum production commitments	{All}	{Global}	\N	\N	\N	[]	\N	\N	\N	1	t	0.95	early_termination	Termination Events: Material breach, insolvency, bankruptcy, patent invalidation, failure to meet minimum production commitments	2025-11-12 15:34:03.820048	2025-11-12 15:34:03.820048	\N	\N
fdc8c2a8-878e-4b39-902f-4d51ddcd3c64	8f321b79-1ab4-4b02-8806-26db3a21a8ff	license_scope	License Scope	License granted for use of licensed technologies in manufacturing and sales of Licensed Products	{All}	{Global}	\N	\N	\N	[]	\N	\N	\N	1	t	0.95	license_scope	License Scope: License granted for use of licensed technologies in manufacturing and sales of Licensed Products	2025-11-12 15:34:03.842937	2025-11-12 15:34:03.842937	\N	\N
07b88f95-658a-40b7-9550-c80e1281fbad	8f321b79-1ab4-4b02-8806-26db3a21a8ff	volume_discount	Volume Discount	Discounts based on cumulative sales achievements	{All}	{Global}	\N	\N	\N	[{"max": 25000000, "min": 5000000, "rate": 0.1}, {"max": 100000000, "min": 25000001, "rate": 0.05}, {"max": null, "min": 100000001, "rate": 0.01}]	\N	\N	\N	1	t	0.95	volume_discount	Volume Discount: Discounts based on cumulative sales achievements	2025-11-12 15:34:03.865926	2025-11-12 15:34:03.865926	\N	\N
e651ab79-1667-4d85-9f02-82318a1e113a	8f321b79-1ab4-4b02-8806-26db3a21a8ff	minimum_guarantee	Quarterly Min Guarantee	Minimum guarantee payments due quarterly in advance	{All}	{US}	\N	\N	\N	\N	\N	\N	\N	1	t	0.95	minimum_guarantee	Quarterly Min Guarantee: Minimum guarantee payments due quarterly in advance	2025-11-12 15:34:03.889105	2025-11-12 15:34:03.889105	\N	\N
522c5533-e327-44af-898e-029324088e18	8f321b79-1ab4-4b02-8806-26db3a21a8ff	fixed_price	Late Payment Penalty	1.75% per month plus collection costs	{All}	{US}	\N	\N	\N	[]	1.75	\N	\N	2	t	0.90	fixed_price	Late Payment Penalty: 1.75% per month plus collection costs	2025-11-12 15:34:03.911003	2025-11-12 15:34:03.911003	\N	\N
867220ab-1fcb-434a-b8e6-e792470ad349	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Foreign Exchange Costs	Borne by Licensee	{All}	{US}	\N	\N	\N	[]	0.00	\N	\N	3	t	0.80	percentage	Foreign Exchange Costs: Borne by Licensee	2025-11-12 15:34:03.933988	2025-11-12 15:34:03.933988	\N	\N
6191e2e4-118c-4acb-ac57-aec42f9d8d95	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Audit Costs	Licensee bears all audit costs plus interest on underpaid amounts	{All}	{US}	\N	\N	\N	[]	0.00	\N	\N	4	t	0.70	percentage	Audit Costs: Licensee bears all audit costs plus interest on underpaid amounts	2025-11-12 15:34:03.957648	2025-11-12 15:34:03.957648	\N	\N
0e18a6c7-7b4f-4f68-9228-db4edac80689	8f321b79-1ab4-4b02-8806-26db3a21a8ff	percentage	Cost Sharing for Joint Patents	60% Licensor / 40% Licensee	{All}	{US}	\N	\N	\N	[]	0.60	\N	\N	5	t	0.60	percentage	Cost Sharing for Joint Patents: 60% Licensor / 40% Licensee	2025-11-12 15:34:03.980134	2025-11-12 15:34:03.980134	\N	\N
aa9a184f-5055-49e1-9171-7b043fe33e30	8f321b79-1ab4-4b02-8806-26db3a21a8ff	minimum_guarantee	Min Payment	Quarterly minimum	{All}	{US}	{}	{}	{}	[]	\N	625000.00	\N	2	t	0.90	minimum_guarantee	Min Payment: Quarterly minimum	2025-11-12 15:34:03.102034	2025-11-12 15:34:03.102034	\N	\N
4695ff0e-e79a-4a11-9596-735bcc7f7359	46d0e351-4296-45bb-a680-9ee65497967b	tiered	Ornamental Trees & Shrubs Royalty Structure	Royalty rates for Ornamental Trees & Shrubs	{"Aurora Flame Maple","Golden Spire Juniper"}	{}	\N	\N	\N	[{"max": 5000, "min": 0, "rate": 1.25}, {"max": 10000, "min": 5000, "rate": 1.1}]	1.25	\N	\N	1	t	1.00	3.1 Plant Royalty Rates	Tier 1 - Ornamental Trees & Shrubs (Aurora Flame Maple, Golden Spire Juniper):	2025-11-12 15:36:37.968983	2025-11-12 15:36:37.968983	\N	\N
f1e3674e-25cd-48e8-9388-708fc502f499	46d0e351-4296-45bb-a680-9ee65497967b	tiered	Perennials & Roses Royalty Structure	Royalty rates for Perennials & Roses	{"Pacific Sunset Rose","Emerald Crown Hosta"}	{}	\N	\N	\N	[{"max": 5000, "min": 0, "rate": 0.75}, {"max": 10000, "min": 5000, "rate": 1}]	0.75	\N	\N	2	t	1.00	3.1 Plant Royalty Rates	Tier 2 - Perennials & Roses (Pacific Sunset Rose, Emerald Crown Hosta):	2025-11-12 15:36:37.99229	2025-11-12 15:36:37.99229	\N	\N
9f626cc1-a222-4952-a17b-6a8a2565d84b	46d0e351-4296-45bb-a680-9ee65497967b	tiered	Flowering Shrubs Royalty Structure	Royalty rates for Flowering Shrubs	{"Cascade Blue Hydrangea"}	{}	\N	\N	\N	[{"max": 2500, "min": 0, "rate": 2.25}, {"max": 7500, "min": 2501, "rate": 1.95}, {"max": 15000, "min": 7501, "rate": 1.7}, {"max": null, "min": 15001, "rate": 1.45}]	2.25	\N	\N	3	t	1.00	3.1 Plant Royalty Rates	Tier 3 - Flowering Shrubs (Cascade Blue Hydrangea):	2025-11-12 15:36:38.016011	2025-11-12 15:36:38.016011	\N	\N
0cb27199-aa85-4fd1-b530-5c8ff05e1550	46d0e351-4296-45bb-a680-9ee65497967b	percentage	Seasonal Royalty Adjustments	Seasonal adjustments to royalty rates	{General}	{}	\N	\N	\N	\N	0.10	\N	\N	4	t	1.00	3.2 Seasonal Royalty Adjustments	Spring Season (March-May): +10-15% premium for peak demand varieties	2025-11-12 15:36:38.039019	2025-11-12 15:36:38.039019	\N	\N
edfeda25-54a8-41c4-961c-eb2fae35d6a1	46d0e351-4296-45bb-a680-9ee65497967b	minimum_guarantee	Minimum Annual Royalty Guarantee	Minimum annual royalty guarantee	{General}	{}	\N	\N	\N	\N	0.00	85000.00	\N	5	t	1.00	3.3 Minimum Annual Guarantees	Licensee guarantees minimum annual royalty payments totaling $85,000	2025-11-12 15:36:38.062168	2025-11-12 15:36:38.062168	\N	\N
b54df357-aaa2-438c-a71b-67aa1d4775ce	46d0e351-4296-45bb-a680-9ee65497967b	fixed_price	Initial License Fee	One-time initial license fee	{General}	{}	\N	\N	\N	\N	0.00	\N	\N	6	t	1.00	4.1 Initial License Fee	Licensee shall pay a one-time, non-refundable license fee of $125,000	2025-11-12 15:36:38.116863	2025-11-12 15:36:38.116863	\N	\N
ca55a271-384f-4f76-9cdf-269a3696ab36	46d0e351-4296-45bb-a680-9ee65497967b	variable_price	Mother Stock and Starter Plants	Variable pricing for mother stock and starter plants	{"Aurora Flame Maple","Pacific Sunset Rose","Emerald Crown Hosta","Cascade Blue Hydrangea","Golden Spire Juniper"}	{}	\N	\N	\N	\N	0.00	\N	\N	7	t	1.00	4.2 Mother Stock and Starter Plants	Total Mother Stock Investment: $38,875 (payable upon delivery and certification)	2025-11-12 15:36:38.140039	2025-11-12 15:36:38.140039	\N	\N
f8bce056-2711-4735-8d7f-276659110fde	46d0e351-4296-45bb-a680-9ee65497967b	auto_renewal	License Term	Automatic renewal of license term	{General}	{}	\N	\N	\N	\N	0.00	\N	\N	8	t	1.00	2.3 License Term	Initial term of eight (8) years commencing February 12, 2024, with automatic renewal for successive four (4) year periods	2025-11-12 15:36:38.164005	2025-11-12 15:36:38.164005	\N	\N
14c614f4-e63f-4fc3-ad1f-9d60ac1ecc90	46d0e351-4296-45bb-a680-9ee65497967b	escalation_clause	Price Escalation	Price escalation clause	{General}	{}	\N	\N	\N	\N	0.00	\N	\N	9	t	1.00	3.2 Seasonal Royalty Adjustments	Holiday Seasons: +20% premium for gift-appropriate varieties during December	2025-11-12 15:36:38.187312	2025-11-12 15:36:38.187312	\N	\N
b2d83c2b-26fb-4e9c-963b-c028f8321157	46d0e351-4296-45bb-a680-9ee65497967b	early_termination	Termination Fee	Termination fee	{General}	{}	\N	\N	\N	\N	0.00	\N	\N	10	t	1.00	9.1 Termination Events	Either party may terminate upon 180 days written notice for material breach uncured after 90-day cure period	2025-11-12 15:36:38.210209	2025-11-12 15:36:38.210209	\N	\N
\.


--
-- Data for Name: rule_definitions; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.rule_definitions (id, contract_id, extraction_run_id, linked_graph_node_id, rule_type, rule_name, description, formula_definition, applicability_filters, confidence, validation_status, validation_errors, is_active, version, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: rule_node_definitions; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.rule_node_definitions (id, node_type, display_name, description, schema, evaluation_adapter, examples, created_at) FROM stdin;
\.


--
-- Data for Name: rule_validation_events; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.rule_validation_events (id, rule_definition_id, validation_type, validation_result, issues, recommendations, validator_id, validated_at) FROM stdin;
\.


--
-- Data for Name: sales_data; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.sales_data (id, matched_contract_id, match_confidence, transaction_date, transaction_id, product_code, product_name, category, territory, currency, gross_amount, net_amount, quantity, unit_price, custom_fields, import_job_id, created_at) FROM stdin;
da235295-2bf0-450c-8c43-9d9db0630289	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive Components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 21:12:18.672139
d7faa801-f3e2-4d01-92a5-133e9ada693f	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 21:12:18.704214
050d2260-7d16-42c3-800d-352613e1b372	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive Components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 21:12:18.736694
463e1680-ed9d-469f-8a20-1036bc83f62c	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive Components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 21:12:18.770122
eb394944-bbf1-49c1-be5b-3fba4092a3a6	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial Components	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 21:12:18.80367
dac62906-3dc4-46dd-9041-62d40398b851	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 21:12:18.836916
35c50a08-8919-4320-ae9c-4f1b52c8702e	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive Components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 21:12:18.86948
8df27acb-4e4c-4034-98c5-e63cf0cb443b	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 21:12:18.902209
9fd4d780-cf38-462d-bc0d-ac60e1f6eaf3	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial Components	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 21:12:18.934857
a4961f40-cf27-45ae-8ea5-568ac4b86641	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive Components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 21:12:18.967237
35295ebf-3634-4576-917e-7c514237d98c	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial Components	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 21:12:18.999925
162be917-c47d-4c14-ac9c-4b99081c0ca8	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 21:12:19.032405
fd996e41-c4f8-41ee-a271-5ee6852ce7ca	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive Components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 21:12:19.065484
d7acd835-27a3-45a9-9290-c66e278a33b6	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 21:12:19.098195
f494a3ee-2c9b-4465-8298-b06e1f406d4b	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive Components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 21:12:19.130681
3c3570ea-1188-4a63-a325-e3fc0a58c266	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial Components	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 21:12:19.162292
b0e83cc4-7bee-4bbb-a5d1-51993d09dfa2	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial Components	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 21:12:19.19519
9bad7808-7706-409b-ae4a-6c2e0adfe558	\N	\N	2024-03-15 00:00:00	TXN-2024-001	MAPLE-001	Aurora Flame Maple	Ornamental Trees	Primary	USD	30000.00	27000.00	6200.0000	4.84	\N	\N	2025-11-11 21:44:00.677923
3e31cd0c-5d89-4910-b0ad-12a4d3c28b19	\N	\N	2024-03-20 00:00:00	TXN-2024-002	MAPLE-002	Aurora Flame Maple	Ornamental Trees	Primary	USD	25000.00	22500.00	1100.0000	22.73	\N	\N	2025-11-11 21:44:00.713249
b370987d-ac3b-4ff0-8367-4f02f606e971	\N	\N	2024-10-05 00:00:00	TXN-2024-003	JUNIPER-001	Golden Spire Juniper	Ornamental Shrubs	Secondary	USD	28000.00	25200.00	1800.0000	15.56	\N	\N	2025-11-11 21:44:00.748319
300f2423-558c-4557-a149-afdeac1f4dcf	\N	\N	2024-04-12 00:00:00	TXN-2024-004	ROSE-001	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	12000.00	10800.00	3000.0000	4.00	\N	\N	2025-11-11 21:44:00.78377
4ef451d3-1f54-48a8-a381-bc6b2bd819eb	\N	\N	2024-09-18 00:00:00	TXN-2024-005	HOSTA-001	Emerald Crown Hosta	Perennials	Primary	USD	18000.00	16200.00	900.0000	20.00	\N	\N	2025-11-11 21:44:00.817938
a82f93fb-d852-4eab-84bb-dc59de649fc3	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive Components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 21:22:29.452746
2125fa60-ec52-414b-917c-1847ec67657b	\N	\N	2024-08-25 00:00:00	ELC-2024-016	\N	Premium Tablet T10	Tablets and E-Readers	Japan	USD	16800000.00	0.00	35000.0000	480.00	\N	\N	2025-11-12 15:24:37.877675
8315d29e-5f3b-4a7c-853c-ac6809b40c21	\N	\N	2024-09-05 00:00:00	ELC-2024-017	\N	Smart Home Hub SH200	Smart Home Devices	European Union	USD	12825000.00	0.00	95000.0000	135.00	\N	\N	2025-11-12 15:24:37.902851
a5e3b45e-0966-4a4d-a259-a4e13fb85f52	\N	\N	2024-09-18 00:00:00	ELC-2024-018	\N	Fitness Tracker FT500	Wearable Electronics	South Korea	USD	26600000.00	0.00	140000.0000	190.00	\N	\N	2025-11-12 15:24:37.927638
f6059a1b-0900-4e57-ab57-685769223647	\N	\N	2024-09-28 00:00:00	ELC-2024-019	\N	Auto Infotainment AI800	Automotive Electronics	European Union	USD	17595000.00	0.00	18000.0000	977.50	\N	\N	2025-11-12 15:24:37.950905
b58f35fc-30e7-4325-80a7-44ff328e4ca7	\N	\N	2024-10-10 00:00:00	ELC-2024-020	\N	Medical Monitor MM300	Medical Device Electronics	Japan	USD	18000000.00	0.00	12000.0000	1500.00	\N	\N	2025-11-12 15:24:37.975373
c891769b-e001-4628-aa9b-a15f4ab7ad99	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-12 15:33:03.197638
d9b447d4-b224-4a3c-b49f-d1ee8ac048c4	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-12 15:33:03.224332
81b906c3-5f6f-4fa7-9734-17e35e166269	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-12 15:33:03.251078
2a1e6a68-7db7-4c43-8674-c3e81f18a40d	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive transmission components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-12 15:33:03.278296
d32d5461-303d-48d6-91e0-fce1a4776082	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-12 15:33:03.304616
49919606-412a-4782-a94e-3bca32616e06	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive Components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 21:19:36.383648
e0135713-79aa-4c99-b3ca-8d696431f7b2	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 21:19:36.425289
50241f54-f98a-401f-bacd-ba4a2c9788cb	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive Components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 21:19:36.459194
7205feaf-0816-4ad8-a59e-871330c617a4	\N	\N	2024-03-15 00:00:00	ELC-2024-001	\N	ARM Processor A72	ARM-Compatible Processors	United States	USD	3900000.00	0.00	1200000.0000	3.25	\N	\N	2025-11-11 20:55:51.389847
c65f13f9-ef7a-4edc-9f83-d706a3e8ab16	\N	\N	2024-03-20 00:00:00	ELC-2024-002	\N	Power Management IC PM500	Power Management ICs	United States	USD	4375000.00	0.00	2500000.0000	1.75	\N	\N	2025-11-11 20:55:51.430204
c05b3379-9046-4816-b6c6-e68db82634d8	\N	\N	2024-04-05 00:00:00	ELC-2024-003	\N	Memory Controller MC850	Memory Controllers	United States	USD	3870000.00	0.00	1800000.0000	2.15	\N	\N	2025-11-11 20:55:51.464241
2f386970-f04c-424f-8973-c8d1083d7c28	\N	\N	2024-04-12 00:00:00	ELC-2024-004	\N	WiFi Chipset WF6E	Wireless Chipsets	United States	USD	2700000.00	0.00	600000.0000	4.50	\N	\N	2025-11-11 20:55:51.4974
cf015a33-0cd0-4c31-ac24-120a5ccc93ed	\N	\N	2024-04-18 00:00:00	ELC-2024-005	\N	AI Accelerator APU-X1	AI Acceleration Units	United States	USD	2025000.00	0.00	300000.0000	6.75	\N	\N	2025-11-11 20:55:51.530448
83bfaa49-16fd-42c4-8fd5-92b35d450c43	\N	\N	2024-05-02 00:00:00	ELC-2024-006	\N	Premium Tablet T10	Tablets and E-Readers	European Union	USD	22500000.00	0.00	50000.0000	450.00	\N	\N	2025-11-11 20:55:51.563498
cd5696e5-0b68-4281-a4ad-6c6ae4602d8c	\N	\N	2024-05-10 00:00:00	ELC-2024-007	\N	Ultrabook Pro 15	Laptops and Ultrabooks	United States	USD	36000000.00	0.00	30000.0000	1200.00	\N	\N	2025-11-11 20:55:51.596416
99227424-714e-485f-9d55-c4a1ff42ffb8	\N	\N	2024-05-22 00:00:00	ELC-2024-008	\N	Smart Home Hub SH200	Smart Home Devices	United States	USD	10000000.00	0.00	80000.0000	125.00	\N	\N	2025-11-11 20:55:51.627986
16e16164-89ee-4919-a6d4-92678198ae95	\N	\N	2024-06-01 00:00:00	ELC-2024-009	\N	Fitness Tracker FT500	Wearable Electronics	United States	USD	21600000.00	0.00	120000.0000	180.00	\N	\N	2025-11-11 20:55:51.661091
b04c282b-087c-4e78-90b6-6ce838706ac8	\N	\N	2024-06-15 00:00:00	ELC-2024-010	\N	Auto Infotainment AI800	Automotive Electronics	United States	USD	21250000.00	0.00	25000.0000	850.00	\N	\N	2025-11-11 20:55:51.694224
6084cb97-4c1b-4b90-a69a-62c5fabdb221	\N	\N	2024-06-28 00:00:00	ELC-2024-011	\N	Medical Monitor MM300	Medical Device Electronics	United States	USD	18000000.00	0.00	15000.0000	1200.00	\N	\N	2025-11-11 20:55:51.727239
eeeebb83-97ea-4d6c-ad93-751783718d7a	\N	\N	2024-07-05 00:00:00	ELC-2024-012	\N	Industrial Controller IC750	Industrial Automation	United States	USD	22000000.00	0.00	40000.0000	550.00	\N	\N	2025-11-11 20:55:51.760467
7260f6ad-74d6-458c-8de7-60044b5236b5	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive Components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 21:19:36.493873
ae8eecbf-4423-40db-92b1-f4ed0743b614	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial Components	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 21:19:36.530811
ca80c492-914c-472e-b0ad-a951289a4f91	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 21:22:30.104574
fc881b3a-455c-4899-b868-18db7ff31cde	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-12 15:33:03.332171
4c5cce3d-b15b-4648-89df-be7f42525a38	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-12 15:33:03.358578
7b163ffe-0bc9-43c5-a05b-e6b8880d4ba3	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-12 15:33:03.383845
0485f463-ea2d-4435-b27d-bfe33de06c39	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial machinery parts	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-12 15:33:03.41487
bd13d3c5-9189-46de-81b1-592d599e5617	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 21:26:33.191743
66c4a18a-5824-4075-aa58-4dd6b1659e1a	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 21:19:36.564097
091c0491-ffa0-4a01-8a7c-844606f6f447	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive Components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 21:19:36.597868
e42cd3a4-da77-48f1-8d96-9b53d07a9c88	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 21:19:36.631744
02eb76ae-a360-4638-971d-1da3816dd29e	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial Components	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 21:19:36.664328
f61dba0d-4da8-4d57-bf92-b01c61b7979b	\N	\N	2024-07-18 00:00:00	ELC-2024-013	\N	Server Processor SP2000	Server and Data Center	United States	USD	20000000.00	0.00	8000.0000	2500.00	\N	\N	2025-11-11 20:55:51.793629
c2582483-71fd-44d2-8044-26f670c8eef7	\N	\N	2024-08-01 00:00:00	ELC-2024-014	\N	ARM Processor A72	ARM-Compatible Processors	Japan	USD	2422500.00	0.00	850000.0000	2.85	\N	\N	2025-11-11 20:55:51.826903
1cd3262e-ccb0-4298-b079-340ad1f604b0	\N	\N	2024-08-12 00:00:00	ELC-2024-015	\N	Power Management IC PM500	Power Management ICs	European Union	USD	2610000.00	0.00	1800000.0000	1.45	\N	\N	2025-11-11 20:55:51.858753
c5bf226c-2784-4f95-8778-ea29ac2b432a	\N	\N	2024-08-25 00:00:00	ELC-2024-016	\N	Premium Tablet T10	Tablets and E-Readers	Japan	USD	16800000.00	0.00	35000.0000	480.00	\N	\N	2025-11-11 20:55:51.89191
a877a512-00a1-4d1c-ab1e-6096b7060b14	\N	\N	2024-09-05 00:00:00	ELC-2024-017	\N	Smart Home Hub SH200	Smart Home Devices	European Union	USD	12825000.00	0.00	95000.0000	135.00	\N	\N	2025-11-11 20:55:51.924737
aba304c3-2bcf-42d3-8966-08ac4469b3d9	\N	\N	2024-09-18 00:00:00	ELC-2024-018	\N	Fitness Tracker FT500	Wearable Electronics	South Korea	USD	26600000.00	0.00	140000.0000	190.00	\N	\N	2025-11-11 20:55:51.958095
69bf081d-40bb-4e0b-827f-7b6542be09c9	\N	\N	2024-09-28 00:00:00	ELC-2024-019	\N	Auto Infotainment AI800	Automotive Electronics	European Union	USD	17595000.00	0.00	18000.0000	977.50	\N	\N	2025-11-11 20:55:51.991373
ffa9d30c-2e1a-4deb-9824-30708a5fb3ed	\N	\N	2024-10-10 00:00:00	ELC-2024-020	\N	Medical Monitor MM300	Medical Device Electronics	Japan	USD	18000000.00	0.00	12000.0000	1500.00	\N	\N	2025-11-11 20:55:52.024366
7e84d3a9-8407-4a77-870d-06fc6fa78255	\N	\N	2024-03-15 00:00:00	ELC-2024-001	\N	ARM Processor A72	ARM-Compatible Processors	United States	USD	3900000.00	0.00	1200000.0000	3.25	\N	\N	2025-11-11 20:56:26.626586
aaa08a30-322e-417e-8fbb-b774459e218d	\N	\N	2024-03-20 00:00:00	ELC-2024-002	\N	Power Management IC PM500	Power Management ICs	United States	USD	4375000.00	0.00	2500000.0000	1.75	\N	\N	2025-11-11 20:56:26.66205
e8f21dc3-c434-4b0e-8845-9642b5286dc0	\N	\N	2024-04-05 00:00:00	ELC-2024-003	\N	Memory Controller MC850	Memory Controllers	United States	USD	3870000.00	0.00	1800000.0000	2.15	\N	\N	2025-11-11 20:56:26.695493
1df8095a-770e-4242-b86a-62ffa91347ed	\N	\N	2024-04-12 00:00:00	ELC-2024-004	\N	WiFi Chipset WF6E	Wireless Chipsets	United States	USD	2700000.00	0.00	600000.0000	4.50	\N	\N	2025-11-11 20:56:26.728792
9c6b0d46-7bd5-4abe-bd4a-8fe7507f6c06	\N	\N	2024-04-18 00:00:00	ELC-2024-005	\N	AI Accelerator APU-X1	AI Acceleration Units	United States	USD	2025000.00	0.00	300000.0000	6.75	\N	\N	2025-11-11 20:56:26.762311
004531c3-8246-49c9-9068-1f2197242683	\N	\N	2024-05-02 00:00:00	ELC-2024-006	\N	Premium Tablet T10	Tablets and E-Readers	European Union	USD	22500000.00	0.00	50000.0000	450.00	\N	\N	2025-11-11 20:56:26.795806
96c24c2c-4c42-44d3-b516-c528d25a2708	\N	\N	2024-05-10 00:00:00	ELC-2024-007	\N	Ultrabook Pro 15	Laptops and Ultrabooks	United States	USD	36000000.00	0.00	30000.0000	1200.00	\N	\N	2025-11-11 20:56:26.83305
12b4bc5f-3255-4eb1-a36b-d2bd91ebc412	\N	\N	2024-05-22 00:00:00	ELC-2024-008	\N	Smart Home Hub SH200	Smart Home Devices	United States	USD	10000000.00	0.00	80000.0000	125.00	\N	\N	2025-11-11 20:56:26.866312
e1b4f417-0709-44ec-8b7f-3564ced907a0	\N	\N	2024-06-01 00:00:00	ELC-2024-009	\N	Fitness Tracker FT500	Wearable Electronics	United States	USD	21600000.00	0.00	120000.0000	180.00	\N	\N	2025-11-11 20:56:26.899577
fb0a1056-462b-4d0b-b487-c6600bbb2e9e	\N	\N	2024-06-15 00:00:00	ELC-2024-010	\N	Auto Infotainment AI800	Automotive Electronics	United States	USD	21250000.00	0.00	25000.0000	850.00	\N	\N	2025-11-11 20:56:26.933305
d544a7a8-2e5b-4eea-ad0e-dad0f854ccac	\N	\N	2024-06-28 00:00:00	ELC-2024-011	\N	Medical Monitor MM300	Medical Device Electronics	United States	USD	18000000.00	0.00	15000.0000	1200.00	\N	\N	2025-11-11 20:56:26.966722
eb2a475c-7d83-422f-88b3-15bd5cc2d15d	\N	\N	2024-07-05 00:00:00	ELC-2024-012	\N	Industrial Controller IC750	Industrial Automation	United States	USD	22000000.00	0.00	40000.0000	550.00	\N	\N	2025-11-11 20:56:27.000892
d18b9bed-6d57-4802-b121-24d2f3f9e109	\N	\N	2024-07-18 00:00:00	ELC-2024-013	\N	Server Processor SP2000	Server and Data Center	United States	USD	20000000.00	0.00	8000.0000	2500.00	\N	\N	2025-11-11 20:56:27.034207
d1792283-c9ca-4b4d-9815-1005810a1c4f	\N	\N	2024-08-01 00:00:00	ELC-2024-014	\N	ARM Processor A72	ARM-Compatible Processors	Japan	USD	2422500.00	0.00	850000.0000	2.85	\N	\N	2025-11-11 20:56:27.067576
b45d816a-6f06-44c9-9229-b6bb1e3d6573	\N	\N	2024-08-12 00:00:00	ELC-2024-015	\N	Power Management IC PM500	Power Management ICs	European Union	USD	2610000.00	0.00	1800000.0000	1.45	\N	\N	2025-11-11 20:56:27.101782
52860934-519a-45d8-8c06-71101b661ea6	\N	\N	2024-08-25 00:00:00	ELC-2024-016	\N	Premium Tablet T10	Tablets and E-Readers	Japan	USD	16800000.00	0.00	35000.0000	480.00	\N	\N	2025-11-11 20:56:27.135444
96fc740a-6144-475a-a9f8-40c1ebf1bb42	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive Components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 21:19:36.697538
eb139d0d-3b13-4b7f-bae5-6c1280fbf535	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial Components	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 21:19:36.730836
3c6a5e89-4931-4592-aa07-ec1073a76e49	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 21:19:36.763393
43447b79-fe23-4cc9-bb7e-792498f29914	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive Components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 21:19:36.797901
20e431b5-ec22-45a8-b271-76883aa59772	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 21:19:36.831281
adcda904-1493-4fbd-a64e-381d6e4038dc	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive Components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 21:19:36.864334
a7d89a2c-a93d-46db-b5e1-74d8a792b245	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial Components	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 21:19:36.897905
5ab500cc-9688-4d01-ae8c-54874e7cde72	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial Components	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 21:19:36.93104
c656af5b-84fe-4014-968a-dcefad8ca06a	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 21:19:36.964866
7b5dc47a-19a0-4ddd-8d19-30495e315752	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive Components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 21:19:36.997208
7fcf88d9-7b31-4a18-8545-5e9580f29a4e	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 21:19:37.030361
631ab7f4-d47d-4d20-ad63-4d2060d85dc0	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 21:22:29.487184
d95897dc-2315-4f36-9a3e-133cbba8e3a7	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive Components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 21:22:29.520047
c35eaba2-3a36-4c01-ae2f-2e238912d7f0	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive Components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 21:22:29.554385
deb02f26-c9d7-4b4f-92e3-38dd98cfd016	\N	\N	2024-09-05 00:00:00	ELC-2024-017	\N	Smart Home Hub SH200	Smart Home Devices	European Union	USD	12825000.00	0.00	95000.0000	135.00	\N	\N	2025-11-11 20:56:27.168962
da385541-2b8e-4497-be4a-3492017fa4ed	\N	\N	2024-09-18 00:00:00	ELC-2024-018	\N	Fitness Tracker FT500	Wearable Electronics	South Korea	USD	26600000.00	0.00	140000.0000	190.00	\N	\N	2025-11-11 20:56:27.201203
4117d108-1070-40c8-8c39-73432481e170	\N	\N	2024-09-28 00:00:00	ELC-2024-019	\N	Auto Infotainment AI800	Automotive Electronics	European Union	USD	17595000.00	0.00	18000.0000	977.50	\N	\N	2025-11-11 20:56:27.266024
0906d608-5df9-4faf-ae96-0c5e0ec2409c	\N	\N	2024-10-10 00:00:00	ELC-2024-020	\N	Medical Monitor MM300	Medical Device Electronics	Japan	USD	18000000.00	0.00	12000.0000	1500.00	\N	\N	2025-11-11 20:56:27.299727
bfc6af75-3365-41c3-a5c9-1a0bc82f5240	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 21:12:19.227724
f7a9c007-f127-4fb2-9d4f-2002742efb93	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive Components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 21:12:19.27722
738bf642-acf9-4a3b-b603-16fbad221636	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 21:12:19.310171
12981e62-6eae-47b5-bac3-3a51268c3d88	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial Components	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 21:22:29.588595
10c0f502-1cc8-4a59-b81c-168b6db69eb3	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 21:22:29.622148
e9f16514-1151-4c2a-8fc6-1a3633a8facc	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive Components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 21:22:29.657362
eaf8bcc2-9210-4678-9305-bab46106f2df	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 21:22:29.691262
e574c451-da79-4b0d-8640-904acf3bca2a	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial Components	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 21:22:29.725008
200cc6f2-85b8-4699-9958-7db75732e52d	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive Components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 21:22:29.759045
b31dea71-31d6-4eef-a894-e34075757c55	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial Components	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 21:22:29.794137
cd7447ca-3f6b-40ba-bc1d-a0fea746e8ea	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 21:22:29.828091
aee3a4c9-73df-4df4-9767-112c788b887a	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive Components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 21:22:29.862229
ade126f1-37ab-4e22-aa6c-6686f0e833df	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive Components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 21:22:29.900625
bb45ff3f-bc13-42b1-a7d5-1261e9871927	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive Components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 21:22:29.934202
f6ab0e31-24d5-4ba0-8e95-24c1de604ea1	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial Components	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 21:22:29.968504
568fea24-373e-4f55-b2f5-c364034473ca	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial Components	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 21:22:30.002278
7aee6d38-6aa2-42c5-aa8c-f61f6d435357	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace Components	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 21:22:30.03628
94772e4b-58ab-4ce5-9f55-f53eb7354f78	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive Components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 21:22:30.070647
907f94f8-a8ba-49fe-a773-22498d15c964	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 21:45:23.411413
b140921f-52a1-4a94-beef-290fe415e252	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive transmission components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 21:45:23.443641
3ab84b00-1d3c-46e4-a590-2c945f5ddd8e	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 21:45:23.475732
dec268be-86c7-4021-ad1b-2717a709c952	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 22:03:52.487433
09c8b316-58e8-42a1-874a-af068ea969fd	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 22:03:52.52164
98da60b8-9d4e-48b0-bd23-76aeb8aeadcd	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial machinery parts	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 22:03:52.55555
c885f150-1645-477e-91b6-4b35deada6f9	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 22:03:52.589455
191a63b5-b8d3-4530-8761-bf0506216e99	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 22:03:52.623341
be2718ed-748c-42d2-a719-1a4c29ff1534	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive transmission components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 22:03:52.662338
e74fe070-978d-4fb0-92fb-9df82eeaf515	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 22:03:52.696849
76f44466-a795-4e54-b792-f2d943d09c71	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 22:09:17.140906
b5922935-81d1-4677-8d19-8be71f1eb0b4	\N	\N	2024-03-25 00:00:00	TXN-2024-006	HYDRANGEA-001	Cascade Blue Hydrangea	Flowering Shrubs	Primary	USD	120000.00	108000.00	20000.0000	6.00	\N	\N	2025-11-11 21:44:00.852861
22d4f25e-04b6-43b4-9784-54c64e757862	\N	\N	2024-12-15 00:00:00	TXN-2024-007	ROSE-002	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	5000.00	4500.00	250.0000	20.00	\N	\N	2025-11-11 21:44:00.887674
6e6a06f8-8633-4f1f-bf1a-0bd7222266e2	\N	\N	2024-05-10 00:00:00	TXN-2024-008	MAPLE-003	Aurora Flame Maple	Ornamental Trees	Secondary	USD	15000.00	13500.00	3000.0000	5.00	\N	\N	2025-11-11 21:44:00.922637
aa2e294b-ab74-4a1a-a09c-ba91891c9a47	\N	\N	2024-06-15 00:00:00	TXN-2024-009	JUNIPER-002	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	22000.00	19800.00	800.0000	27.50	\N	\N	2025-11-11 21:44:00.957362
a2c21a77-635b-4bba-8eef-4a7f48ef0f7e	\N	\N	2024-07-20 00:00:00	TXN-2024-010	HYDRANGEA-002	Cascade Blue Hydrangea	Flowering Shrubs	Secondary	USD	18000.00	16200.00	1200.0000	15.00	\N	\N	2025-11-11 21:44:00.993687
f9641213-842e-4cea-a30a-d82a0ebda15c	\N	\N	2024-08-10 00:00:00	TXN-2024-011	HOSTA-002	Emerald Crown Hosta	Perennials	Primary	USD	12000.00	10800.00	1500.0000	8.00	\N	\N	2025-11-11 21:44:01.028639
85feb4bf-f5bb-4dff-a277-9333dd12f917	\N	\N	2024-09-05 00:00:00	TXN-2024-012	ROSE-003	Pacific Sunset Rose	Flowering Shrubs	Secondary	USD	16000.00	14400.00	1000.0000	16.00	\N	\N	2025-11-11 21:44:01.064254
e30e1f17-71fb-4f55-b53f-c26bc02677de	\N	\N	2024-10-15 00:00:00	TXN-2024-013	MAPLE-004	Aurora Flame Maple	Ornamental Trees	Primary	USD	35000.00	31500.00	2000.0000	17.50	\N	\N	2025-11-11 21:44:01.100229
297800a9-6f47-4d8d-ba6d-833cbe2c2d50	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 21:26:33.226912
0ee50423-aa5e-4475-8b8c-a4244f75380a	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 21:26:33.261941
5a6756ec-6c1d-4f81-8ee4-bca89f438202	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive transmission components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 21:26:33.297491
ead9c112-191e-48f3-bce9-f34e8a7d48d7	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 21:26:33.333304
9998b59b-e963-42ed-b12b-5b34f0054ade	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 21:26:33.368252
379a951e-9f05-4137-83b3-fa59888565b1	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 21:26:33.403149
11cd3601-e72b-4c14-b0dc-3afcf676f097	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 21:26:33.437971
74631c6b-15f7-4e6c-8df7-d39fde67cec7	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial machinery parts	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 21:26:33.473561
4c71ca80-c888-4aeb-a8a7-b9cb08b1fe44	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive transmission components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 21:26:33.508622
55a9a6bd-94d8-45b4-b01b-d1e4c4514766	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial machinery parts	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 21:26:33.543348
9400237e-22ad-4d81-a178-8e910993776c	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 21:26:33.578272
27f51399-26cd-44da-a653-83add81b51fb	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 21:26:33.613144
00d00b77-578a-4b90-8b46-7119a684608b	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 21:26:33.647935
a1b8ec4a-afd0-4779-9826-7fceaf5a0f78	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 21:26:33.683024
5c1c64ff-3419-47fb-a63b-5b2e60f1a768	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial machinery parts	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 21:26:33.720393
82f941fd-9ab3-4146-a6f0-6c69711cfaaa	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 21:26:33.75535
d7e137a4-f142-47dd-8370-97f5e954ebaa	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 21:26:33.790537
87290bb0-a1d8-4149-b56c-b36447503913	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive transmission components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 21:26:33.825443
163640d8-8705-49a3-b110-97f7bb5e471f	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 21:26:33.861498
4215d868-4f11-4ef5-81d2-9e39657b7a18	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 21:36:56.872202
b93351cd-24aa-435a-b7c9-8a76dd0b5f7e	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 21:36:56.913705
b62f1a88-7de7-46f7-aaaf-db51ed017485	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 21:36:56.946402
48faf4cd-4e0a-4e23-845b-b5fd1f94190c	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive transmission components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 21:36:56.979528
df860360-0f4d-4b9d-8cac-2348b4d88bcf	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 21:36:57.011189
a50fdb1a-2269-47d7-bb1b-c45bc47b99ea	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 21:36:57.044145
86a21b53-c2a8-4f33-a020-aa7945c60c52	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 21:36:57.076952
37f4901f-2538-4ffc-be03-8884d82ad953	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 21:36:57.109695
be517984-78c5-4e59-8a1d-ccf9904ed38c	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial machinery parts	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 21:36:57.142946
d8cd7d0e-26bc-4c24-a68c-8f18692c9a78	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive transmission components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 21:36:57.176091
6d2793ae-89ee-4b35-854f-d12c478d7e04	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial machinery parts	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 21:36:57.208694
dd77b4c7-9d2e-41de-a5bd-0523b432f27b	\N	\N	2024-11-20 00:00:00	TXN-2024-014	JUNIPER-003	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	8000.00	7200.00	1600.0000	5.00	\N	\N	2025-11-11 21:44:01.135181
bd31a99e-479c-4699-ad0c-68d19718e8b7	\N	\N	2024-12-10 00:00:00	TXN-2024-015	HOSTA-003	Emerald Crown Hosta	Perennials	Secondary	USD	20000.00	18000.00	750.0000	26.67	\N	\N	2025-11-11 21:44:01.170448
c65232ff-651d-4fa6-ae9c-521eb8271c33	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 21:36:57.241425
b1ce070f-0ef2-4ba1-9249-2d9456fdba34	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 21:36:57.273919
42b3036e-a9ae-4673-8023-bfad45f07944	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 21:36:57.306396
69b98ab7-65ce-4101-a74e-d40a8cac145b	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 21:36:57.340083
f86fa162-9628-4ddd-be27-b09a40d93b83	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial machinery parts	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 21:36:57.37282
67e7e2e0-b3e9-4983-95cd-4a119055bebb	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 21:36:57.405607
62e5f0aa-d7ea-4f94-af36-cb57b0888c6a	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 21:36:57.438223
a859fc07-c4bf-4ee5-980b-37ea0c375197	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive transmission components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 21:36:57.471748
41e86262-8888-4a6f-a482-291a5e61a8c9	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 21:36:57.504376
bad26a33-2a29-4bae-a213-3cad61e6fb6e	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 21:45:22.85552
17ad3721-2d33-4579-bcc4-95d666428722	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 21:45:22.888781
e81e2095-17fd-410e-a69a-576a9b4dd715	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 21:45:22.923063
1d79a741-44db-49a0-a33b-e633ca1c64fd	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive transmission components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 21:45:22.955456
1b0bb3e9-215b-42d5-aa77-7015f1fa97ba	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 21:45:22.988127
973baeb3-df2a-45ef-9d64-3b51555c997a	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 21:45:23.020921
ba145f86-89cc-4513-b353-6a31495ee91b	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 21:45:23.053232
9e200ef7-f811-4708-9e13-e1d2097fc11e	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 21:45:23.086028
d9e00634-e789-4846-80b9-11e9300b2aff	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial machinery parts	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 21:45:23.11821
75bce87b-d640-4bc5-8a38-9f8a19b07693	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive transmission components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 21:45:23.150511
89e44e4c-3df9-43d5-8841-64e37b329881	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial machinery parts	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 21:45:23.183087
781513f5-bbe2-4463-8fa7-efe2daa682b7	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 21:45:23.215901
569082c3-183b-474e-8d06-2f0a8788dbdb	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 21:45:23.248337
6c347a99-6b4a-4558-b848-e432d0a71c47	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 21:45:23.280826
a35892c5-78d9-4422-ab38-12e532f9c5dd	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 21:45:23.313677
8afceb05-6b28-4d41-a03f-d6d2abf64f5a	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial machinery parts	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 21:45:23.347216
3406b960-78bf-4d91-ba51-9043e96e8a9a	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 21:45:23.379334
38ef74d0-d700-4ca0-9e89-b62300a98c65	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 22:03:52.029548
9dd5041b-ce7e-42e8-b6e0-a3ca18b19a17	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 22:03:52.063819
ecd3cdbe-778e-4ba9-8337-a89d2edf8ec1	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 22:03:52.097359
2ecea884-b28a-4ea0-90fb-afec5b292a99	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive transmission components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 22:03:52.131144
61f4e0f6-2472-4beb-8e98-1106faab75da	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 22:03:52.164661
162e50e5-ea2f-400c-a0c2-2fb531ba0eb8	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 22:03:52.198697
57efb15e-cbc4-4b6b-b589-454910f22a1e	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 22:03:52.232066
de6fe8fe-cf49-44e0-a0b6-0a3a5cfeb592	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 22:03:52.266279
7fcd5caa-90e6-485b-ab82-dc36a46e695e	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial machinery parts	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 22:03:52.299716
a6c7c6da-ddc8-48de-b9b8-74e69890e86c	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive transmission components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 22:03:52.333973
b0776bc3-1de4-42f6-99fb-60a4f7a505dc	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial machinery parts	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 22:03:52.368464
6baee126-b71a-46a2-a230-fedf0aff1c92	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 22:03:52.40216
18a47190-fe62-4b0a-a2c0-13103229ee1e	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 22:03:52.435844
6cff66c1-d52e-4fb6-8037-6f634c2e791a	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-12 15:25:55.157079
4beef5f3-e8ec-44b2-8bf5-c84cac6e286a	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-12 15:25:55.179905
a4641778-88d9-4c70-8d09-882584dc1082	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial machinery parts	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-12 15:25:55.202729
f7341e09-5b27-4a44-ab74-85d5c753da9b	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive transmission components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-12 15:25:55.226431
2eaa122b-bf65-4720-957d-5840ae154ab2	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 22:09:17.174079
9358f71f-5493-4cb9-8a88-8277bcb47317	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 22:09:17.206568
216cab2b-0474-4e72-9321-319e518c2b0a	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive transmission components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 22:09:17.238757
280c6f47-9504-45df-b185-30e0185460aa	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 22:09:17.270885
eb9352b9-c3d7-4798-bd91-5de421c93034	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 22:09:17.302824
4d45434b-3b2d-4668-9583-1fa96786d83b	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 22:09:17.33635
23885a2a-3489-4ad6-82e9-7490d904c4c5	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 22:09:17.36722
47def537-1f68-4437-8710-c59bb834b2dc	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial machinery parts	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 22:09:17.399479
f889c875-58f9-43c8-b80a-1b3c9c63df1c	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive transmission components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 22:09:17.433232
c808dc58-8832-4378-8f6f-203f9690054e	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial machinery parts	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 22:09:17.470574
fe80923e-01ab-4c04-b911-9bc429251537	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 22:09:17.502241
e126154f-86ad-400b-82dd-cf5419c7af99	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 22:09:17.534323
0d6d14f5-d4c6-42c5-ae07-4f524248d11c	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 22:09:17.566471
99654ac0-0d6e-4d8c-90a4-929c728c9c1b	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 22:09:17.598687
e571973c-fdba-41b3-be07-94946d5b93a7	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial machinery parts	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 22:09:17.631443
70fd5ed0-2272-4622-bfab-31d806b70234	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 22:09:17.66393
bb8a7740-f252-4858-a697-68d76394f604	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial machinery parts	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-12 15:25:55.28885
39e93196-5cb5-41af-91b5-ae8ed7878a31	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-12 15:25:55.315395
d186159d-9836-481b-a9b1-72489b3bab49	\N	\N	2024-03-15 00:00:00	TXN-2024-001	MAPLE-001	Aurora Flame Maple	Ornamental Trees	Primary	USD	30000.00	27000.00	6200.0000	4.84	\N	\N	2025-11-11 22:16:43.865343
40daad91-ffa7-4aee-a35a-51e1428695b4	\N	\N	2024-03-20 00:00:00	TXN-2024-002	MAPLE-002	Aurora Flame Maple	Ornamental Trees	Primary	USD	25000.00	22500.00	1100.0000	22.73	\N	\N	2025-11-11 22:16:43.898779
a30e4684-963a-4042-b692-22b226b4804f	\N	\N	2024-10-05 00:00:00	TXN-2024-003	JUNIPER-001	Golden Spire Juniper	Ornamental Shrubs	Secondary	USD	28000.00	25200.00	1800.0000	15.56	\N	\N	2025-11-11 22:16:43.931624
d8b3a8fa-3ded-4ad4-8b0b-a332355bbb99	\N	\N	2024-04-12 00:00:00	TXN-2024-004	ROSE-001	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	12000.00	10800.00	3000.0000	4.00	\N	\N	2025-11-11 22:16:43.97369
ab6e43b1-863e-450f-a970-6708a588239e	\N	\N	2024-09-18 00:00:00	TXN-2024-005	HOSTA-001	Emerald Crown Hosta	Perennials	Primary	USD	18000.00	16200.00	900.0000	20.00	\N	\N	2025-11-11 22:16:44.007182
5a31488b-fab2-407e-86ce-b1d0e322c0ff	\N	\N	2024-03-25 00:00:00	TXN-2024-006	HYDRANGEA-001	Cascade Blue Hydrangea	Flowering Shrubs	Primary	USD	120000.00	108000.00	20000.0000	6.00	\N	\N	2025-11-11 22:16:44.046455
93fe33d1-ef2f-4a03-aea4-f4c2a2a21b37	\N	\N	2024-12-15 00:00:00	TXN-2024-007	ROSE-002	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	5000.00	4500.00	250.0000	20.00	\N	\N	2025-11-11 22:16:44.08364
bd487e7b-f91e-4bd8-b9d2-42f7865a8bfc	\N	\N	2024-05-10 00:00:00	TXN-2024-008	MAPLE-003	Aurora Flame Maple	Ornamental Trees	Secondary	USD	15000.00	13500.00	3000.0000	5.00	\N	\N	2025-11-11 22:16:44.116712
bfd107b8-9b8a-446d-a8fa-fafcca6336f8	\N	\N	2024-06-15 00:00:00	TXN-2024-009	JUNIPER-002	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	22000.00	19800.00	800.0000	27.50	\N	\N	2025-11-11 22:16:44.149307
aa99a268-bea3-4548-83c8-21b99076c4af	\N	\N	2024-07-20 00:00:00	TXN-2024-010	HYDRANGEA-002	Cascade Blue Hydrangea	Flowering Shrubs	Secondary	USD	18000.00	16200.00	1200.0000	15.00	\N	\N	2025-11-11 22:16:44.184393
4c61dd5b-65eb-4d71-ada8-0cc366cd4b8f	\N	\N	2024-08-10 00:00:00	TXN-2024-011	HOSTA-002	Emerald Crown Hosta	Perennials	Primary	USD	12000.00	10800.00	1500.0000	8.00	\N	\N	2025-11-11 22:16:44.217269
e840904a-4de0-464c-b28d-d7092910448a	\N	\N	2024-09-05 00:00:00	TXN-2024-012	ROSE-003	Pacific Sunset Rose	Flowering Shrubs	Secondary	USD	16000.00	14400.00	1000.0000	16.00	\N	\N	2025-11-11 22:16:44.250089
943f7ca7-ba9b-4bd0-ae1d-aa340e90d3e1	\N	\N	2024-10-15 00:00:00	TXN-2024-013	MAPLE-004	Aurora Flame Maple	Ornamental Trees	Primary	USD	35000.00	31500.00	2000.0000	17.50	\N	\N	2025-11-11 22:16:44.283021
5e7f80f5-6770-4431-b1c5-916390a09a8c	\N	\N	2024-11-20 00:00:00	TXN-2024-014	JUNIPER-003	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	8000.00	7200.00	1600.0000	5.00	\N	\N	2025-11-11 22:16:44.315555
a8c8342e-895a-4a38-af57-4ea0f0f6ea82	\N	\N	2024-12-10 00:00:00	TXN-2024-015	HOSTA-003	Emerald Crown Hosta	Perennials	Secondary	USD	20000.00	18000.00	750.0000	26.67	\N	\N	2025-11-11 22:16:44.348556
1583d476-ca02-4e55-9b68-4e1d452f45bb	\N	\N	2024-03-15 00:00:00	ELC-2024-001	\N	ARM Processor A72	ARM-Compatible Processors	United States	USD	3900000.00	0.00	1200000.0000	3.25	\N	\N	2025-11-11 22:14:38.368432
ce8d2520-4dff-4b8d-bdaf-806d4ed6e827	\N	\N	2024-03-20 00:00:00	ELC-2024-002	\N	Power Management IC PM500	Power Management ICs	United States	USD	4375000.00	0.00	2500000.0000	1.75	\N	\N	2025-11-11 22:14:38.409376
14ee133b-05e9-4be3-b806-f4f19e8f9021	\N	\N	2024-04-05 00:00:00	ELC-2024-003	\N	Memory Controller MC850	Memory Controllers	United States	USD	3870000.00	0.00	1800000.0000	2.15	\N	\N	2025-11-11 22:14:38.445413
0857fd44-9f53-48f5-b4fb-d47b608d1828	\N	\N	2024-08-25 00:00:00	ELC-2024-016	\N	Premium Tablet T10	Tablets and E-Readers	Japan	USD	16800000.00	0.00	35000.0000	480.00	\N	\N	2025-11-11 22:14:38.901784
92e36353-743d-4535-befe-002a438451e1	\N	\N	2024-09-05 00:00:00	ELC-2024-017	\N	Smart Home Hub SH200	Smart Home Devices	European Union	USD	12825000.00	0.00	95000.0000	135.00	\N	\N	2025-11-11 22:14:38.936073
df7af902-44cb-4076-acca-bb5f554f9f0c	\N	\N	2024-09-18 00:00:00	ELC-2024-018	\N	Fitness Tracker FT500	Wearable Electronics	South Korea	USD	26600000.00	0.00	140000.0000	190.00	\N	\N	2025-11-11 22:14:38.969484
e6a234b1-6085-4765-bc21-f266f36db342	\N	\N	2024-09-28 00:00:00	ELC-2024-019	\N	Auto Infotainment AI800	Automotive Electronics	European Union	USD	17595000.00	0.00	18000.0000	977.50	\N	\N	2025-11-11 22:14:39.002969
2d5a8d79-76d6-4b66-a2db-4d9c66abf155	\N	\N	2024-10-10 00:00:00	ELC-2024-020	\N	Medical Monitor MM300	Medical Device Electronics	Japan	USD	18000000.00	0.00	12000.0000	1500.00	\N	\N	2025-11-11 22:14:39.036343
f3476df8-ba71-4556-a259-2691e94905bc	\N	\N	2024-03-15 00:00:00	TXN-2024-001	MAPLE-001	Aurora Flame Maple	Ornamental Trees	Primary	USD	30000.00	27000.00	6200.0000	4.84	\N	\N	2025-11-12 15:26:53.38339
7f48e1f9-38ea-4c50-bfa6-ae5b26c22a8b	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive transmission components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-12 15:33:03.441548
98249528-1320-441b-aa72-39cec1567888	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial machinery parts	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-12 15:33:03.467766
79b7e744-16e8-42de-b3cd-bcd983838471	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-12 15:33:03.494649
1cb614e3-205e-45e3-b020-79cf75468414	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-12 15:33:03.521181
afec807f-1f20-4065-8eaf-d6f8935c3dad	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-12 15:33:03.548952
ce988fcc-62db-4f13-9c8a-a13ccd9ec769	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-12 15:33:03.575361
3e640420-9d5c-4e13-929d-c0f7523af239	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial machinery parts	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-12 15:33:03.602662
cb923af0-ea03-4f82-848a-16ae68b21389	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-12 15:33:03.627723
57035850-2c1d-40f8-b5fd-fc036eef1edf	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-12 15:33:03.654153
1a03a986-afdb-4776-a7d1-d2ac05a68334	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive transmission components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-12 15:33:03.680465
d94a3f5f-e49e-4d7c-aaa8-9ea80f09ce12	8c7a356f-33ce-4eaa-8cfe-eb483b6a823f	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-12 15:33:03.706936
1bf4c94a-5b98-4b99-9dd3-2eb3bd9046c4	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-03-15 00:00:00	TXN-2024-001	MAPLE-001	Aurora Flame Maple	Ornamental Trees	Primary	USD	30000.00	27000.00	6200.0000	4.84	\N	\N	2025-11-12 15:37:10.370762
e6c1426e-e596-430f-8c04-26985ecb77aa	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-03-20 00:00:00	TXN-2024-002	MAPLE-002	Aurora Flame Maple	Ornamental Trees	Primary	USD	25000.00	22500.00	1100.0000	22.73	\N	\N	2025-11-12 15:37:10.394346
b3e8d2e1-f30d-4b9d-b4bb-daa4a7381d9f	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-10-05 00:00:00	TXN-2024-003	JUNIPER-001	Golden Spire Juniper	Ornamental Shrubs	Secondary	USD	28000.00	25200.00	1800.0000	15.56	\N	\N	2025-11-12 15:37:10.41742
723294f2-27ea-45d9-b7ee-b5d3be46cc83	\N	\N	2024-04-12 00:00:00	ELC-2024-004	\N	WiFi Chipset WF6E	Wireless Chipsets	United States	USD	2700000.00	0.00	600000.0000	4.50	\N	\N	2025-11-11 22:14:38.478784
18b9caf7-456d-4ae6-9658-72a7a214d0bd	\N	\N	2024-04-18 00:00:00	ELC-2024-005	\N	AI Accelerator APU-X1	AI Acceleration Units	United States	USD	2025000.00	0.00	300000.0000	6.75	\N	\N	2025-11-11 22:14:38.512455
565bf216-916c-494e-8b49-07fc2fe37fbb	\N	\N	2024-05-02 00:00:00	ELC-2024-006	\N	Premium Tablet T10	Tablets and E-Readers	European Union	USD	22500000.00	0.00	50000.0000	450.00	\N	\N	2025-11-11 22:14:38.548616
bbcd5ca6-c504-453e-9f00-a113dd0786d0	\N	\N	2024-05-10 00:00:00	ELC-2024-007	\N	Ultrabook Pro 15	Laptops and Ultrabooks	United States	USD	36000000.00	0.00	30000.0000	1200.00	\N	\N	2025-11-11 22:14:38.582803
e9ead60e-6586-4d04-adb9-c64f573c04ce	\N	\N	2024-05-22 00:00:00	ELC-2024-008	\N	Smart Home Hub SH200	Smart Home Devices	United States	USD	10000000.00	0.00	80000.0000	125.00	\N	\N	2025-11-11 22:14:38.620787
ebe69158-f906-4e50-9ec8-e845236b0e4d	\N	\N	2024-06-01 00:00:00	ELC-2024-009	\N	Fitness Tracker FT500	Wearable Electronics	United States	USD	21600000.00	0.00	120000.0000	180.00	\N	\N	2025-11-11 22:14:38.665215
7da5eb15-ae9b-4130-a268-60a4c7683a4b	\N	\N	2024-06-15 00:00:00	ELC-2024-010	\N	Auto Infotainment AI800	Automotive Electronics	United States	USD	21250000.00	0.00	25000.0000	850.00	\N	\N	2025-11-11 22:14:38.700246
651ae76d-e3fc-436f-8e1d-9c0013a1e7e1	\N	\N	2024-06-28 00:00:00	ELC-2024-011	\N	Medical Monitor MM300	Medical Device Electronics	United States	USD	18000000.00	0.00	15000.0000	1200.00	\N	\N	2025-11-11 22:14:38.733968
a84fb25f-f707-4b9a-9bf8-ea10fe6013b7	\N	\N	2024-07-05 00:00:00	ELC-2024-012	\N	Industrial Controller IC750	Industrial Automation	United States	USD	22000000.00	0.00	40000.0000	550.00	\N	\N	2025-11-11 22:14:38.768324
82844d35-0d83-4476-8a2c-69680a21e78e	\N	\N	2024-07-18 00:00:00	ELC-2024-013	\N	Server Processor SP2000	Server and Data Center	United States	USD	20000000.00	0.00	8000.0000	2500.00	\N	\N	2025-11-11 22:14:38.801645
06ee25d8-03f7-427a-b819-3db9ced31249	\N	\N	2024-08-01 00:00:00	ELC-2024-014	\N	ARM Processor A72	ARM-Compatible Processors	Japan	USD	2422500.00	0.00	850000.0000	2.85	\N	\N	2025-11-11 22:14:38.834972
7c8b79ad-5a15-44be-8e91-c061fc5e74b0	\N	\N	2024-08-12 00:00:00	ELC-2024-015	\N	Power Management IC PM500	Power Management ICs	European Union	USD	2610000.00	0.00	1800000.0000	1.45	\N	\N	2025-11-11 22:14:38.868481
310dd1df-a023-478c-a184-b57ce86a18e4	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 22:09:17.69588
b5110ccc-233a-430a-b71f-d6fafabbaea8	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive transmission components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 22:09:17.72786
9d1aa8c7-580c-462a-a4b7-18f8b1207c3f	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 22:09:17.759242
7e33bc6c-16ea-40cf-9be7-ff5cc024c104	\N	\N	2024-03-15 00:00:00	ELC-2024-001	\N	ARM Processor A72	ARM-Compatible Processors	United States	USD	3900000.00	0.00	1200000.0000	3.25	\N	\N	2025-11-12 15:24:37.48517
2ea53aba-5cc7-42b6-9dd8-365bd3292052	\N	\N	2024-03-20 00:00:00	ELC-2024-002	\N	Power Management IC PM500	Power Management ICs	United States	USD	4375000.00	0.00	2500000.0000	1.75	\N	\N	2025-11-12 15:24:37.520213
424e485d-c641-4b1b-9c90-5724290575d1	\N	\N	2024-04-05 00:00:00	ELC-2024-003	\N	Memory Controller MC850	Memory Controllers	United States	USD	3870000.00	0.00	1800000.0000	2.15	\N	\N	2025-11-12 15:24:37.546302
0d887096-fc8c-4609-b1ec-c2e12d325266	\N	\N	2024-04-12 00:00:00	ELC-2024-004	\N	WiFi Chipset WF6E	Wireless Chipsets	United States	USD	2700000.00	0.00	600000.0000	4.50	\N	\N	2025-11-12 15:24:37.571659
59b7212c-11e5-41ac-be3e-fe41363a2f02	\N	\N	2024-04-18 00:00:00	ELC-2024-005	\N	AI Accelerator APU-X1	AI Acceleration Units	United States	USD	2025000.00	0.00	300000.0000	6.75	\N	\N	2025-11-12 15:24:37.597014
ebcbb0f1-2d17-409f-a5c3-f5ee6284cba5	\N	\N	2024-05-02 00:00:00	ELC-2024-006	\N	Premium Tablet T10	Tablets and E-Readers	European Union	USD	22500000.00	0.00	50000.0000	450.00	\N	\N	2025-11-12 15:24:37.623069
6706bf0b-f163-4616-adf8-5993edb3e4f2	\N	\N	2024-05-10 00:00:00	ELC-2024-007	\N	Ultrabook Pro 15	Laptops and Ultrabooks	United States	USD	36000000.00	0.00	30000.0000	1200.00	\N	\N	2025-11-12 15:24:37.648929
9befe600-8c94-4a54-80cb-acef1c569968	\N	\N	2024-05-22 00:00:00	ELC-2024-008	\N	Smart Home Hub SH200	Smart Home Devices	United States	USD	10000000.00	0.00	80000.0000	125.00	\N	\N	2025-11-12 15:24:37.673635
95c5d6c6-fedc-403d-9cca-5f1f25d34f10	\N	\N	2024-06-01 00:00:00	ELC-2024-009	\N	Fitness Tracker FT500	Wearable Electronics	United States	USD	21600000.00	0.00	120000.0000	180.00	\N	\N	2025-11-12 15:24:37.699517
955d3df6-499b-4e08-aeca-ed06f0dfad6d	\N	\N	2024-06-15 00:00:00	ELC-2024-010	\N	Auto Infotainment AI800	Automotive Electronics	United States	USD	21250000.00	0.00	25000.0000	850.00	\N	\N	2025-11-12 15:24:37.724496
cbdcde19-5b50-45e3-9610-ee85fd0d94a9	\N	\N	2024-06-28 00:00:00	ELC-2024-011	\N	Medical Monitor MM300	Medical Device Electronics	United States	USD	18000000.00	0.00	15000.0000	1200.00	\N	\N	2025-11-12 15:24:37.749284
b01446da-744d-4e9b-9fab-e84ce8bd26e8	\N	\N	2024-07-05 00:00:00	ELC-2024-012	\N	Industrial Controller IC750	Industrial Automation	United States	USD	22000000.00	0.00	40000.0000	550.00	\N	\N	2025-11-12 15:24:37.775677
fec32de4-8f72-4203-b029-777a855ff7c7	\N	\N	2024-07-18 00:00:00	ELC-2024-013	\N	Server Processor SP2000	Server and Data Center	United States	USD	20000000.00	0.00	8000.0000	2500.00	\N	\N	2025-11-12 15:24:37.80085
c5f5d8de-68d9-429e-9c9d-7b29ef0bcbff	\N	\N	2024-08-01 00:00:00	ELC-2024-014	\N	ARM Processor A72	ARM-Compatible Processors	Japan	USD	2422500.00	0.00	850000.0000	2.85	\N	\N	2025-11-12 15:24:37.827811
6f520862-8c0a-4e72-8fec-32a59a092751	\N	\N	2024-08-12 00:00:00	ELC-2024-015	\N	Power Management IC PM500	Power Management ICs	European Union	USD	2610000.00	0.00	1800000.0000	1.45	\N	\N	2025-11-12 15:24:37.852936
5f505911-8766-48b6-93c5-0ebc6911db0b	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-03-15 00:00:00	ELC-2024-001	\N	ARM Processor A72	ARM-Compatible Processors	United States	USD	3900000.00	0.00	1200000.0000	3.25	\N	\N	2025-11-12 15:35:45.09366
5835cff6-d01a-42eb-9aab-9858d2910f74	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-03-20 00:00:00	ELC-2024-002	\N	Power Management IC PM500	Power Management ICs	United States	USD	4375000.00	0.00	2500000.0000	1.75	\N	\N	2025-11-12 15:35:45.127289
960794f7-fae2-4cdc-a101-c61ed8484aa1	\N	\N	2024-03-15 00:00:00	ELC-2024-001	\N	ARM Processor A72	ARM-Compatible Processors	United States	USD	3900000.00	0.00	1200000.0000	3.25	\N	\N	2025-11-11 22:54:40.864361
f8d325b5-3ea2-426b-b68d-5bd91103b084	\N	\N	2024-03-20 00:00:00	ELC-2024-002	\N	Power Management IC PM500	Power Management ICs	United States	USD	4375000.00	0.00	2500000.0000	1.75	\N	\N	2025-11-11 22:54:40.897223
045c1b01-c349-4c7e-ba8d-83085571dba5	\N	\N	2024-04-05 00:00:00	ELC-2024-003	\N	Memory Controller MC850	Memory Controllers	United States	USD	3870000.00	0.00	1800000.0000	2.15	\N	\N	2025-11-11 22:54:40.930239
403b2ab2-7a65-459a-900c-5dd090b9346d	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-04-05 00:00:00	ELC-2024-003	\N	Memory Controller MC850	Memory Controllers	United States	USD	3870000.00	0.00	1800000.0000	2.15	\N	\N	2025-11-12 15:35:45.149057
d54e594d-549e-43c7-92b9-8c19cd135965	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-04-12 00:00:00	ELC-2024-004	\N	WiFi Chipset WF6E	Wireless Chipsets	United States	USD	2700000.00	0.00	600000.0000	4.50	\N	\N	2025-11-12 15:35:45.172063
49166d47-18c7-49e1-b369-7411748f1f87	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-04-18 00:00:00	ELC-2024-005	\N	AI Accelerator APU-X1	AI Acceleration Units	United States	USD	2025000.00	0.00	300000.0000	6.75	\N	\N	2025-11-12 15:35:45.194374
0e635951-9f76-4a60-864b-63c4865e696b	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-05-02 00:00:00	ELC-2024-006	\N	Premium Tablet T10	Tablets and E-Readers	European Union	USD	22500000.00	0.00	50000.0000	450.00	\N	\N	2025-11-12 15:35:45.216949
36febdbc-e4b1-4453-8672-c092090785c2	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-05-10 00:00:00	ELC-2024-007	\N	Ultrabook Pro 15	Laptops and Ultrabooks	United States	USD	36000000.00	0.00	30000.0000	1200.00	\N	\N	2025-11-12 15:35:45.239417
9a382a0a-1263-4a15-96b9-03c2575d4877	\N	\N	2024-04-12 00:00:00	ELC-2024-004	\N	WiFi Chipset WF6E	Wireless Chipsets	United States	USD	2700000.00	0.00	600000.0000	4.50	\N	\N	2025-11-11 22:54:40.963142
ea6608f4-80d1-40e2-9674-29303d8103ba	\N	\N	2024-04-18 00:00:00	ELC-2024-005	\N	AI Accelerator APU-X1	AI Acceleration Units	United States	USD	2025000.00	0.00	300000.0000	6.75	\N	\N	2025-11-11 22:54:40.995935
b68eba29-3e9b-4f70-82b6-a575c1d8bd1c	\N	\N	2024-05-02 00:00:00	ELC-2024-006	\N	Premium Tablet T10	Tablets and E-Readers	European Union	USD	22500000.00	0.00	50000.0000	450.00	\N	\N	2025-11-11 22:54:41.028679
ef083d2f-4562-43f5-acc5-3b50b00882c4	\N	\N	2024-05-10 00:00:00	ELC-2024-007	\N	Ultrabook Pro 15	Laptops and Ultrabooks	United States	USD	36000000.00	0.00	30000.0000	1200.00	\N	\N	2025-11-11 22:54:41.061035
56af1724-181e-479b-82ae-7678c0f74278	\N	\N	2024-05-22 00:00:00	ELC-2024-008	\N	Smart Home Hub SH200	Smart Home Devices	United States	USD	10000000.00	0.00	80000.0000	125.00	\N	\N	2025-11-11 22:54:41.094087
9f0c736a-3a54-418e-9f1a-7a2a25426811	\N	\N	2024-06-01 00:00:00	ELC-2024-009	\N	Fitness Tracker FT500	Wearable Electronics	United States	USD	21600000.00	0.00	120000.0000	180.00	\N	\N	2025-11-11 22:54:41.126548
bac87fc6-5985-49b8-9f4b-08d86b830836	\N	\N	2024-06-15 00:00:00	ELC-2024-010	\N	Auto Infotainment AI800	Automotive Electronics	United States	USD	21250000.00	0.00	25000.0000	850.00	\N	\N	2025-11-11 22:54:41.158319
59be2a3a-d3c9-4206-ada6-b7f39cbec6b9	\N	\N	2024-06-28 00:00:00	ELC-2024-011	\N	Medical Monitor MM300	Medical Device Electronics	United States	USD	18000000.00	0.00	15000.0000	1200.00	\N	\N	2025-11-11 22:54:41.190947
c2e927ff-6aa9-4346-879f-6e2049fec73b	\N	\N	2024-07-05 00:00:00	ELC-2024-012	\N	Industrial Controller IC750	Industrial Automation	United States	USD	22000000.00	0.00	40000.0000	550.00	\N	\N	2025-11-11 22:54:41.224274
ea9d48ae-126b-4241-bc80-b986ffa9fcdb	\N	\N	2024-07-18 00:00:00	ELC-2024-013	\N	Server Processor SP2000	Server and Data Center	United States	USD	20000000.00	0.00	8000.0000	2500.00	\N	\N	2025-11-11 22:54:41.256639
b3b328ad-df83-4275-8a71-359528637328	\N	\N	2024-08-01 00:00:00	ELC-2024-014	\N	ARM Processor A72	ARM-Compatible Processors	Japan	USD	2422500.00	0.00	850000.0000	2.85	\N	\N	2025-11-11 22:54:41.289139
1131ab8f-000c-4039-98d3-ebffcd8173df	\N	\N	2024-08-12 00:00:00	ELC-2024-015	\N	Power Management IC PM500	Power Management ICs	European Union	USD	2610000.00	0.00	1800000.0000	1.45	\N	\N	2025-11-11 22:54:41.321629
8f2d2ae9-5494-4e60-b554-172efcdaf65d	\N	\N	2024-08-25 00:00:00	ELC-2024-016	\N	Premium Tablet T10	Tablets and E-Readers	Japan	USD	16800000.00	0.00	35000.0000	480.00	\N	\N	2025-11-11 22:54:41.354174
60bbc01d-41db-4c53-b30d-edbae32b6fc8	\N	\N	2024-09-05 00:00:00	ELC-2024-017	\N	Smart Home Hub SH200	Smart Home Devices	European Union	USD	12825000.00	0.00	95000.0000	135.00	\N	\N	2025-11-11 22:54:41.386799
9a852754-8fbe-4895-8a8e-c5c4f36ce671	\N	\N	2024-09-18 00:00:00	ELC-2024-018	\N	Fitness Tracker FT500	Wearable Electronics	South Korea	USD	26600000.00	0.00	140000.0000	190.00	\N	\N	2025-11-11 22:54:41.419743
79ec3aa6-0c92-4c80-a367-4f88d606d3d1	\N	\N	2024-09-28 00:00:00	ELC-2024-019	\N	Auto Infotainment AI800	Automotive Electronics	European Union	USD	17595000.00	0.00	18000.0000	977.50	\N	\N	2025-11-11 22:54:41.452258
6457bae0-3943-461c-965c-58ef725d0b5e	\N	\N	2024-10-10 00:00:00	ELC-2024-020	\N	Medical Monitor MM300	Medical Device Electronics	Japan	USD	18000000.00	0.00	12000.0000	1500.00	\N	\N	2025-11-11 22:54:41.484856
f2080997-dad3-4083-b315-d3c26f7dbc93	\N	\N	2024-03-15 00:00:00	ELC-2024-001	\N	ARM Processor A72	ARM-Compatible Processors	United States	USD	3900000.00	0.00	1200000.0000	3.25	\N	\N	2025-11-12 15:31:53.927928
32b7a50d-9805-4ddf-8c61-f3bec6aa2cf5	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-05-22 00:00:00	ELC-2024-008	\N	Smart Home Hub SH200	Smart Home Devices	United States	USD	10000000.00	0.00	80000.0000	125.00	\N	\N	2025-11-12 15:35:45.262104
e0e8c2d1-2273-44b1-9b78-3bc7517c15a7	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-06-01 00:00:00	ELC-2024-009	\N	Fitness Tracker FT500	Wearable Electronics	United States	USD	21600000.00	0.00	120000.0000	180.00	\N	\N	2025-11-12 15:35:45.284328
054aa32d-623a-49f0-9459-7fb644b70b49	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-06-15 00:00:00	ELC-2024-010	\N	Auto Infotainment AI800	Automotive Electronics	United States	USD	21250000.00	0.00	25000.0000	850.00	\N	\N	2025-11-12 15:35:45.306794
697e538c-9361-4412-865c-ec23b5935785	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-06-28 00:00:00	ELC-2024-011	\N	Medical Monitor MM300	Medical Device Electronics	United States	USD	18000000.00	0.00	15000.0000	1200.00	\N	\N	2025-11-12 15:35:45.329069
4b51cebb-9fa6-4e5a-9d5c-5184752858e3	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-07-05 00:00:00	ELC-2024-012	\N	Industrial Controller IC750	Industrial Automation	United States	USD	22000000.00	0.00	40000.0000	550.00	\N	\N	2025-11-12 15:35:45.35189
ca77862c-d9f0-4b6f-91d2-cc540c89cbce	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-07-18 00:00:00	ELC-2024-013	\N	Server Processor SP2000	Server and Data Center	United States	USD	20000000.00	0.00	8000.0000	2500.00	\N	\N	2025-11-12 15:35:45.375507
bbd7de79-f054-4a8a-8c47-1f8f7d1a60e2	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-08-01 00:00:00	ELC-2024-014	\N	ARM Processor A72	ARM-Compatible Processors	Japan	USD	2422500.00	0.00	850000.0000	2.85	\N	\N	2025-11-12 15:35:45.397195
e16ae15e-babb-4f25-89c2-d35728370eb3	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-08-12 00:00:00	ELC-2024-015	\N	Power Management IC PM500	Power Management ICs	European Union	USD	2610000.00	0.00	1800000.0000	1.45	\N	\N	2025-11-12 15:35:45.419864
bd7267da-2a5b-478b-a10a-e000d6500140	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-08-25 00:00:00	ELC-2024-016	\N	Premium Tablet T10	Tablets and E-Readers	Japan	USD	16800000.00	0.00	35000.0000	480.00	\N	\N	2025-11-12 15:35:45.442636
a3c16886-b0ef-4d79-b4ca-697ce8ab963f	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-09-05 00:00:00	ELC-2024-017	\N	Smart Home Hub SH200	Smart Home Devices	European Union	USD	12825000.00	0.00	95000.0000	135.00	\N	\N	2025-11-12 15:35:45.465527
d8b1e3c9-af61-4cb3-b311-9f3476394186	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-09-18 00:00:00	ELC-2024-018	\N	Fitness Tracker FT500	Wearable Electronics	South Korea	USD	26600000.00	0.00	140000.0000	190.00	\N	\N	2025-11-12 15:35:45.48824
bf99be26-1c73-4903-aa79-d39e992ce164	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-09-28 00:00:00	ELC-2024-019	\N	Auto Infotainment AI800	Automotive Electronics	European Union	USD	17595000.00	0.00	18000.0000	977.50	\N	\N	2025-11-12 15:35:45.511674
da5d6136-8caa-4797-be8e-234f89b8e842	8f321b79-1ab4-4b02-8806-26db3a21a8ff	\N	2024-10-10 00:00:00	ELC-2024-020	\N	Medical Monitor MM300	Medical Device Electronics	Japan	USD	18000000.00	0.00	12000.0000	1500.00	\N	\N	2025-11-12 15:35:45.534718
b9b0d3c0-b469-4d74-b625-89c20a0e7548	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-04-12 00:00:00	TXN-2024-004	ROSE-001	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	12000.00	10800.00	3000.0000	4.00	\N	\N	2025-11-12 15:37:10.440214
75e29720-bd56-475c-8293-46cfdb4ee400	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-09-18 00:00:00	TXN-2024-005	HOSTA-001	Emerald Crown Hosta	Perennials	Primary	USD	18000.00	16200.00	900.0000	20.00	\N	\N	2025-11-12 15:37:10.463751
2be37310-e76c-41bd-85fd-83b05753c602	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-03-25 00:00:00	TXN-2024-006	HYDRANGEA-001	Cascade Blue Hydrangea	Flowering Shrubs	Primary	USD	120000.00	108000.00	20000.0000	6.00	\N	\N	2025-11-12 15:37:10.488029
411b8a4d-f6bb-4bef-a206-590e2187d37f	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-12-15 00:00:00	TXN-2024-007	ROSE-002	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	5000.00	4500.00	250.0000	20.00	\N	\N	2025-11-12 15:37:10.511291
aedaef70-ae36-4d82-b34e-f93387e57973	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-05-10 00:00:00	TXN-2024-008	MAPLE-003	Aurora Flame Maple	Ornamental Trees	Secondary	USD	15000.00	13500.00	3000.0000	5.00	\N	\N	2025-11-12 15:37:10.537349
0b659b6a-2710-4e67-83d8-0be2e3d526e0	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-06-15 00:00:00	TXN-2024-009	JUNIPER-002	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	22000.00	19800.00	800.0000	27.50	\N	\N	2025-11-12 15:37:10.560412
eff99248-2279-4886-941f-d38c94ba824a	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-07-20 00:00:00	TXN-2024-010	HYDRANGEA-002	Cascade Blue Hydrangea	Flowering Shrubs	Secondary	USD	18000.00	16200.00	1200.0000	15.00	\N	\N	2025-11-12 15:37:10.583229
7fdfbd2a-5f21-4e86-a577-585b89058fd5	\N	\N	2024-03-20 00:00:00	ELC-2024-002	\N	Power Management IC PM500	Power Management ICs	United States	USD	4375000.00	0.00	2500000.0000	1.75	\N	\N	2025-11-12 15:31:53.954859
a5cd9b9f-6d27-4b4c-96ec-2c5c8b160bbe	\N	\N	2024-04-05 00:00:00	ELC-2024-003	\N	Memory Controller MC850	Memory Controllers	United States	USD	3870000.00	0.00	1800000.0000	2.15	\N	\N	2025-11-12 15:31:53.981292
47e684b5-c954-4657-9d7a-2c5db6d7d4f3	\N	\N	2024-04-12 00:00:00	ELC-2024-004	\N	WiFi Chipset WF6E	Wireless Chipsets	United States	USD	2700000.00	0.00	600000.0000	4.50	\N	\N	2025-11-12 15:31:54.008738
3c4fcd48-b01e-4084-8c45-f8db8ab40f5b	\N	\N	2024-04-18 00:00:00	ELC-2024-005	\N	AI Accelerator APU-X1	AI Acceleration Units	United States	USD	2025000.00	0.00	300000.0000	6.75	\N	\N	2025-11-12 15:31:54.035032
43159f0c-c292-4834-b547-64865b849b12	\N	\N	2024-05-02 00:00:00	ELC-2024-006	\N	Premium Tablet T10	Tablets and E-Readers	European Union	USD	22500000.00	0.00	50000.0000	450.00	\N	\N	2025-11-12 15:31:54.061467
17bf919c-4876-4fcf-9fd3-193d12ecf969	\N	\N	2024-05-10 00:00:00	ELC-2024-007	\N	Ultrabook Pro 15	Laptops and Ultrabooks	United States	USD	36000000.00	0.00	30000.0000	1200.00	\N	\N	2025-11-12 15:31:54.088532
da34063c-cad6-48f2-b080-63f94de61736	\N	\N	2024-05-22 00:00:00	ELC-2024-008	\N	Smart Home Hub SH200	Smart Home Devices	United States	USD	10000000.00	0.00	80000.0000	125.00	\N	\N	2025-11-12 15:31:54.114671
b9498128-18ce-4d8c-8c96-e7289ccba76b	\N	\N	2024-06-01 00:00:00	ELC-2024-009	\N	Fitness Tracker FT500	Wearable Electronics	United States	USD	21600000.00	0.00	120000.0000	180.00	\N	\N	2025-11-12 15:31:54.139841
b5a104d8-91be-4fbf-a0e8-946f0d7c1e1a	\N	\N	2024-06-15 00:00:00	ELC-2024-010	\N	Auto Infotainment AI800	Automotive Electronics	United States	USD	21250000.00	0.00	25000.0000	850.00	\N	\N	2025-11-12 15:31:54.166347
423f8166-ec9e-454d-b009-156d56b5d1cd	\N	\N	2024-06-28 00:00:00	ELC-2024-011	\N	Medical Monitor MM300	Medical Device Electronics	United States	USD	18000000.00	0.00	15000.0000	1200.00	\N	\N	2025-11-12 15:31:54.192455
d7c45099-ce81-43d1-98e6-7617b63ac280	\N	\N	2024-07-05 00:00:00	ELC-2024-012	\N	Industrial Controller IC750	Industrial Automation	United States	USD	22000000.00	0.00	40000.0000	550.00	\N	\N	2025-11-12 15:31:54.219918
ac9ac22a-d095-4678-9e41-8df5398202cd	\N	\N	2024-07-18 00:00:00	ELC-2024-013	\N	Server Processor SP2000	Server and Data Center	United States	USD	20000000.00	0.00	8000.0000	2500.00	\N	\N	2025-11-12 15:31:54.245965
b8ed0130-949e-4f77-b68a-c6cfe3e81435	\N	\N	2024-08-01 00:00:00	ELC-2024-014	\N	ARM Processor A72	ARM-Compatible Processors	Japan	USD	2422500.00	0.00	850000.0000	2.85	\N	\N	2025-11-12 15:31:54.274222
dba4d17d-bb87-41e7-b394-6ff7ba60ce68	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-11 23:17:58.746395
4a90be2a-bf6d-4492-a35f-5da46090813b	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-11 23:17:58.769771
bdb5aa26-92a3-47c7-a4d4-c3d6dc17e648	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-11 23:17:58.792575
372e1a7f-e12f-4e4b-b0f6-c444ef20f3c8	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive transmission components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-11 23:17:58.849994
207dd440-94f3-4d03-ac52-6b8938a794a9	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-11 23:17:58.872758
b885647f-1912-4ec1-bc89-8022b2faf730	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-11 23:17:58.894916
a36392ed-8463-43c7-93df-948f07169b58	\N	\N	2024-05-08 00:00:00	MFG-2024-007	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	5270000.00	0.00	62000.0000	85.00	\N	\N	2025-11-11 23:17:58.918793
826f3842-7865-4d88-ab8d-4738296dedbf	\N	\N	2024-05-22 00:00:00	MFG-2024-008	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6000000.00	0.00	48000.0000	125.00	\N	\N	2025-11-11 23:17:58.942341
232e6522-9076-4096-9727-238ddef36648	\N	\N	2024-06-05 00:00:00	MFG-2024-009	\N	Precision Molded Component PM-350	Industrial machinery parts	Canada	USD	2875000.00	0.00	25000.0000	115.00	\N	\N	2025-11-11 23:17:58.965126
d74dfaee-2303-496f-9687-6f65727b53b3	\N	\N	2024-06-18 00:00:00	MFG-2024-010	\N	Smart Material Component SM-100	Automotive transmission components	United States	USD	6475000.00	0.00	35000.0000	185.00	\N	\N	2025-11-11 23:17:58.988269
24981834-0d98-4f15-ad4d-98e3d29956a6	\N	\N	2024-07-02 00:00:00	MFG-2024-011	\N	Industrial Bearing IB-750	Industrial machinery parts	Mexico	USD	4500000.00	0.00	18000.0000	250.00	\N	\N	2025-11-11 23:17:59.011154
6151cf99-52a1-487b-8885-17da2d692fc4	\N	\N	2024-07-15 00:00:00	MFG-2024-012	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	3780000.00	0.00	2800.0000	1350.00	\N	\N	2025-11-11 23:17:59.033996
b1a19a55-9497-43f4-8f91-74ab3a0033b1	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-11 23:17:59.057072
e5624d56-938d-4522-abab-38f1109a955e	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-11 23:17:59.079844
a4d7bdd7-5a78-4a25-ad58-d0ffc800be0b	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-11 23:17:59.102575
6dcc5cde-6b94-48c5-b678-c27bcc5e63eb	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial machinery parts	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-11 23:17:59.128768
2271e0e0-6a7b-428b-a7f3-0a9535716ac1	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-11 23:17:59.151453
16466502-8db5-4059-bc1b-3f19e173ebc2	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-11 23:17:59.174116
ddcc65b9-74cb-413f-9a31-f686a5c86b32	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive transmission components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-11 23:17:59.196746
1ab50f88-bbbc-4531-8927-fffad54cc701	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-11 23:17:59.219979
61ed1e3d-45d9-41e2-9f29-4d18d552bca7	\N	\N	2024-03-15 00:00:00	TXN-2024-001	MAPLE-001	Aurora Flame Maple	Ornamental Trees	Primary	USD	30000.00	27000.00	6200.0000	4.84	\N	\N	2025-11-11 23:19:09.975381
64b28dbe-7397-4872-b456-5e2bc47d509a	\N	\N	2024-03-20 00:00:00	TXN-2024-002	MAPLE-002	Aurora Flame Maple	Ornamental Trees	Primary	USD	25000.00	22500.00	1100.0000	22.73	\N	\N	2025-11-11 23:19:09.998627
11b6e20b-c8e2-476f-900f-93be92bbc42d	\N	\N	2024-08-12 00:00:00	ELC-2024-015	\N	Power Management IC PM500	Power Management ICs	European Union	USD	2610000.00	0.00	1800000.0000	1.45	\N	\N	2025-11-12 15:31:54.300165
b8f58659-d6d6-4499-a0ec-2e5edccc91ee	\N	\N	2024-08-25 00:00:00	ELC-2024-016	\N	Premium Tablet T10	Tablets and E-Readers	Japan	USD	16800000.00	0.00	35000.0000	480.00	\N	\N	2025-11-12 15:31:54.326435
fd2cc88c-d047-4b97-bc7a-4c0bd1c2c27c	\N	\N	2024-09-05 00:00:00	ELC-2024-017	\N	Smart Home Hub SH200	Smart Home Devices	European Union	USD	12825000.00	0.00	95000.0000	135.00	\N	\N	2025-11-12 15:31:54.353835
d12fdb3a-574d-4be0-9788-3a7a5f2d2260	\N	\N	2024-09-18 00:00:00	ELC-2024-018	\N	Fitness Tracker FT500	Wearable Electronics	South Korea	USD	26600000.00	0.00	140000.0000	190.00	\N	\N	2025-11-12 15:31:54.380592
57e7ec0d-a554-41b8-b86e-ec032a5007e6	\N	\N	2024-09-28 00:00:00	ELC-2024-019	\N	Auto Infotainment AI800	Automotive Electronics	European Union	USD	17595000.00	0.00	18000.0000	977.50	\N	\N	2025-11-12 15:31:54.406965
803aff2e-4fdd-4301-9a55-d82ea52c7961	\N	\N	2024-10-10 00:00:00	ELC-2024-020	\N	Medical Monitor MM300	Medical Device Electronics	Japan	USD	18000000.00	0.00	12000.0000	1500.00	\N	\N	2025-11-12 15:31:54.433462
7d965490-b13d-4d06-851e-c1fec444cad4	\N	\N	2024-10-05 00:00:00	TXN-2024-003	JUNIPER-001	Golden Spire Juniper	Ornamental Shrubs	Secondary	USD	28000.00	25200.00	1800.0000	15.56	\N	\N	2025-11-11 23:19:10.021709
ded63141-7297-4207-82c5-0703ef50746d	\N	\N	2024-04-12 00:00:00	TXN-2024-004	ROSE-001	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	12000.00	10800.00	3000.0000	4.00	\N	\N	2025-11-11 23:19:10.04557
998b49cf-8eb8-4ad3-ba90-84f6075a319b	\N	\N	2024-09-18 00:00:00	TXN-2024-005	HOSTA-001	Emerald Crown Hosta	Perennials	Primary	USD	18000.00	16200.00	900.0000	20.00	\N	\N	2025-11-11 23:19:10.068623
e415e9b8-a2b4-4d77-bffa-7075e18c2607	\N	\N	2024-03-25 00:00:00	TXN-2024-006	HYDRANGEA-001	Cascade Blue Hydrangea	Flowering Shrubs	Primary	USD	120000.00	108000.00	20000.0000	6.00	\N	\N	2025-11-11 23:19:10.092362
1f638318-fae7-42c9-8a0a-b1d06b89d522	\N	\N	2024-12-15 00:00:00	TXN-2024-007	ROSE-002	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	5000.00	4500.00	250.0000	20.00	\N	\N	2025-11-11 23:19:10.11568
e27b54e0-561b-49ba-b698-7bfa1477d518	\N	\N	2024-05-10 00:00:00	TXN-2024-008	MAPLE-003	Aurora Flame Maple	Ornamental Trees	Secondary	USD	15000.00	13500.00	3000.0000	5.00	\N	\N	2025-11-11 23:19:10.139618
48a51358-c53b-4bb1-a00e-38bb367e4588	\N	\N	2024-06-15 00:00:00	TXN-2024-009	JUNIPER-002	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	22000.00	19800.00	800.0000	27.50	\N	\N	2025-11-11 23:19:10.163597
2506168b-35fe-4008-9a30-a379cc16ba56	\N	\N	2024-07-20 00:00:00	TXN-2024-010	HYDRANGEA-002	Cascade Blue Hydrangea	Flowering Shrubs	Secondary	USD	18000.00	16200.00	1200.0000	15.00	\N	\N	2025-11-11 23:19:10.185801
a43fdfda-2566-4d02-86b2-7a104736a1d5	\N	\N	2024-08-10 00:00:00	TXN-2024-011	HOSTA-002	Emerald Crown Hosta	Perennials	Primary	USD	12000.00	10800.00	1500.0000	8.00	\N	\N	2025-11-11 23:19:10.212725
42681f72-d84e-4794-b068-d5f3d82a1fac	\N	\N	2024-09-05 00:00:00	TXN-2024-012	ROSE-003	Pacific Sunset Rose	Flowering Shrubs	Secondary	USD	16000.00	14400.00	1000.0000	16.00	\N	\N	2025-11-11 23:19:10.236564
5e7c806e-f266-4f96-bd99-fb745a4d330d	\N	\N	2024-10-15 00:00:00	TXN-2024-013	MAPLE-004	Aurora Flame Maple	Ornamental Trees	Primary	USD	35000.00	31500.00	2000.0000	17.50	\N	\N	2025-11-11 23:19:10.25975
e7bf2fbc-6285-4f8f-967c-314ae02bfcf6	\N	\N	2024-11-20 00:00:00	TXN-2024-014	JUNIPER-003	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	8000.00	7200.00	1600.0000	5.00	\N	\N	2025-11-11 23:19:10.281757
20d96287-609c-43fe-8a4b-4b9730e21b01	\N	\N	2024-12-10 00:00:00	TXN-2024-015	HOSTA-003	Emerald Crown Hosta	Perennials	Secondary	USD	20000.00	18000.00	750.0000	26.67	\N	\N	2025-11-11 23:19:10.304851
c5f3d178-2b46-47d3-892b-38df2272e16e	\N	\N	2024-03-20 00:00:00	TXN-2024-002	MAPLE-002	Aurora Flame Maple	Ornamental Trees	Primary	USD	25000.00	22500.00	1100.0000	22.73	\N	\N	2025-11-12 15:26:53.408266
f40694ec-7240-414b-b01b-4d56bfd1c127	\N	\N	2024-10-05 00:00:00	TXN-2024-003	JUNIPER-001	Golden Spire Juniper	Ornamental Shrubs	Secondary	USD	28000.00	25200.00	1800.0000	15.56	\N	\N	2025-11-12 15:26:53.444448
26b9b1ff-72b6-4803-b41c-7008cbd30c70	\N	\N	2024-04-12 00:00:00	TXN-2024-004	ROSE-001	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	12000.00	10800.00	3000.0000	4.00	\N	\N	2025-11-12 15:26:53.468819
71c2ea66-33b5-4d53-9bab-9ebe4883912e	\N	\N	2024-09-18 00:00:00	TXN-2024-005	HOSTA-001	Emerald Crown Hosta	Perennials	Primary	USD	18000.00	16200.00	900.0000	20.00	\N	\N	2025-11-12 15:26:53.493576
5ebf150b-0d9f-4ec0-8c14-4e659851e91e	\N	\N	2024-03-25 00:00:00	TXN-2024-006	HYDRANGEA-001	Cascade Blue Hydrangea	Flowering Shrubs	Primary	USD	120000.00	108000.00	20000.0000	6.00	\N	\N	2025-11-12 15:26:53.517139
8e66496c-6785-48b1-8cb0-450796cc828e	\N	\N	2024-12-15 00:00:00	TXN-2024-007	ROSE-002	Pacific Sunset Rose	Flowering Shrubs	Primary	USD	5000.00	4500.00	250.0000	20.00	\N	\N	2025-11-12 15:26:53.546642
00981e77-6f5d-4328-a7a3-e596e3d0e671	\N	\N	2024-05-10 00:00:00	TXN-2024-008	MAPLE-003	Aurora Flame Maple	Ornamental Trees	Secondary	USD	15000.00	13500.00	3000.0000	5.00	\N	\N	2025-11-12 15:26:53.570836
c0d8cb2d-5413-4c74-804a-85f911ba07c3	\N	\N	2024-06-15 00:00:00	TXN-2024-009	JUNIPER-002	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	22000.00	19800.00	800.0000	27.50	\N	\N	2025-11-12 15:26:53.599843
75632eef-036a-4d20-8978-2152ae1e47a1	\N	\N	2024-07-20 00:00:00	TXN-2024-010	HYDRANGEA-002	Cascade Blue Hydrangea	Flowering Shrubs	Secondary	USD	18000.00	16200.00	1200.0000	15.00	\N	\N	2025-11-12 15:26:53.622989
65694773-12f0-4a2e-8200-5618a4a95585	\N	\N	2024-08-10 00:00:00	TXN-2024-011	HOSTA-002	Emerald Crown Hosta	Perennials	Primary	USD	12000.00	10800.00	1500.0000	8.00	\N	\N	2025-11-12 15:26:53.647894
adb8e4ec-749d-4b92-a3b5-04f98d234bbf	\N	\N	2024-09-05 00:00:00	TXN-2024-012	ROSE-003	Pacific Sunset Rose	Flowering Shrubs	Secondary	USD	16000.00	14400.00	1000.0000	16.00	\N	\N	2025-11-12 15:26:53.671994
dffdb8aa-3b63-4b12-b38b-509b0d99e4d6	\N	\N	2024-10-15 00:00:00	TXN-2024-013	MAPLE-004	Aurora Flame Maple	Ornamental Trees	Primary	USD	35000.00	31500.00	2000.0000	17.50	\N	\N	2025-11-12 15:26:53.703039
d6f51839-9f0f-450d-b2bd-ac7be1bdab22	\N	\N	2024-11-20 00:00:00	TXN-2024-014	JUNIPER-003	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	8000.00	7200.00	1600.0000	5.00	\N	\N	2025-11-12 15:26:53.728726
1cb48fc5-7579-4769-a704-271e43ea44c6	\N	\N	2024-12-10 00:00:00	TXN-2024-015	HOSTA-003	Emerald Crown Hosta	Perennials	Secondary	USD	20000.00	18000.00	750.0000	26.67	\N	\N	2025-11-12 15:26:53.753237
6be79a23-66e9-4252-8dbf-91c081311b96	\N	\N	2024-02-15 00:00:00	MFG-2024-001	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	3825000.00	0.00	45000.0000	85.00	\N	\N	2025-11-12 15:25:54.963329
6388da7a-2341-4e3c-add8-7ab707bb1ef2	\N	\N	2024-02-28 00:00:00	MFG-2024-002	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	4750000.00	0.00	38000.0000	125.00	\N	\N	2025-11-12 15:25:54.995192
97fd965f-28b4-4224-aabe-94bfbeea5d3d	\N	\N	2024-03-12 00:00:00	MFG-2024-003	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	4940000.00	0.00	52000.0000	95.00	\N	\N	2025-11-12 15:25:55.018905
f278f562-0855-421d-aba7-73284fdfd38a	\N	\N	2024-03-28 00:00:00	MFG-2024-004	\N	Smart Material Component SM-100	Automotive transmission components	Mexico	USD	5180000.00	0.00	28000.0000	185.00	\N	\N	2025-11-12 15:25:55.059764
1eb2c511-78be-4a07-b9ea-841b01fac486	\N	\N	2024-04-10 00:00:00	MFG-2024-005	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	3750000.00	0.00	15000.0000	250.00	\N	\N	2025-11-12 15:25:55.085169
b7271fa6-3383-4088-b883-e5ef53be633f	\N	\N	2024-04-25 00:00:00	MFG-2024-006	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	4200000.00	0.00	3500.0000	1200.00	\N	\N	2025-11-12 15:25:55.133398
3e37cf47-7e48-48e0-bac0-f2d92f7956d7	\N	\N	2024-08-01 00:00:00	MFG-2024-013	\N	Composite Bearing CB-500	Automotive transmission components	United States	USD	6150000.00	0.00	75000.0000	82.00	\N	\N	2025-11-12 15:25:55.339389
0e8a431b-8dba-4159-aeee-1114253c73c5	\N	\N	2024-08-18 00:00:00	MFG-2024-014	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	6710000.00	0.00	55000.0000	122.00	\N	\N	2025-11-12 15:25:55.431765
aa5b6726-e0c0-4f56-bbf9-6488114c01fe	\N	\N	2024-09-05 00:00:00	MFG-2024-015	\N	Precision Molded Component PM-350	Automotive transmission components	Canada	USD	6256000.00	0.00	68000.0000	92.00	\N	\N	2025-11-12 15:25:55.463546
a00fcdb2-4d84-4f66-ab07-6d1b712d909e	\N	\N	2024-09-20 00:00:00	MFG-2024-016	\N	Smart Material Component SM-100	Industrial machinery parts	United States	USD	6240000.00	0.00	32000.0000	195.00	\N	\N	2025-11-12 15:25:55.486124
2374dedb-5455-4b5c-a548-8ca4995812b9	\N	\N	2024-10-08 00:00:00	MFG-2024-017	\N	Industrial Bearing IB-750	Industrial machinery parts	United States	USD	5830000.00	0.00	22000.0000	265.00	\N	\N	2025-11-12 15:25:55.509159
c4257e45-3402-47d2-a5ab-9ee5a3da0659	\N	\N	2024-10-22 00:00:00	MFG-2024-018	\N	Aerospace Composite AC-1000	Aerospace-grade composite materials	United States	USD	5376000.00	0.00	4200.0000	1280.00	\N	\N	2025-11-12 15:25:55.534108
f54392f8-ec75-4c52-81fd-d5cecaa1da01	\N	\N	2024-11-05 00:00:00	MFG-2024-019	\N	Composite Bearing CB-500	Automotive transmission components	Mexico	USD	6560000.00	0.00	82000.0000	80.00	\N	\N	2025-11-12 15:25:55.557761
97d69dd6-cc45-4695-8440-b67c199447b3	\N	\N	2024-11-20 00:00:00	MFG-2024-020	\N	High-Temp Polymer Coating HT-200	Automotive transmission components	United States	USD	7680000.00	0.00	64000.0000	120.00	\N	\N	2025-11-12 15:25:55.580565
a21eb4a5-0aad-4dcd-9223-1022f89b8168	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-08-10 00:00:00	TXN-2024-011	HOSTA-002	Emerald Crown Hosta	Perennials	Primary	USD	12000.00	10800.00	1500.0000	8.00	\N	\N	2025-11-12 15:37:10.606508
04736c11-e14d-4a67-a7cb-d9172657c0ed	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-09-05 00:00:00	TXN-2024-012	ROSE-003	Pacific Sunset Rose	Flowering Shrubs	Secondary	USD	16000.00	14400.00	1000.0000	16.00	\N	\N	2025-11-12 15:37:10.629165
f293a77a-6023-44d8-85f8-af96e607a620	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-10-15 00:00:00	TXN-2024-013	MAPLE-004	Aurora Flame Maple	Ornamental Trees	Primary	USD	35000.00	31500.00	2000.0000	17.50	\N	\N	2025-11-12 15:37:10.651774
332e4927-6958-4381-b0ec-a3af508c88c1	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-11-20 00:00:00	TXN-2024-014	JUNIPER-003	Golden Spire Juniper	Ornamental Shrubs	Primary	USD	8000.00	7200.00	1600.0000	5.00	\N	\N	2025-11-12 15:37:10.674327
969f398d-a738-4811-8432-76e4b17ca68c	46d0e351-4296-45bb-a680-9ee65497967b	\N	2024-12-10 00:00:00	TXN-2024-015	HOSTA-003	Emerald Crown Hosta	Perennials	Secondary	USD	20000.00	18000.00	750.0000	26.67	\N	\N	2025-11-12 15:37:10.695773
\.


--
-- Data for Name: sales_field_mappings; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.sales_field_mappings (id, contract_id, source_field_name, target_field_type, mapping_confidence, mapping_method, sample_values, approved_by, approved_at, usage_count, last_used_at, created_at) FROM stdin;
\.


--
-- Data for Name: semantic_index_entries; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.semantic_index_entries (id, contract_id, index_type, source_id, content, embedding, metadata, created_at) FROM stdin;
\.


--
-- Data for Name: session; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.session (sid, sess, expire) FROM stdin;
OewWEaCeXBXIk6ptS7_oACT-wzS_eKPe	{"cookie":{"originalMaxAge":604800000,"expires":"2025-11-18T23:14:23.957Z","secure":false,"httpOnly":true,"path":"/"},"passport":{"user":"993f41bb-59e7-4a3a-ad0d-25e808ca81c7"}}	2025-11-19 17:28:15
gYmvwCesGzcHQVYnuvMPj6JPgu_P97ZB	{"cookie":{"originalMaxAge":604800000,"expires":"2025-11-10T02:31:49.298Z","secure":false,"httpOnly":true,"path":"/"}}	2025-11-13 02:58:31
\.


--
-- Data for Name: sessions; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.sessions (sid, sess, expire) FROM stdin;
-clxiaWRzZbuaIKB8ylIs0xX7wMH64LY	{"cookie": {"path": "/", "secure": true, "expires": "2025-09-12T14:00:20.409Z", "httpOnly": true, "originalMaxAge": 604800000}}	2025-09-12 14:10:25
\.


--
-- Data for Name: strategic_analysis; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.strategic_analysis (id, contract_id, strategic_value, market_alignment, competitive_advantage, risk_concentration, standardization_score, negotiation_insights, benchmark_comparison, recommendations, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: system_embeddings; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.system_embeddings (id, document_id, category, title, source_text, embedding, metadata, created_at, updated_at) FROM stdin;
b0b697cc-10bb-4d73-b96a-52cdce395549	platform-overview	Platform Overview	What is LicenseIQ?	LicenseIQ Research Platform is a comprehensive SaaS web application designed for intelligent contract management and analysis. \n    \nThe platform specializes in automating contract processing, risk assessment, and compliance checks using AI technology. It offers features like automated payment calculations, dynamic rule engines, and a RAG-powered Q&A system (LIQ AI).\n\nKey benefits:\n- Reduces manual effort in contract analysis\n- Provides actionable insights from complex agreements\n- Ensures revenue assurance for industries with complex licensing agreements\n- Uses an "AI-Native" architecture with 100% FREE AI services (Groq API for LLaMA models, HuggingFace for embeddings)\n\nLicenseIQ is particularly valuable for organizations managing multiple licensing agreements across different territories, products, and pricing structures.	[-0.036611598,0.0020865893,-0.0779792,-0.024952427,0.025111387,-0.036246203,0.0325897,0.055808943,0.03387322,0.018204512,-0.008260961,-0.006008785,0.059228633,-0.019881327,0.008049041,-0.07196502,-0.01857166,0.0038440453,0.027704082,0.009479471,-0.021057345,-0.015760437,-0.042179927,-0.056502294,0.0046205465,0.05620239,-0.053104445,-0.0058537475,-0.0577038,-0.14885859,-0.03710854,0.026511943,0.07786728,-0.0039579873,0.03644259,0.0135529,-0.045337524,0.022692345,-0.01977245,-0.020557597,0.059852477,-0.0046622776,-0.0069324584,-0.029764444,0.043839667,-0.069254346,0.041742265,-0.009176625,-0.08307502,0.032122195,-0.004599348,-0.06839823,-0.030721173,0.021767857,-0.066086486,0.0026455617,0.061248127,0.060225934,0.022442978,0.004316089,0.013241226,0.008675021,-0.087924674,0.11020723,-0.020421142,0.055606466,-0.030102802,-0.06783183,-0.020173958,0.029644378,0.03391355,-0.015201748,-0.074287556,0.008360792,0.07494531,0.04060319,0.016438486,-0.0123095475,0.010167158,0.034100756,-0.029611528,-0.079656035,0.008152289,0.005496771,-0.07776126,-0.03766145,-0.0037489694,-0.02711809,0.05640593,-0.005881687,0.0557384,-0.015680669,0.027532596,0.006793082,-0.005489136,0.02621232,-0.06400048,-0.034017313,-0.016065642,0.33595914,0.004377139,0.018816045,0.0047519407,-0.05613295,-0.021498596,0.01287437,-0.05746142,0.0021760082,-0.0018204154,0.0043436973,-0.021621501,0.036250923,0.021484312,-0.009284615,-0.011353043,0.0037643148,-0.062299646,0.090236075,0.02750522,0.01762309,-0.025733657,0.040288653,0.003274328,0.064830765,-0.032561183,0.0051133516,0.031574663,0.05846259,0.013553547,-0.01553249,0.0539522,-0.009383897,-0.03260575,-0.041142117,0.07565421,-0.006971579,-0.06754191,0.052401643,-0.032610595,0.053379647,-0.053749204,0.06147867,0.042354003,-0.03404404,-0.07405405,0.0473563,0.041924085,0.05037839,-0.06212517,-0.006533684,0.023028392,0.010230273,0.010120355,-0.0011565895,0.051114894,0.035039335,0.12001659,0.031306293,-0.048967943,0.031644646,0.014753385,-0.029276075,-0.013635809,0.14503792,0.023662494,-0.15755858,0.024026819,-0.01880408,-0.077511236,-0.007379926,0.01898987,0.039292585,-0.060518116,0.017466806,-0.038640458,-0.056059446,-0.024517903,0.037049994,0.031076498,0.040022917,-0.030686297,-0.030950157,-0.06785091,-0.028567266,-0.021112956,-0.011959461,-0.03572146,-0.012747655,0.003832922,-0.027891884,-0.027186325,0.040468834,-0.028221434,0.019835314,0.037410762,0.024199383,-0.022011396,-0.019994343,0.0025291336,-0.032731883,0.022083495,0.09109325,-0.017960139,-0.007460651,0.02199052,0.016059913,0.021467015,-0.0045622713,0.008202712,0.040595844,-0.01975127,0.058357745,-0.054787505,0.03891415,-0.031424455,0.044928834,0.053705487,-0.029939173,-0.01904555,-0.0018411145,-0.06398792,0.06159754,0.014751302,-0.3009515,0.024428386,-0.060957767,0.058156524,0.01999846,-0.06142534,-0.033999708,0.053576276,0.0049242526,0.0704181,0.08641264,0.032712657,-0.0004983315,0.018042663,-0.05096622,0.01453408,-0.04538287,-0.016449856,-0.04176349,0.038522746,-0.032558452,0.04046007,-0.027970057,-0.05763706,0.11547013,-0.012776373,0.08716532,-0.051465888,0.063907355,0.022015458,0.010132073,0.014572718,-0.0015132712,-0.13218783,-0.012565903,-0.062428296,-0.0134178335,-0.0046319035,-0.06350516,-0.014266298,-0.070386134,-0.017526694,0.038058,-0.10056385,0.010613475,0.009515477,0.035896797,0.039760787,-0.04630242,0.0780085,0.007059291,0.041899487,0.03828322,0.06288083,-0.006396941,0.042205367,-0.010467876,0.0052749985,0.0060248957,-0.013110413,0.068298444,-0.037390526,0.05046276,-0.0065948837,0.040474106,-0.058821518,0.07486408,0.0029917106,-0.0017127852,-0.03800398,-0.021305434,0.104930624,0.012627766,-0.00054765365,0.0286511,-0.015179231,0.04950083,-0.0511451,0.003854933,0.036597017,0.030449722,-0.018444529,0.07540825,-0.002889356,0.028529542,0.071222894,-0.0051411535,0.0053827134,-0.004476322,0.0325519,0.026725031,-0.0016468494,-0.04487237,-0.0016934427,0.0015971744,-0.02982011,-0.24446489,0.018650344,-0.053530738,0.022787932,-0.018720597,-0.02914058,0.009664722,-0.08307499,0.0040041115,-0.011065501,-0.01498898,0.07898286,-0.022810621,-0.08202945,-0.004220225,-0.052815072,0.10947633,-0.034486786,-0.017703658,0.032146912,0.0020490861,0.014292032,0.13706963,0.0042206873,0.039621703,0.03192211,-0.038937736,-0.030600958,0.016036665,-0.00020431208,-0.020294396,-0.015554728,0.043429706,-0.003843792,-0.052319076,0.048059817,-0.05448779,0.02620385,-0.05949329,0.02149369,-0.010606645,-0.028184483,0.016546933,0.033669677,0.023540253,0.03805811,-0.08362153,-0.03485561,0.009991862,0.040872447,-0.01947323,-0.082858406,-0.035566844,0.020639388,-0.014176663,-0.03192469,0.08963198,0.011462823,-0.019791255,-0.054906193,0.015990015,-0.015351978,0.033558022,0.021722006,0.004048308]	{"title": "What is LicenseIQ?", "category": "Platform Overview"}	2025-11-11 22:43:36.327694	2025-11-11 22:43:36.327694
f35b2321-15b0-4003-a5d7-90308bf2884c	contract-types	Contract Types	What contract types does LicenseIQ handle?	LicenseIQ supports a wide range of contract types through its universal contract processing system:\n\n1. **Licensing Agreements**\n   - Technology licenses\n   - Patent licenses\n   - Trademark licenses\n   - Software licenses\n   - Content licensing\n\n2. **Manufacturing Contracts**\n   - Production agreements\n   - Supply agreements\n   - Distribution agreements\n\n3. **Electronics & Semiconductor**\n   - Patent portfolio licenses\n   - Component royalty agreements\n   - IP licensing\n\n4. **Plant Variety & Agriculture**\n   - Seed licensing\n   - Plant variety royalties\n   - Agricultural technology licenses\n\n5. **Service Contracts**\n   - Consulting agreements\n   - Professional services\n   - Maintenance contracts\n\n6. **Payment & Fee Structures**\n   - Percentage-based royalties (license fees)\n   - Fixed fees\n   - Volume-tiered pricing\n   - Minimum guarantees\n   - Caps and thresholds\n   - Seasonal adjustments\n   - Territory-based premiums\n\nThe system uses AI-powered zero-shot extraction to automatically identify contract types and extract relevant terms, making it adaptable to any contract structure.	[-0.067279406,-0.0063991235,-0.07259741,-0.06547992,0.048648376,-0.030978875,0.037572455,0.02588543,-0.017352998,0.05207458,0.04468734,0.011971571,0.030676326,-0.06075983,0.015472885,-0.05214339,0.019341817,0.0073543857,-0.041278884,-0.0054490934,0.0041293013,-0.044174604,-0.053456023,0.008381824,0.02011163,0.025400477,-0.019322852,-0.037194923,0.0056319437,-0.19137868,-0.026132748,0.08638095,-0.016652644,0.023684518,0.052913822,-0.009149872,-0.09234012,-0.050392892,-0.022960557,-0.05510901,0.05277586,0.01194646,-0.021077618,-0.04919555,0.015095977,-0.0035086637,0.021587828,-0.012251501,-0.03464172,-0.017133567,-0.016265718,9.115688e-05,0.02178174,0.045178827,-0.082652524,-0.02035399,0.09352532,0.082566895,-0.005006465,-0.024194313,0.023517128,-0.017414512,-0.0856884,0.104624756,-0.0056543145,0.07210319,-0.04640896,-0.021676585,-0.036718033,0.076537535,0.015232652,0.011908811,-0.15324111,-0.021520982,-0.018735461,0.013107529,0.051423922,-0.03556374,-0.022943515,-0.011774836,-0.05514159,-0.091377564,-0.061958376,-0.0014299589,-0.06487446,-0.002188568,-0.013021264,0.025957521,0.0811842,-0.0016005945,0.019148106,-0.0072263447,0.02801142,-0.013762649,-0.01677484,0.012689803,-0.02789327,-0.059738036,0.047576576,0.31766593,0.031658042,-0.027650462,0.035600618,-0.057815745,-0.019346379,0.057924088,-0.07486731,-0.031044792,-0.0040866286,-0.008769149,0.009738601,0.043226242,0.021565065,0.005925046,-0.033676285,-0.020770708,-0.05402688,0.04147955,0.033990834,-0.028137323,-0.070680425,0.029840002,-0.05824764,0.05993202,-0.029865244,0.044222064,0.048666958,0.043483578,-0.009607712,0.03053077,0.0127679445,-0.015333502,-0.04890222,-0.018232815,0.054270685,0.040856708,0.00781328,0.032303926,-0.018154817,0.036234833,-0.019474735,0.03285525,0.040445954,-0.043457195,-0.06771702,0.099647656,0.013159589,0.100734435,-0.030403316,-0.017959971,-0.0050947727,0.050171666,-0.0058792876,-0.015422715,0.053404137,0.07629281,0.09828986,-0.02870659,-0.05092458,0.049269218,0.023934672,-0.0027239493,-0.003217014,0.15102357,0.07684532,-0.14965999,0.013810121,-0.05988197,-0.04629638,-0.034087326,-0.04848496,0.005966135,-0.020839153,0.0035676954,0.0033857631,-0.002560513,-0.06830458,-0.043716956,0.040560324,0.015800897,-0.022068197,-0.011883295,-0.012100646,-0.04559689,0.030968273,0.005971141,-0.05132905,0.024268704,0.03479674,-0.010381367,-0.029284773,-0.010173863,-0.050903324,0.061463237,0.02855855,0.038295448,-0.04388785,-0.020771531,0.03718066,-0.061795022,0.0431175,0.09640663,-0.021280512,0.0015678643,0.0256717,0.01973555,-0.0017274487,-0.008640493,-0.0018063916,0.014419579,-0.0006314348,0.06126125,-0.003629215,0.010566336,-0.07073308,0.015921757,0.05455941,0.0051288973,-0.0047250036,0.02908332,-0.015991967,0.06624117,0.011753944,-0.3014962,-0.024670977,-0.024063418,0.009977486,-0.0038256543,-0.052506518,-0.061295602,0.02110444,-0.032398526,0.07270855,0.06462987,0.012203951,0.0049310382,-0.013782596,-0.004799432,0.048557904,-0.021406323,-0.036321834,-0.022970712,0.000984845,-0.024281014,0.016582938,-0.06788239,-0.010908843,0.1169572,0.003255436,0.07567391,-0.10772044,0.0701308,0.0075974306,0.012301779,-0.0027168703,0.00046355432,-0.05854824,0.014892246,-0.03799019,-0.006055805,-0.025896214,0.0110008465,0.024762845,-0.027142735,0.00017943776,0.028474165,-0.04102982,0.047729664,-0.010468182,-0.00093104376,0.029566929,-0.050548043,0.052345816,-0.027212385,0.04406383,-0.01235442,0.053198818,-0.019571377,0.028055578,-0.022794375,-0.015284494,-0.009339914,0.010756816,0.06483534,-0.059662115,0.030653028,0.0209513,0.07044007,-0.043770216,0.03447208,0.028216053,-0.0066824416,-0.03259569,0.007924615,0.05497048,-0.0069208625,-0.013410564,0.015566192,-0.0070187696,0.035528537,-0.0057960902,-0.010629461,0.01583084,0.0054662335,0.004018442,0.09252697,0.02774798,0.00468085,0.077867515,0.03369933,0.001896486,0.024756052,0.00774809,0.032324567,0.019391073,-0.024017558,0.032238044,0.0672187,-0.0037414199,-0.22465612,0.032470703,-0.04748922,-0.030063208,-0.010948051,-0.039694495,0.0015934735,-0.076550916,-0.008916314,0.0019002464,0.050840426,-0.015444121,-0.013406518,-0.03975248,-0.031038929,-0.016023895,0.11365649,-0.03978002,0.018485805,-0.007236362,0.04048579,0.06701071,0.16567042,0.03974677,0.022601072,-0.031328022,-0.053029217,-0.03368956,-0.024067974,0.020466132,0.042842906,0.008581242,0.034380306,-0.022121428,-0.020505594,0.056940872,-0.008273974,0.014638237,-0.055575594,0.026691142,-0.033463206,-0.031971965,-0.08347479,0.017850542,-0.010274628,0.024787333,-0.08346258,-0.048488643,-0.028440261,0.089275464,-0.014159039,-0.033319134,-0.041458488,0.028771482,0.015990496,-0.0016685473,0.07040659,0.028389238,-0.031666055,0.001147118,-0.0040329145,-0.0018621805,-0.0403337,0.09290686,-0.0036665136]	{"title": "What contract types does LicenseIQ handle?", "category": "Contract Types"}	2025-11-11 22:43:36.553033	2025-11-11 22:43:36.553033
35ba7bf6-524f-47db-8d86-d807bd3d3dc6	core-features	Features	What are the main features of LicenseIQ?	LicenseIQ offers comprehensive features for contract lifecycle management:\n\n**AI-Powered Analysis:**\n- Automated contract reading and term extraction using Groq LLaMA models\n- Risk assessment and compliance checking\n- Confidence scoring for AI extractions\n- Multi-source contract ingestion (PDF, DocuSign, contract management systems)\n\n**License Fee Management:**\n- Dynamic rule engine for payment calculations\n- Support for percentage, fixed fee, tiered pricing, minimum guarantees, and caps\n- Seasonal adjustments and territory premiums\n- Volume-based discounting\n- FormulaNode JSON expression trees for complex calculations\n\n**Payment Calculator Dashboard:**\n- Customizable calculations with run history\n- Professional PDF invoice generation\n- Centralized Royalty Calculator\n- Support for multiple pricing structures\n\n**LIQ AI Assistant (RAG-Powered Q&A):**\n- Omnipresent AI assistant accessible via floating button\n- Context-aware chat for contract inquiries\n- Source citations with confidence scores\n- Semantic search using vector embeddings\n\n**Master Data Management:**\n- Three-level company hierarchy (Company → Business Unit → Location)\n- Universal ERP catalog system supporting any ERP (SAP, Oracle, NetSuite)\n- LicenseIQ Schema Catalog for data consistency\n- AI-driven field mapping with dual schema auto-population\n\n**Security & Compliance:**\n- Five-tier Role-Based Access Control (RBAC)\n- Secure session management with PostgreSQL storage\n- Audit logging for all critical operations\n- Contract metadata versioning and approval workflows\n\n**Advanced Capabilities:**\n- AI sales data matching using semantic embeddings\n- Dynamic contract processing with knowledge graph construction\n- Flexible CSV import with 70+ field aliases\n- Mobile-responsive design with dark mode support	[-0.06726283,-0.009533132,-0.041672427,-0.03279023,0.0024065522,-0.035769977,0.044426575,0.05756807,0.0184835,0.018159548,-0.0043682796,0.03594962,0.016988004,-0.032211334,-0.0122285625,-0.05768585,0.011188414,0.010368396,0.016433572,0.02412921,-0.02601738,-0.016713317,-0.058514092,0.028141366,-0.005850391,0.03866957,-0.052047417,-0.019090535,-0.060791604,-0.20754385,-0.014424325,0.06254146,0.07067896,-0.012582896,0.02971876,0.018762788,-0.08354459,0.024839083,-0.008930772,-0.028284023,0.029058939,0.019362414,-0.018367548,-0.040488556,0.015842149,-0.066112064,0.078647874,0.00035617972,-0.058309145,0.03221536,0.0008656791,-0.043059368,-0.031580508,0.06489523,-0.012052969,0.017642567,0.10575356,0.07181989,-0.020662565,0.004984167,0.008609168,-0.000822685,-0.08216805,0.087109566,-0.04686966,0.06784956,-0.03650818,-0.042857718,-0.036900643,0.08490403,0.007720648,0.0056685135,-0.076918244,0.03430864,0.02280134,0.03246828,0.030666739,0.015926527,-0.0003284323,0.051515967,-0.023072591,-0.034080844,-0.0229833,-0.018942934,-0.07350358,-0.020279396,0.019311387,-0.008853768,0.044094447,-0.042939574,0.072302446,-0.015284055,-0.016292812,0.0026401137,-0.03676096,-0.0005175167,-0.0028539563,-0.005778761,0.017640453,0.3389139,0.041028824,0.0020107902,0.025078187,-0.080480814,-0.0087166475,-0.014754166,-0.02539699,-0.030835403,-0.017140776,0.018583834,-0.041527603,0.0335449,0.016484339,-0.04219973,-0.03594413,0.0050225304,-0.08127942,0.055124767,0.031452242,0.017173624,0.0050998894,0.0104029905,-0.008035655,0.063762315,-4.762173e-05,0.014449068,0.049791064,0.031300038,0.015454753,0.02379967,0.051858716,-0.021127393,-0.0451782,-0.010815292,0.087742686,-0.004051881,-0.035089247,0.04707128,-0.029451057,0.055789273,-0.098218516,0.07581879,0.04087361,-0.04164549,-0.09408919,0.10926203,0.063138805,0.07268055,-0.0077192,-0.010060863,-0.0012657742,0.04373595,0.033149473,-0.03538501,0.07021352,0.044893976,0.07119733,0.022049064,-0.05633178,0.030653656,-0.0032283876,-0.012383717,-0.02357643,0.16644411,0.00796393,-0.1666545,0.0061991587,-0.008516035,-0.07363825,0.016338529,-0.0259337,0.0057102884,-0.049277317,0.008086408,0.015600776,-0.012567783,-0.020912351,-0.0032188732,0.045442164,0.033306412,-0.005749752,-0.015121108,-0.06270589,-0.04320233,-0.006541628,-0.023140946,-0.053796846,-0.006177723,0.0081702,-0.011328673,0.019653166,0.03595923,-0.004744487,0.06940274,0.02364602,0.018764693,-0.057972163,0.026141174,-0.012164299,-0.05881025,0.033370327,0.082623705,0.0024698814,-0.04608549,0.014710781,-0.02438873,0.018096998,0.0037774155,0.012952734,0.004636285,-0.021767346,0.06738533,-0.06239567,0.024054376,-0.017219316,0.037942026,0.052040722,0.012075481,-0.04416005,0.035203654,-0.018122425,0.096317686,0.02315706,-0.32122225,0.029381666,-0.062383126,0.0019806793,-0.0067457766,-0.0961706,-0.024245966,0.04734159,0.00855691,0.05025195,0.06579331,0.007233468,-0.012797913,-0.042808108,-0.0010798571,0.04150969,-0.05442518,-0.007450337,-0.023534413,0.04430153,-0.01853461,0.019980414,-0.03282623,-0.035192527,0.089761496,-0.014912313,0.070573725,-0.118705705,0.013479534,-0.0017569907,-0.0046695033,-0.006634382,-0.029743155,-0.09403579,0.047957473,-0.062799565,-0.026971526,-0.013961532,-0.029639626,0.017559122,-0.026774326,0.013441829,0.030775348,-0.07295678,-0.009375193,-0.018312432,0.00957506,0.019280415,-0.057748873,0.036173303,0.013404798,0.024461757,0.04454907,0.04771065,0.023648227,-0.004984853,-0.025146607,-0.0010669956,-0.034044225,-0.019997694,0.059294533,-0.061930563,0.039999332,0.0009926857,0.021239938,-0.069969915,0.031149687,-0.008749573,0.011612492,-0.05929033,-0.059525717,0.057000224,-0.010741381,-0.024263414,0.04568429,-0.020106282,0.06420946,-0.038990043,-0.037209086,0.008676397,0.018663755,-0.007942763,0.06859914,0.012463886,0.01125122,0.071413495,0.015526817,0.003875767,0.0014030071,0.044833045,-0.009766922,-0.017935349,-7.628699e-05,-0.023868823,0.04105506,0.019515349,-0.23199482,0.012704485,-0.08155648,0.02056009,-0.013191937,0.015987065,-0.001602177,-0.06292489,-0.002221833,-0.036631856,0.018515242,0.04755076,-0.0029818327,-0.074202985,-0.013237139,-0.028319743,0.07935967,-0.038069647,-0.0005568576,0.052083515,0.031575114,0.022233853,0.14056695,0.00058392185,0.0076955915,0.02245698,-0.04283453,0.0021178608,0.028432878,0.0068659647,-0.004635321,-0.0019079756,0.06521855,0.019616922,-0.02666204,0.065081775,-0.031892493,0.020230833,-0.046923187,0.04361573,0.010577631,-0.022738956,-0.032832947,-0.0009301667,0.0018426689,-0.015058327,-0.07779914,-0.021794634,0.014063622,0.035386093,-0.036678188,-0.06865659,-0.03791179,0.034312777,0.025450088,-0.002325475,0.059933107,0.018615372,0.0041601006,-0.064924724,0.017661717,-0.017011568,0.041004814,0.07203767,-0.022458373]	{"title": "What are the main features of LicenseIQ?", "category": "Features"}	2025-11-11 22:43:36.784176	2025-11-11 22:43:36.784176
d4640e6a-d44f-48a2-8efd-b616dd969fce	ai-services	Technology	What AI services does LicenseIQ use?	LicenseIQ uses 100% FREE AI services to keep costs minimal:\n\n**Groq API (Primary LLM):**\n- Model: LLaMA 3.1 70B and 8B variants\n- Use cases: Contract analysis, term extraction, rule identification, Q&A responses\n- Features: Fast inference, high accuracy, JSON output support\n- Cost: Completely FREE\n\n**HuggingFace Inference API (Embeddings):**\n- Model: BAAI/bge-small-en-v1.5\n- Dimensions: 384-dimensional vectors\n- Use cases: Semantic search, contract matching, sales data alignment\n- Storage: PostgreSQL with pgvector extension\n- Index: HNSW (Hierarchical Navigable Small World) for fast similarity search\n- Cost: Completely FREE\n\n**Architecture Benefits:**\n- No API costs for core functionality\n- Asynchronous processing for better UX\n- Scalable semantic search capabilities\n- Real-time contract analysis\n- Context-aware AI assistance\n\n**Data Flow:**\n1. Contract uploads are processed by Groq for extraction\n2. Text chunks are embedded using HuggingFace\n3. Embeddings stored in PostgreSQL with pgvector\n4. Queries use vector similarity search\n5. LIQ AI generates answers using retrieved context + Groq LLaMA	[-0.06524508,0.011853407,-0.06974072,-0.027057763,0.033161644,-0.040162273,-0.011605338,0.02923159,0.046196826,-0.007853922,-0.013011047,-0.005806071,0.048889045,-0.026684541,0.007577175,-0.07932872,0.02146601,-0.011030136,-0.0040347315,0.00570884,-0.015631648,-0.06247728,-0.032831002,-0.035992227,-0.03109054,0.07184489,-0.056363042,0.038112804,-0.0032881063,-0.16684487,-0.0025875913,0.028356425,0.060086045,0.021118732,-0.0036476476,0.056421068,-0.020123439,-0.011709073,-0.051925026,0.010105678,0.04214916,-0.005694629,-0.022238785,0.0023918673,0.032654554,-0.03818795,0.0038837048,0.011239257,-0.07762596,0.035907555,-0.030507436,-0.016061008,-0.055459265,0.02978121,-0.015644431,0.0051132124,0.07342299,0.084376805,-0.0218993,-0.009634269,0.014969163,-0.023370814,-0.08634593,0.11306348,-0.04087902,0.06449698,0.019624844,-0.05653287,0.0030806954,0.048713308,-0.01516618,-0.0035930974,-0.036126085,-0.009890854,0.05414626,0.054680888,0.0037698003,-0.0035254625,0.03816337,0.03582878,-0.040431116,-0.057256777,0.012811245,0.013599842,-0.06366081,-0.016734632,-0.019777304,0.0037615774,0.025399074,-0.074973874,0.051003605,-0.040221583,0.00597902,0.048842512,-0.03360606,0.028605325,-0.03289102,0.0006177403,-0.035446357,0.35034332,0.005277171,0.0037232575,-0.03244002,-0.050402552,-0.0068480545,-0.00768093,-0.04990381,-0.03696468,-0.0039046847,0.04205827,-0.05069529,0.04030914,0.056351505,-0.023045817,-0.043343574,-0.018527912,-0.029482482,0.0010079626,-0.012118037,0.016970849,-0.05520458,0.0117962295,-0.022143323,0.0630596,0.006723618,0.011699619,0.006017073,0.04967943,0.026965465,0.016394284,0.048291717,-0.06417694,-0.022810921,-0.019711755,0.080691986,-0.025556706,0.054044645,0.053199463,-0.04265559,0.05731719,-0.03526932,0.03950315,0.03534744,0.0066063073,-0.061458327,0.10657712,0.025130663,0.07651823,-0.03119648,-0.049408045,-0.014673298,0.066581294,-0.0018707301,-0.021361917,0.052429777,0.021935884,0.08500301,0.019281615,-0.08180412,0.036406998,-0.013904962,-0.044970594,0.013727031,0.10663936,-0.024712315,-0.13506764,-0.040531054,-0.019958902,-0.08140655,-0.008060111,0.017734503,0.062300034,-0.04629395,-0.011498045,-0.019303992,0.0063395435,-0.048895545,0.032192517,0.010668547,0.061194483,-0.053454433,-0.05895935,-0.030940972,-0.017621275,-0.04213048,-0.012052119,-0.015485005,-0.014794726,0.022058992,-0.054972526,0.0064975573,0.008850597,0.0033854677,0.050233416,0.033751298,0.01749244,-0.024935424,0.03234097,0.01639714,-0.016493173,0.05734128,0.07757846,-0.011812731,-0.027204316,0.0074370895,-0.026731454,0.038504347,-0.01652684,-0.0011630639,0.03403272,-0.051234197,0.025463352,-0.078589,0.076920256,-0.07294443,0.0510449,0.039368954,-0.013898557,-0.045108512,0.036115255,-0.04961047,0.0435109,0.010692268,-0.325937,-0.014638083,-0.0705019,0.032865014,-0.0201066,-0.14538167,-0.008709961,0.038808204,-0.005078051,0.084859416,0.08228125,0.004130933,0.0015082103,0.009969154,-0.014787385,0.045623265,0.0155446995,-0.000983592,-0.019915752,0.028555037,0.0049436707,0.010519782,-0.004637351,-0.07081148,0.11684971,0.010902822,0.101067446,-0.07061612,0.024141481,-0.0350056,-0.0011639509,-0.022123234,-0.004985909,-0.0880719,0.019531375,-0.047371924,-0.004435499,0.008571927,0.00014365921,-0.017809883,-0.057222605,-0.00962649,0.036503762,-0.09842193,0.0031557302,-0.0037411114,0.023018338,-0.009194855,-0.061486717,0.051784106,-0.010724204,0.042278703,0.05799784,0.056300722,-0.06815257,0.0047250413,-0.03610969,-0.03286598,-0.032126427,0.0024699045,0.04177055,-0.0078490125,0.01988161,-0.03500781,0.044174347,-0.06622865,0.066288956,0.019016746,0.004935006,-0.007286016,-0.04098306,0.054431714,-0.018879818,0.018199308,0.047783166,-0.014576828,0.06705058,-0.02334811,-0.0374664,0.01414758,0.034724243,0.016750904,0.02722181,0.0035170938,0.021891845,0.102931574,0.07660744,0.038515713,0.0017672156,0.018371105,0.037145194,0.02453511,-0.01554671,-0.026132204,0.032228906,-0.03349294,-0.23124556,0.019445857,-0.010987893,0.0344658,-0.047767285,0.008151735,-0.013323406,-0.09975334,5.5762262e-05,-0.009519709,0.01771399,0.079222396,0.0110779805,-0.06138715,0.0021616346,-0.04806414,0.06529438,-0.022536732,-0.008768985,0.053374007,0.014704502,-0.006557717,0.17035685,0.005860207,0.006988608,0.01235129,-0.0063830656,-0.015346658,-0.0034451662,0.019723553,-0.02543733,0.0013455141,0.07231078,0.013645737,0.0035325275,0.030867508,-0.05944862,0.0031892299,-0.020683154,0.028066628,-0.017553745,0.012906251,-0.022923956,0.04656541,0.030717295,0.008825458,-0.09322633,-0.041918736,-0.03448442,0.053724647,-0.004846956,-0.07225321,0.0075180167,0.005157658,0.004628537,0.007531458,0.05568827,0.03560956,0.009207354,-0.032086834,0.056058846,-0.028188145,0.02903124,0.04756631,0.0028534727]	{"title": "What AI services does LicenseIQ use?", "category": "Technology"}	2025-11-11 22:43:37.012055	2025-11-11 22:43:37.012055
b6c114c4-9924-47bf-be5a-8f6756e5f4a1	terminology	Terminology	What terminology does LicenseIQ use?	LicenseIQ uses specific standardized terminology:\n\n**License Fee (NOT "Royalty"):**\nThe platform consistently uses "License Fee" instead of "Royalty" throughout the application for payment-related features. This includes:\n- License Fee Rules Management\n- License Fee Calculator\n- License Fee Calculations\n- Per-unit license fee rate\n\n**LIQ AI (NOT "Contract Q&A"):**\nThe RAG-powered AI assistant is called "LIQ AI" - a conversational assistant that can answer questions about:\n- Uploaded contract documents\n- The LicenseIQ platform itself\n- Platform features and capabilities\n- How to use various functions\n\n**Contract ID Format:**\nContracts are assigned unique IDs in the format: CNT-YYYY-NNN\n- Example: CNT-2025-001, CNT-2025-002\n- YYYY = Year\n- NNN = Sequential number (001, 002, etc.)\n\n**Master Data Hierarchy:**\n- Company (GRP_ID): Top-level organization\n- Business Unit (ORG_ID): Division or department\n- Location (LOC_ID): Physical site or region\n\nAll LicenseIQ Schema entity records must link to all three levels as mandatory foreign keys.	[-0.054065574,-0.009776832,-0.05014121,-0.03980267,-0.01956725,-0.034266155,0.06709914,0.015634492,0.05410978,0.03353885,-0.013027037,0.046699762,0.017166412,-0.04587393,0.008706599,-0.050660286,0.028008655,-0.042220682,-0.0013207167,-0.011670523,0.035828322,-0.03799744,-0.0071628997,0.020431515,-0.038510174,0.035429273,-0.04420831,-0.005722359,-0.054081745,-0.16708149,-0.009495772,0.067705296,0.029484894,0.020465758,0.007120249,-0.012714858,-0.05411926,0.03865194,-0.030136462,0.011287059,0.053802453,-0.007815893,0.0029384163,-0.01822679,0.049155667,-0.01850421,0.004914093,-0.019413646,-0.051974196,0.08129664,0.0021788122,-0.025823606,0.012294764,0.0029114343,-0.06354328,0.01536747,0.094987735,0.07255495,0.042695228,-0.011478957,0.0012783373,0.0034190752,-0.10427205,0.12973362,-0.03839412,0.034897838,-0.018468263,-0.058301527,-0.018452533,0.044578135,-0.004540052,-0.010533577,-0.10429549,0.049247894,0.05593193,-0.015101274,-0.016853407,-0.018564746,-0.0010249427,0.00046238097,-0.07358041,-0.07213299,-0.032597173,-0.06778735,-0.04432826,-0.021949416,0.009653094,0.0067095887,0.03943423,-0.07047707,0.04384759,-0.03381652,0.01402018,-0.003356871,-0.023844073,0.028070435,-0.06500695,0.002428038,-0.008146928,0.33943492,0.000572921,-0.005399582,-0.025310649,-0.01332813,-0.019420559,0.010359539,-0.047874175,-0.046345375,-0.027089464,0.03200335,-0.050415963,0.029242951,0.024038125,-0.033968445,-0.020297207,0.00733841,-0.053552985,0.028951209,0.009950439,-0.0035817698,0.015097836,-0.008876161,-0.043231167,0.08036051,-0.0414079,0.028629983,0.066419214,0.03450674,-0.0010701636,0.013683961,0.063004,-0.047670905,-0.058610044,-0.00809716,0.054951306,-0.018235078,0.014990153,0.05116102,-0.018164739,0.04861148,-0.04689199,0.02322102,-0.029971644,-0.0021731323,-0.080347784,0.09107333,-0.009806385,0.06172027,0.025987923,0.0023174018,0.023880275,-0.002903999,0.010654749,0.0105405655,0.02384117,0.020673709,0.13924514,0.018574843,-0.05186824,0.0016718479,0.036187768,-0.039480846,-0.015933994,0.14565723,0.02693232,-0.1332061,-0.06979283,0.0076200864,-0.052046094,-0.011603247,-0.032737687,-0.008534651,-0.03739287,0.043695908,0.03164449,-0.027740475,-0.04256657,0.029441332,-0.0063348543,0.04301484,-0.029798796,-0.07039978,-0.047474895,-0.027028546,-0.041015815,-0.019189792,-0.058963932,-0.008366373,0.048858367,0.010949393,-0.0067075086,0.023468127,-0.0106058335,0.06699703,-0.020455468,0.032856505,-0.040609423,-0.010328481,0.016578801,-0.056827396,0.06038815,0.10976811,0.05876113,-0.014406923,-0.008822737,-0.010557116,0.052007228,-0.05113791,0.017144801,0.026635943,-0.052275997,0.028287204,-0.05121246,0.029432379,-0.011980238,0.044431373,0.0084333895,-0.030314075,-0.045888305,0.013004298,-0.044502802,0.035889287,-0.036735807,-0.2964485,0.036632903,-0.046019003,0.024652185,0.027699275,-0.061755005,0.00944124,0.056548234,-0.038287405,0.074737236,0.058879267,-0.0021294556,-0.01884575,-0.051558368,-0.0018254211,0.08719181,-0.011477068,-0.00017069974,0.018525835,0.019396018,-0.021436611,0.021832433,-0.032368783,-0.044316787,0.10485326,0.05066723,0.096137434,-0.06701764,0.055083085,0.0091853095,0.033836,-0.047511555,-0.025223065,-0.08657338,0.007669647,-0.09036193,-0.0082407035,0.00016756827,0.004379658,-0.028324127,-0.05856245,0.008963734,0.022541838,-0.04753518,0.030797996,0.0210572,0.0018474025,0.014709661,-0.058907226,0.013175431,-0.036792498,0.027239298,0.0016697625,0.033181936,0.020327017,-0.027698658,-0.026208,-0.0077398494,0.0053849956,0.005553823,0.078509495,-0.045027412,0.06463478,0.003735357,0.043119855,-0.08163686,0.08354791,0.041485623,0.013735894,-0.012789099,-0.054496534,0.11177086,0.0055693327,-0.041622225,0.03435475,-0.03237397,0.049627207,-0.054077663,0.0048983446,0.017528737,0.021884356,0.022001768,0.07585615,0.010046094,0.049145807,0.0630683,-0.0050928066,0.034524314,0.03202494,-0.007474695,-0.052605823,0.023402225,0.021842472,0.04044763,-0.024529206,-0.053515024,-0.2583915,0.0564262,-0.027846124,0.018042572,0.017726546,0.0008702666,0.0146182,-0.08722033,-0.00078677625,0.0049242466,0.033498645,0.030925397,-0.005507618,-0.04921534,-0.00894056,-0.040492076,0.0868773,-0.0018269124,-0.004780228,0.038885504,0.0013027516,-0.011025098,0.1514654,0.01920214,0.036452483,-0.017706197,-0.020498559,0.0506766,0.04724704,0.024199987,-0.024613846,-0.021846665,0.07217354,0.0092539,-0.030280309,0.023037922,-0.015294358,0.006061642,-0.02363901,0.0121364985,0.0012485639,-0.00916194,-0.06105921,0.01711424,-0.012184874,0.039980732,-0.07854613,-0.0773141,-0.009678045,0.08907155,-0.021690838,-0.03563091,-0.013028859,0.013552299,0.022728184,0.007054717,0.04208632,0.026766917,-0.0051957695,-0.046067122,-0.026928397,0.00018527628,0.029646019,0.07978685,-0.0026678701]	{"title": "What terminology does LicenseIQ use?", "category": "Terminology"}	2025-11-11 22:43:37.230129	2025-11-11 22:43:37.230129
100853c4-1b0a-411b-b1a1-1911305b91d9	data-management	Data Management	How does LicenseIQ handle data management?	LicenseIQ implements comprehensive data management:\n\n**Multi-Tenant Architecture:**\n- Mandatory 3-level company hierarchy (Company → Business Unit → Location)\n- All records link to GRP_ID, ORG_ID, and LOC_ID as foreign keys\n- Supports multi-entity operations\n\n**ERP Integration:**\n- Universal ERP Catalog System supporting any ERP platform\n- Frontend-managed ERP configuration\n- Dynamic catalog management\n- Pre-configured support for SAP, Oracle, NetSuite\n\n**LicenseIQ Schema Catalog:**\n- 28 standard schema entities for data consistency\n- Mirrors ERP Catalog architecture\n- Provides standardized field definitions\n- Ensures data quality across the platform\n\n**AI Master Data Mapping:**\n- AI-driven field mapping between ERPs and LicenseIQ schema\n- Universal ERP support through dynamic catalogs\n- Dual schema auto-population\n- Intelligent field normalization\n\n**Sales Data Import:**\n- Flexible CSV column name variations\n- 70+ field aliases (case-insensitive, whitespace-tolerant)\n- Intelligent field normalization\n- Support for .csv, .xlsx, .xls formats\n\n**Required columns:** transactionDate, grossAmount\n**Optional columns:** transactionId, productName, productCode, category, territory, currency, netAmount, quantity, unitPrice\n\n**Data Security:**\n- PostgreSQL database with Drizzle ORM\n- Secure session management\n- Audit trail logging\n- Version control for contracts\n- Role-based data access controls	[0.001269698,-0.051172018,-0.0035601887,-0.07984365,-0.019976191,-0.04103254,-0.004248703,0.021748636,0.017402194,0.061752774,0.03615599,0.0010940218,0.004894877,-0.08058851,-0.0156139955,-0.04704072,0.0019997307,0.008742905,0.01020423,0.041105058,0.013139218,-0.011390754,-0.043002836,0.004261601,-0.015524932,0.08379614,-0.021917785,-0.018248748,-0.06384836,-0.20628127,-0.06059556,0.0392703,0.06478149,0.012869159,0.05582353,-0.026065962,-0.062958725,0.023042163,-0.035155546,-0.0468842,0.020250974,0.0006959341,0.0016854868,-0.02380471,0.027775746,-0.06551692,0.027591037,0.01215569,-0.04579177,0.016539328,-0.022344304,0.010572371,-0.05099386,0.05008385,-0.041619297,0.05804607,0.10177254,0.041170634,-0.054597184,0.006643838,0.06970546,0.03640677,-0.076919794,0.10970014,-0.00018315652,0.07676554,-0.07048607,-0.08317282,-0.016726457,0.033207744,-0.013908147,-0.040950995,-0.08314644,0.024595728,0.027602492,0.012773807,0.0047352673,0.022847703,-0.036470834,0.003949948,-0.041361652,-0.022612302,-0.0071935295,-0.013483009,-0.012065347,0.0040529226,-0.05121421,-0.042185955,0.026959931,-0.04355876,0.083101325,0.0066946773,-0.0180282,0.021230834,-0.0089773815,-0.007755809,-0.058417946,0.04631339,0.029386912,0.3159382,0.0039684405,-0.013287114,0.005906666,-0.014615576,0.0033787615,-0.0043845675,-0.01232864,-0.016013738,0.011958887,0.015748614,-0.035689473,-0.046262454,0.012033638,-0.03396238,0.010205589,-0.022699585,-0.06407403,0.008233713,0.0038365494,0.0050262017,0.03747395,0.021012818,0.0043302313,0.07460618,0.014596178,0.012870736,0.03320166,0.028773312,-0.014137376,0.028114494,0.033940334,-0.01708481,-0.0726814,-0.04067222,0.06251178,-0.024525415,0.0028213456,-0.0035459108,0.0054042567,-0.010635599,-0.047401585,0.04733078,0.0398341,-0.04450047,-0.0120346,0.16881303,0.02890689,0.015014946,-0.049188863,-0.02640141,0.025138495,0.027155131,0.0380661,-0.039953455,-0.0026802316,0.050994597,0.091271594,0.062546305,-0.03389664,-0.0019710325,-0.004328909,-0.034516465,-0.007491956,0.15970933,0.016148476,-0.13007824,-0.010729797,-0.0029883033,-0.0604308,-0.022961773,0.017178733,-0.010919639,-0.0026776346,0.039196555,-0.01908996,-0.034861203,-0.039337695,0.021833556,0.041929923,0.019398415,-0.034401376,-0.010541618,0.0059165796,-0.008782231,-0.029766576,-0.047921803,-0.08714771,0.07043461,0.006005998,-0.010468635,-0.026498536,0.05650667,-0.02362904,0.048300598,0.018433943,0.0005246896,0.00783058,0.01187773,0.023939487,-0.046901964,0.055361196,0.09526703,-0.0063136774,0.004824431,-0.04738297,0.0058873217,0.015016349,0.010786988,-0.047191694,0.015287522,-0.011875656,0.024090162,-0.09400357,0.043965947,0.00013711854,0.056538865,0.027298514,0.023122948,-0.020703994,0.044951063,0.0022018678,0.07202906,0.024188245,-0.34060472,0.026865425,-0.057664502,0.020815177,-0.066095464,-0.028386462,0.025696883,0.05816049,-0.04661842,0.042473376,0.05815181,0.01735511,-0.050891772,0.00112976,-0.038363714,0.03750384,-0.016322639,-0.009184197,-0.042696916,-0.0064150463,-0.027895637,0.013440608,-0.04393731,-0.024759488,0.10702584,-0.048717123,0.08571156,-0.08013948,0.034772158,-0.027982919,0.0034297367,0.011653691,-0.013056594,-0.11239632,0.0708546,-0.08830144,-0.030561674,-0.031877123,0.0102185495,-0.02332085,-0.05258966,0.010930384,0.012529331,-0.083145745,0.015233718,-0.008739024,0.022888094,-0.002330766,-0.018059094,0.046991967,-0.015404127,0.020130863,0.03582835,0.085542016,0.030985277,-0.011016082,-0.00724646,0.010823325,-0.017621709,0.0004215174,0.1095565,0.002962467,-0.009215421,0.008377991,0.05216139,-0.07194204,0.05993571,0.045093577,0.05499899,-0.042031635,-0.05672855,0.04369275,-0.05912665,-0.0032224748,0.036341265,0.010601656,-0.017994052,-0.030129714,-0.013604737,0.024330884,0.011405878,0.010014335,0.08105041,0.04329413,0.02698741,0.058692846,0.027853029,0.012803106,0.025357455,-0.02759366,-0.00013298224,-0.033319164,-0.035958536,-0.0030564673,0.015977219,0.0097898375,-0.21869986,0.020329067,-0.06325615,0.046704605,-0.014616467,0.0070509305,-0.012982768,-0.07385442,0.0105445655,-0.010364437,0.0135842925,0.040554676,0.021838184,-0.07875195,-0.022422884,0.0014143009,0.10625644,-0.00070980535,0.016998641,0.007547261,0.05235407,0.026060713,0.14916691,0.080019124,-0.014490436,0.026303101,-0.0045355377,0.027655043,0.034533393,0.07744008,0.03789093,-0.0428106,0.071204275,0.04252466,0.0070475643,0.040669136,0.008298577,0.046474062,-0.020756861,0.0052837282,-0.05855791,-0.038478255,-0.05798766,-0.02948681,-0.0037039574,-0.03957756,-0.06448452,-0.044964153,-0.016984154,-0.006345628,0.010235363,-0.07532984,-0.048205204,-7.83471e-06,-0.007539104,0.00512402,0.041397665,-0.003408101,-0.02532481,-0.005318911,-0.011596173,0.005689189,-0.012741141,0.052564934,-0.0070901955]	{"title": "How does LicenseIQ handle data management?", "category": "Data Management"}	2025-11-11 22:43:37.450163	2025-11-11 22:43:37.450163
228c976b-0d42-4941-8578-5748117fb782	user-management	Security	What security features does LicenseIQ provide?	LicenseIQ implements enterprise-grade security:\n\n**Five-Tier Role-Based Access Control (RBAC):**\n1. **Administrator** - Full system access, user management, system configuration\n2. **Owner** - Business owner with full access to their organization's data\n3. **Editor** - Can edit contracts and data, create rules, run calculations\n4. **Viewer** - Read-only access to contracts and reports\n5. **Auditor** - Access to audit trail and compliance reports\n\n**Authentication & Sessions:**\n- Passport.js authentication middleware\n- Secure password hashing with bcrypt\n- PostgreSQL session store (connect-pg-simple)\n- Session timeout and automatic logout\n- Protection against CSRF attacks\n\n**Data Security:**\n- All API endpoints require authentication\n- Role-based authorization checks\n- Secure file upload validation\n- SQL injection protection via Drizzle ORM\n- XSS protection through React's built-in escaping\n\n**Audit Logging:**\n- Comprehensive audit trail for all critical operations\n- User action tracking\n- Contract modification history\n- Calculation run logs\n- Compliance reporting\n\n**Contract Metadata Management:**\n- Version control for contracts\n- Role-based approval workflows\n- AI auto-population with manual override\n- Change tracking and history	[-0.013889764,-0.049698446,-0.058695327,-0.0687707,-0.028038478,-0.025202742,0.07238103,0.04084909,-0.015460764,0.019199317,-0.0021337688,0.007613456,0.04294224,-0.059030507,-0.016930038,-0.0719397,0.02257372,0.016743463,-0.026520528,0.09469263,-0.04963519,-0.036699563,-0.060290996,-0.04578388,-0.050917868,0.045387775,-0.044110697,-0.024469523,-0.100487806,-0.17042033,-0.036581688,0.009243974,0.036644753,-0.009953197,0.010492706,-0.03755307,-0.05237629,0.001327147,-0.0391585,-0.055126928,0.023689894,0.041408435,-0.0015640666,-0.036663193,-0.007865416,-0.033547904,0.043228403,-0.016088223,-0.047104623,0.019479958,-0.021324746,0.009580057,-0.026753072,0.05014028,0.007877295,-0.021956448,0.06283017,0.07499002,-0.007510805,0.026597995,0.068811946,-0.0065613934,-0.08925541,0.07804986,0.015432779,0.11394262,-0.03414295,-0.095459625,-0.015662571,0.016781969,-0.021845438,-0.0036329925,-0.11544831,0.028087784,-0.0086575635,0.016083058,-0.01263717,0.011824493,-7.3149604e-05,-0.0034552503,-0.01594785,-0.038131736,0.0004282381,-0.012142195,-0.017187634,-0.01742224,-0.009653828,0.0077614347,0.013348315,0.038807195,0.07890028,-0.06269347,0.012968306,-0.025176518,-0.03423051,-0.031244831,-0.016401134,0.031743318,-0.008570148,0.31133255,-0.014488982,-0.019760039,-0.014887004,-0.037817992,0.002555682,0.016536288,0.062678516,0.0059656166,-0.0063348687,-0.0070239673,-0.025736595,0.03259155,-0.01224995,-0.044731386,-0.0005849789,0.020392485,-0.042964753,0.072768345,-0.014983286,0.030181272,0.013444713,0.025702065,0.014337601,0.035093505,0.012229415,0.0699541,0.03945876,0.055598885,0.02061928,0.08547628,-0.011814379,-0.01784564,-0.007540665,0.00087286107,0.04828969,0.014858196,-0.025962096,0.045449954,0.012388929,0.080390096,-0.050426614,0.07663482,0.019638015,-0.03399301,-0.03301195,0.07348749,0.03196777,0.07131873,-0.04047494,-0.026705557,-0.007988314,0.07640313,0.018090252,-0.013345105,0.024684468,0.012876786,0.0452042,0.0070575764,-0.020195827,0.019575166,-0.03172906,-0.035200655,0.025692448,0.103728056,0.041845966,-0.14862393,-0.0141041055,0.010430463,-0.066279136,-0.049000423,-0.04603232,-0.0014660872,-0.049630832,-0.02002513,0.005779068,-0.014621508,-0.0054386477,0.014405062,0.012280204,0.017475646,-0.025924139,-0.048762158,-0.08533901,-0.016245548,-0.01748508,0.027053382,-0.043889437,0.01164555,0.098911375,0.007636244,-0.037873287,-0.005727615,-0.017064039,0.05458405,0.040745936,0.019648282,-0.03436845,-0.016850542,-0.061478462,-0.03312327,0.06925298,0.13487573,-0.0029309809,0.032029603,-0.019707,-0.0026688373,0.029296407,0.0015013202,0.009914185,-0.02179194,-0.034220956,0.091471456,-0.083599284,0.04901053,-0.030699953,-0.008856612,0.010708962,-0.005901524,0.010193414,0.035763763,0.008078296,0.11640641,0.0032715227,-0.33104628,0.00784903,-0.06010871,0.00851334,-0.030869583,-0.06180705,-0.01524763,0.08721332,-0.027926901,0.021180358,0.11216393,0.033705,0.016199848,0.008540651,-0.041674603,0.0816448,0.0030017602,-0.050120447,-0.019176543,-0.0016883346,-0.02468338,0.0016346092,-0.02622067,0.028084695,0.12357613,-0.019174784,0.07073171,-0.04325805,0.04945859,-0.0058141667,-0.051462453,-0.0036580507,-0.03410958,-0.1005192,0.07021919,-0.019161692,-0.04959856,0.022950932,0.0033926484,-0.014938818,-0.025601907,0.004966678,0.03364822,-0.10077769,0.015860718,-0.061384834,0.0072802077,0.008137877,-0.017088937,0.044061605,0.043034744,0.05007288,0.025717428,0.0494375,-0.01297974,-0.03303224,-0.017464396,-0.015590073,-0.037231132,0.07259045,0.06413579,-0.027629958,-0.015692381,-0.006662644,-0.004184277,-0.0026725698,0.032977223,0.024841795,0.033256635,-0.03559878,-0.06667326,0.1027859,-0.02084217,-0.0029822737,0.027797125,0.02037453,0.058582667,-0.035171993,0.018450363,0.05668524,-0.023086747,-0.03290437,0.033603203,0.008870763,-0.02352277,0.047744177,0.022899285,0.03755662,-0.008820605,0.01430642,-0.06525936,-0.04859138,-0.04424176,-0.05811389,0.015104798,0.021457965,-0.22837625,-0.0015410125,-0.045260012,-0.020889029,-0.031805247,-0.02818015,0.026139898,-0.09046636,-0.07343078,-0.009058928,0.0421925,0.062171735,0.02229184,-0.04102442,-0.01018857,-0.0026935504,0.087359,-0.016125815,-0.012663265,0.027639356,0.016623868,0.01202835,0.12752484,0.060187012,0.0028495553,-0.006221645,-0.012651939,-0.04723288,0.036006156,0.046900064,0.038634807,-0.063611135,0.055091556,0.019173061,-0.0033341958,0.0024623806,0.03129121,0.038041852,-0.060865533,0.029205844,-0.04817969,-0.0016111067,-0.005199971,0.0037827818,0.0014038578,-0.042520933,-0.10034528,-0.017469153,0.063129015,0.023012735,0.036860272,-0.06806555,-0.028828677,0.03860161,-0.020507582,0.026829628,0.0505245,0.022891551,-0.0006757999,0.02310507,0.032689765,0.006267542,0.047825143,0.051544294,0.0075531304]	{"title": "What security features does LicenseIQ provide?", "category": "Security"}	2025-11-11 22:43:37.667663	2025-11-11 22:43:37.667663
e8a57731-3162-4a96-bfa3-08f5e67f1624	getting-started	Usage	How do I get started with LicenseIQ?	Getting started with LicenseIQ is simple:\n\n**Step 1: Upload a Contract**\n- Navigate to "Contracts" → "Upload"\n- Click "Upload Contract" button\n- Select PDF file (supports multi-page contracts)\n- Wait 30-40 seconds for AI analysis\n- Contract is automatically analyzed and categorized\n\n**Step 2: Review AI-Extracted Terms**\n- View extracted parties, dates, and key terms\n- Check license fee rules automatically identified\n- Review confidence scores for AI extractions\n- Edit or correct any fields as needed\n\n**Step 3: Configure License Fee Rules**\n- Click "Manage License Fee Rules" on contract detail page\n- Review AI-extracted rules\n- Add custom rules if needed\n- Configure volume tiers, seasonal adjustments, territory premiums\n- Set minimum guarantees or caps\n\n**Step 4: Upload Sales Data**\n- Navigate to "Sales Data" → "Import Sales Data"\n- Select the contract from dropdown (shows CNT-YYYY-NNN format)\n- Upload CSV or Excel file\n- System automatically matches sales to contract rules\n\n**Step 5: Calculate Payments**\n- Click "Calculate" on contract detail page\n- Review calculated license fees\n- Check rule applications and adjustments\n- Generate PDF invoice if needed\n\n**Step 6: Ask LIQ AI**\n- Click the floating AI button (bottom-right)\n- Ask questions about contracts or the platform\n- Get answers with source citations\n- Use for contract research and insights\n\n**Tips:**\n- Download sample CSV files to understand format\n- Use dark mode for comfortable viewing\n- Check calculation history for audit trails\n- Review confidence scores on AI extractions	[-0.06428149,-0.008643369,-0.04440165,-0.06733768,-0.003087786,-0.004669358,0.020035049,0.050859027,0.022002839,0.032073285,0.006482309,-0.009272877,0.02090979,-0.06297534,-0.028220978,-0.033306018,0.014223625,-0.030433701,-0.009931952,0.009951249,-0.045784093,-0.00145047,-0.034603193,-0.020679457,0.008671224,0.06544327,-0.023401674,-0.033355646,-0.0533744,-0.1922657,-0.032809287,0.015767943,0.06700012,-0.013389839,0.0752653,0.024526326,-0.04268906,-0.01890013,-0.0012686672,-0.062044047,0.07807671,0.0060831406,-0.0073835663,-0.035024397,0.06981221,-0.037950974,0.06418743,0.04793185,0.015257519,0.0451322,-0.021509308,-0.015518367,-0.004719494,0.005528255,-0.027872091,0.00881327,0.09969718,0.05272271,0.021275073,-0.0015994746,-0.025543308,0.026103873,-0.10869793,0.11158947,-0.010893045,0.043242306,-0.01552028,-0.028072195,-0.014711117,0.039903663,0.01940163,-0.0040989853,-0.09801805,0.028476095,0.010822602,-0.016803551,0.045203518,0.03352895,0.046655875,0.013909955,-0.075928494,-0.026583383,-0.022151835,0.0049423403,-0.06439874,-0.0073191263,0.028042274,0.028073313,0.031295095,-0.016723592,0.07935457,-0.03428227,-0.030740444,-0.0033400136,-0.053633172,-0.009677752,-0.021851726,-0.02284051,0.0336072,0.365222,0.03576234,-0.01797393,0.0073038223,-0.015441835,-0.030845165,-0.014396862,-0.03577873,0.021711515,0.011876344,0.004890191,-0.03813658,0.055526774,0.015303812,-0.06873843,-0.002955359,0.04518526,-0.09423897,0.06606472,0.03297732,-0.02580733,0.035713892,0.0020443916,-0.03501267,0.07047572,-0.055482853,0.013046035,0.044870667,0.057445783,0.048569374,0.014118972,0.053734105,-0.026597358,-0.06718853,-0.014019463,0.055662684,-0.011977972,-0.059926618,0.07276298,-0.04002199,0.036439992,-0.04930551,0.0042045363,0.035194457,-0.05188279,-0.060711645,0.07437188,0.007783006,0.050015904,-0.017437717,-0.055895627,0.024680862,0.020846479,-0.007541421,-0.04317007,0.058238965,0.036256876,0.11001673,0.012092465,-0.02824097,0.017583419,-0.021355279,-0.04846797,0.008863166,0.13989618,0.05231159,-0.11785248,0.011855109,-0.0110990135,-0.041111846,0.014835096,-0.014065285,0.027891153,-0.06367184,0.04990929,0.033256724,-0.041573245,-0.004131487,0.009992888,0.04559182,0.013889291,-0.044103846,-0.050926454,-0.07083615,-0.010104628,-0.00806324,-0.035784338,-0.061506305,-0.019153098,-0.015784796,-0.027839368,-0.013208127,0.02702367,-0.069718085,0.057380408,-0.007205681,0.030999787,-0.049269613,-0.025732052,0.014254719,-0.061028697,0.052370094,0.040448826,0.002814163,0.0025090924,-0.0061323983,0.011987085,0.04232782,-0.008511849,0.046855852,0.034281675,-0.029703984,0.038578853,-0.022856887,-0.019678976,-0.02788021,0.06371788,0.030422708,0.008107179,-0.037323803,0.050497867,-0.06426804,0.029314071,-0.0007350553,-0.30289182,0.024052082,-0.03201564,0.031212801,0.013628646,-0.098316826,0.037147988,0.02878348,-0.003620222,0.08919284,0.10671062,-0.0069944016,0.008021832,-0.04314383,-0.01243585,0.027364857,-0.012144509,0.035323564,-0.0059485575,0.03878166,-0.01952862,-0.007884613,-0.031260114,-0.061946593,0.11528896,0.017351668,0.06752662,-0.072022334,0.047480512,0.008287401,0.012027676,-0.014012959,-0.014185639,-0.13471708,0.0056541306,-0.07080197,-0.001331432,0.0013551721,-0.026873909,0.00077022903,-0.011306611,0.016528213,0.025942635,-0.035658095,0.0047515915,0.010215647,-0.046810985,0.06432442,-0.053121738,0.016889157,0.02212083,0.050536655,0.011587153,0.036294375,-0.040104568,-0.015151959,-0.033860154,0.020373179,0.029736748,-0.004554639,0.06855088,-0.029674979,0.017328432,-0.02899466,0.041796785,-0.055277396,0.004318861,-0.0017084378,0.013673504,0.02745265,-0.029135749,0.03012557,-0.005195021,-0.00080435775,0.031132482,-0.0019223592,-0.0018502363,-0.050667312,0.015044278,0.021568114,-0.00053990074,-0.045055415,0.045540456,0.0037906314,0.013614093,0.08898827,-0.006787658,0.0040983576,0.025066396,0.030958274,0.034940057,0.012120353,-0.014345539,0.05264822,0.076490544,-0.017370809,-0.2460117,-0.010562945,-0.028076336,0.07129112,-0.014506813,-0.048399474,0.09253474,-0.08185433,0.015911497,-0.035437614,-0.022815632,0.040288966,-0.03315964,-0.07065631,0.0130982725,-0.041231006,0.06989684,-0.004647616,-0.004980146,0.018550666,-0.0009823501,-0.017834576,0.15044156,0.02574782,0.025377838,0.010905434,-0.040574376,-0.069232196,0.022450758,0.010057109,0.014918823,-0.040788602,0.040544234,-0.0044961083,0.0026913872,0.011140368,-0.065300636,0.029832141,-0.028393425,-0.023471147,-0.013343176,-0.024925694,-0.0077020423,0.031089526,-0.045130428,0.025303474,-0.05255643,-0.015809951,-0.052760366,0.054646045,-0.009545616,-0.070064984,-0.024585422,0.020730786,-0.009530908,0.030618478,0.05599225,0.0326581,0.008098369,-0.047360305,0.011564308,-0.06646663,0.071257636,0.051195327,-0.0134722935]	{"title": "How do I get started with LicenseIQ?", "category": "Usage"}	2025-11-11 22:43:37.903257	2025-11-11 22:43:37.903257
e87ac83d-7de4-435f-ab3a-a59fc88a68e4	calculations	Calculations	How does the license fee calculation work?	LicenseIQ supports sophisticated license fee calculations:\n\n**Calculation Methods:**\n1. **Percentage-Based:** Rate × Gross Amount\n2. **Fixed Fee:** Flat amount per transaction or period\n3. **Volume Tiers:** Different rates based on sales volume ranges\n4. **Minimum Guarantee:** Ensures minimum payment threshold\n5. **Caps:** Maximum payment limit per period\n\n**Advanced Features:**\n- **Seasonal Adjustments:** Multipliers for specific time periods (e.g., spring: 1.15)\n- **Territory Premiums:** Additional rates for specific regions (e.g., California: +0.50%)\n- **Escalation Rates:** Automatic rate increases over time\n- **Container Size Discounts:** Volume-based adjustments\n\n**Formula Engine:**\n- Uses FormulaNode JSON expression trees\n- Supports complex nested calculations\n- Handles multiple conditional rules\n- Applies rules in priority order\n\n**Calculation Process:**\n1. Match sales data to contract rules\n2. Identify applicable rules based on product/territory/date\n3. Apply volume tiers if configured\n4. Calculate seasonal adjustments\n5. Add territory premiums\n6. Apply minimum guarantee or cap\n7. Generate detailed breakdown\n\n**Calculation Output:**\n- Line-item details with applied rules\n- Subtotals by rule type\n- Total license fee amount\n- PDF invoice generation\n- Calculation history and audit trail\n\n**Example Calculation:**\nSales: $7,680,000\nBase Rate: 6.5%\nVolume Tier: 0-5M @ 6.5%\nSeasonal: None (1.0)\nTerritory: None (1.0)\n\nLicense Fee = $7,680,000 × 6.5% × 1.0 × 1.0 = $499,200\n\nThe system handles all calculations automatically and provides transparent breakdown for verification.	[-0.03193488,-0.026157772,-0.04190361,-0.055690892,-0.01055897,-0.04578853,0.053810794,0.0428259,0.060727376,0.04640658,0.0240585,-0.0007397356,0.031928554,-0.018730452,-0.043261413,-0.021412676,0.026848653,-0.03852701,0.0063896296,0.007001448,0.039930683,-0.031036936,-0.031326618,-0.028632322,0.007787411,0.0667734,-0.022205086,1.7090643e-06,-0.046272382,-0.2085792,-0.04022948,0.031407952,0.047199406,-0.017498387,-0.007849511,0.016655227,-0.04908014,-0.0066673006,-0.01102437,-0.038066387,0.068114005,0.042171903,-0.040402774,0.006208056,-0.028004602,-0.0016053489,-0.024033919,0.017127626,0.015523211,0.09090163,0.007983462,-0.04415621,-0.032247797,0.037302736,-0.04828082,-0.011610894,0.09028803,0.055980414,0.009594167,0.016482443,-0.036702562,0.021831587,-0.14621784,0.042225078,-0.018918134,0.040881827,-0.028884657,-0.037733413,-0.029222447,0.056852847,-0.0039969133,-0.0045833616,-0.08555475,0.0047631934,0.03293186,-0.039788414,-0.0027962902,0.013903434,0.012814473,0.00025714145,-0.08995993,-0.05511447,-0.013617583,0.022768568,0.0047949823,-0.009060293,0.074924424,0.0146932015,0.050183494,-0.034623377,0.039253693,-0.007840753,-0.024306657,-0.08372501,-0.04056025,0.043885946,-0.040312458,-0.036659982,-0.0083374875,0.35008308,0.06021752,-0.033005163,-0.05141442,-0.049783897,-0.014541271,0.013169129,-0.02704116,0.026870234,-0.016149437,0.0031957913,-0.03680009,0.051245548,0.054566912,-0.056683857,-0.067925446,0.05376752,-0.069704905,0.050777406,0.017534884,-0.03091286,0.019548936,0.036648415,0.004090218,0.055053104,-0.049011167,0.05820955,0.05975452,0.059837654,-0.021970076,0.06410123,0.038653087,-0.042531297,-0.051505707,-0.01697396,0.059544835,-0.039079104,-0.01183978,0.053885326,-0.029038578,0.07394272,-0.038778536,0.02341702,-0.057577122,-0.040048305,-0.07941536,0.14734395,0.0045434013,0.075901926,0.03514516,-0.038921006,-0.00424759,0.048592,-0.036716554,-0.01271274,0.030023104,0.03306167,0.09596751,0.010653849,-0.045197356,0.009815366,-0.032295294,-0.007957277,-0.018553708,0.12276851,0.0536173,-0.07144171,0.023226984,0.016570762,-0.033744484,-0.032989353,-0.017822258,0.030059323,-0.017320788,0.003574648,0.06325162,-0.017395843,-0.008228383,0.035489157,0.038663182,0.03120245,0.022751415,0.022337683,-0.09760102,-0.03499886,-0.03614154,-0.039783187,-0.0907583,-0.03814052,0.0010898928,0.023977496,0.032063533,-0.05515946,-0.09145086,0.055450995,0.006149028,0.009936863,-0.0030774705,-0.0085728755,0.016986063,-0.022950443,0.08424802,0.057461224,0.05692454,0.00850566,0.015175949,0.023150306,0.044874433,0.020517679,0.003943352,0.020182803,-0.012156508,0.020567946,-0.016228316,-0.032565907,-0.023021553,0.060444117,0.040584933,-0.01113544,-0.02404032,0.04292494,-0.05503152,0.0015498818,0.0076828757,-0.29336208,0.01460147,-0.006932393,0.024480235,0.048325717,-0.032588802,0.028161502,0.019673035,-0.028829377,0.0475687,0.08730533,-0.022265034,-0.011919542,-0.0147221815,0.0054349187,-0.00049243803,-0.033806924,0.020670976,-0.012156136,0.011194348,-0.01589598,-0.021652203,-0.06300011,-0.023420226,0.1139378,0.036716368,0.077930726,-0.11142889,0.042633794,0.026338303,0.05183394,-0.057911944,-0.01713092,-0.0033177964,0.013842166,-0.026523016,-0.01597743,-0.01103624,-0.023220029,-0.008864307,-0.06007152,0.0097762775,0.0021219193,-0.06906634,0.001569027,0.030420963,0.0023472873,0.07144204,-0.055976126,-0.01690733,-0.0108202305,-0.023455039,0.0722914,0.015776101,0.011258848,-0.042891715,-0.014299853,-0.030779986,-0.008115142,-0.022029432,0.08099833,-0.088053994,0.07416772,0.044287194,0.0044871895,-0.07635216,0.05781201,-0.024583314,-0.0649932,0.02109311,-0.012618288,0.07421861,-0.008843768,-0.026679775,0.043587457,-0.035690885,0.02750401,0.020196842,0.022897512,0.033938877,-0.020749696,-0.039808005,0.07320003,-0.009114504,0.032346178,0.06212524,0.017602846,0.013721611,-0.029297622,0.011871848,-0.03180588,-0.028114185,0.0130687505,0.029815666,-0.014547893,0.00810515,-0.2698518,-0.021591729,-0.07013218,0.01861735,0.012947731,-0.026939156,0.09810017,-0.0057034735,-0.04591232,-0.053406723,-0.010695969,0.037490133,0.027136195,-0.011860751,0.016576359,-0.05778869,0.049825843,-0.026402816,0.05480554,0.032019403,0.011921528,-0.009845054,0.13246149,0.026180023,-0.00038210966,-0.021273434,0.0018481084,-0.021335617,0.09106114,-0.013715754,-0.036652163,0.026438748,0.05888699,0.0032978405,-0.01856585,-0.004571732,-0.060165353,0.022942618,-0.018680345,0.021564147,0.0034248393,-0.03479481,-0.023344437,0.018090134,-0.0023913553,-0.021871038,-0.031297743,-0.05162904,-0.02184994,0.060570788,0.011315223,0.00044665835,0.00963064,-0.0500168,0.043789286,0.028979996,-0.009128723,-0.00041788255,-0.0062592854,-0.023808558,-0.058539767,-0.01348564,0.0025047993,0.054408483,0.03617001]	{"title": "How does the license fee calculation work?", "category": "Calculations"}	2025-11-11 22:43:38.119986	2025-11-11 22:43:38.119986
3f58b8cd-72c3-4eb0-8b17-e1eb4bf61e45	liq-ai-capabilities	LIQ AI	What can LIQ AI help me with?	LIQ AI is your intelligent assistant with two main capabilities:\n\n**Contract Document Q&A:**\nLIQ AI can answer questions about your uploaded contracts:\n- "What is the royalty rate in the Electronics Patent License?"\n- "When does the Manufacturing contract expire?"\n- "What territories are covered in this agreement?"\n- "Are there any volume discounts in the Plant Variety contract?"\n- "What are the payment terms?"\n\n**Platform Information:**\nLIQ AI can also help you understand the LicenseIQ platform:\n- "What is LicenseIQ?"\n- "What contract types are supported?"\n- "How do I upload sales data?"\n- "What AI services does this platform use?"\n- "How does the license fee calculator work?"\n- "What security features are available?"\n\n**How It Works:**\n1. **Vector Embeddings:** All contracts and platform documentation are converted to 384-dimensional vectors using HuggingFace embeddings\n2. **Semantic Search:** When you ask a question, LIQ AI searches for the most relevant information using vector similarity\n3. **Context Retrieval:** Top matching sections are retrieved from the database\n4. **Answer Generation:** Groq LLaMA model generates a comprehensive answer using the retrieved context\n5. **Source Citations:** You get answers with source references and confidence scores\n\n**Features:**\n- Omnipresent floating button (bottom-right corner)\n- Context-aware responses\n- Source citations with similarity scores\n- Confidence indicators\n- Works across all contracts\n- Handles both specific and general queries\n\n**Best Practices:**\n- Be specific in your questions\n- Ask one question at a time\n- Review source citations for verification\n- Check confidence scores (higher is better)\n- Use for quick contract research and insights	[-0.09451916,0.010282,-0.05340912,0.0075412085,0.0054355934,-0.03778184,0.036521208,0.02451601,0.023942553,0.0075630583,-0.04073203,0.007408461,0.029747203,-0.03293659,0.020778708,-0.056718316,-0.0022033534,-0.033636287,-0.017508475,-0.009905443,-0.01785752,-0.05693797,-0.04822384,-0.013752143,-0.04677458,0.018604549,-0.060115825,-0.041593462,-0.04399289,-0.19030626,-0.024524452,0.0495672,0.053724073,0.006621254,0.0018827802,0.019392414,-0.06862107,-0.025695581,-0.030000046,-0.014949043,0.062231768,-0.005092929,-0.03646433,-0.032742985,0.070449285,-0.04938302,0.022580365,0.027040934,-0.033028174,-0.004530164,-0.028677963,0.008082392,-0.0016797942,-0.0077454313,-0.05613927,0.05990932,0.050561067,0.122161075,0.0460459,0.029290838,-0.0019691898,0.033106074,-0.08183799,0.12834957,-0.054421503,0.036499247,-0.023032477,-0.04127231,-0.00015057925,0.06767809,0.020293135,0.007785825,-0.0659848,0.009629257,0.06391502,0.039224785,0.022568012,-0.0012503366,0.046793226,-0.008743774,-0.031368583,-0.0627008,-0.03382711,-0.01846178,-0.051031698,-0.0076409755,0.0042973743,0.046171363,0.044741567,-0.042596567,0.04290313,0.008255679,-0.022256171,0.019318085,-0.003061107,0.0010725365,-0.00207937,-0.02701097,-0.032852333,0.37171954,0.037035037,-0.046543274,0.017869081,-0.040519632,-0.06640793,-8.944418e-05,-0.044347085,-0.020253269,-0.027393647,-0.02157792,-0.045316,0.055276774,0.011710321,-0.003772516,-0.017994799,-0.0067130364,-0.014526793,0.045181625,-0.013718125,-0.021876073,-0.030219821,0.028959269,-0.03588573,0.059807062,-0.019669862,-0.012567674,0.024862474,0.039708767,-0.018137772,0.022910425,0.0731613,-0.023041716,-0.07416823,-0.012150485,0.02248975,0.012150525,-0.018320126,0.005674038,0.0077325725,0.047156695,-0.07214848,0.015574704,0.035875812,-0.0059758658,-0.07371558,0.07216234,0.026755955,0.042456534,-0.04698316,-0.08387839,0.02403584,0.05074042,0.002190405,-0.03307883,0.08922967,0.042243905,0.08997059,0.015871506,-0.053796493,0.036169257,0.011485758,-0.0026658585,-0.013174193,0.15160927,-0.003153696,-0.1390947,-0.045246556,-0.010213741,-0.027505288,-0.00078412564,-0.0019714308,0.047892284,-0.052959718,0.006599436,0.0330984,-0.018364666,-0.012658644,0.015908286,0.012236578,0.061497197,-0.03627101,-0.05386041,0.01751079,-0.03157753,-0.028008875,-0.051321767,-0.02016785,-0.028105259,-0.009121762,-0.041865136,0.002748093,0.052025933,0.010907446,0.1068996,0.013043101,0.029840829,-0.034235142,0.023008917,0.014432664,-0.023916328,0.0116165,0.05938158,-0.0018710289,0.0019873742,-0.023836965,-0.044692427,0.037365463,-0.011030204,0.045480385,0.06419681,-0.03733289,0.045023322,-0.037394933,0.00087189936,-0.015931044,0.05326414,0.018201692,-0.014756288,-0.03511037,0.0079684,-0.030753626,0.050066646,-0.001870747,-0.3189959,0.011404727,-0.07051101,0.052452017,-0.026168399,-0.08709584,-0.014058223,0.023562528,-0.011241425,0.09404186,0.088081576,-0.0029352691,0.013391169,-0.022480428,-0.008939063,0.0329237,0.04152039,0.029394448,-0.0034125939,0.025416682,-0.011798426,0.0075902506,-0.01661095,-0.08612613,0.06520393,0.016847998,0.070254624,-0.08387909,-0.0018843695,0.004108776,0.019477198,0.057811216,-0.0652943,-0.0877756,0.028020386,-0.06364755,-0.01952554,0.0160085,-0.03601902,0.018831851,-0.06581161,-0.0008681185,0.026610976,-0.039308786,-0.01849935,0.0062319264,0.020134345,0.015479124,-0.06334865,0.05263122,-0.036026727,0.07045843,0.0033161817,0.038817696,-0.022591878,0.03611307,-0.044588313,0.0020602078,-0.00850147,0.024538545,0.041196093,-0.017877957,0.058943793,-0.023047602,0.06605956,-0.10162876,-0.0015978062,0.040683955,0.02238332,-0.008074696,-0.012661268,0.11933878,-2.4701737e-06,0.03899001,0.042296868,-0.026139991,0.049094994,-0.040122375,-0.019618714,0.023269594,0.044330627,0.012681536,0.059894107,0.0055121686,0.04049006,0.061044455,0.02564855,-0.023021324,0.033353806,0.029746149,-0.022263894,0.025879143,0.0046853283,-0.019279538,-0.0048830113,-0.023593973,-0.23661481,0.016791958,-0.017231738,0.04543459,-0.019732079,0.0060081766,0.008479105,-0.073865995,-0.011892366,0.014055245,-0.031059127,0.06440721,-0.047223076,-0.034448624,-0.0010334118,-0.06325675,0.08783667,-0.01310478,-0.008835544,0.03303205,0.035748128,0.014969106,0.15774792,0.020743323,0.025438227,0.004420165,-0.010094513,-0.04489479,-0.02188185,-0.028624527,-0.025284946,-0.008743568,0.038689386,-0.006003494,0.00041870706,0.04154247,-0.08794057,-0.0018769007,-0.008069246,0.037483986,-0.010552256,-0.017241644,-0.03915576,0.034927785,0.048087593,0.052839004,-0.06576194,-0.0045021446,-0.017391935,0.012884653,-0.017223578,-0.045612663,-0.007307431,0.023005411,-0.039305195,-0.011852265,0.049812883,0.024653181,-0.0069667525,-0.034562953,0.04261701,-0.029324494,0.08993663,0.0518891,0.008515439]	{"title": "What can LIQ AI help me with?", "category": "LIQ AI"}	2025-11-11 22:43:38.333175	2025-11-11 22:43:38.333175
4908ade8-666f-428e-8aab-7e49b6f5083f	deployment	Deployment	How is LicenseIQ deployed?	LicenseIQ can be deployed to production using comprehensive guides:\n\n**Deployment Options:**\n1. **Hostinger VPS** (Recommended for production)\n2. **Replit** (For development and testing)\n3. **Other VPS providers** (DigitalOcean, Linode, AWS EC2, etc.)\n\n**Hostinger VPS Deployment:**\nTwo comprehensive deployment guides are available:\n\n**Command-Line Guide** (HOSTINGER_DEPLOYMENT_GUIDE.md):\n- Terminal-based deployment instructions\n- VPS setup and initial server configuration\n- Node.js, PostgreSQL, and pgvector installation\n- PM2 process manager setup\n- Nginx reverse proxy configuration\n- SSL certificate setup with Let's Encrypt\n- Domain configuration and DNS setup\n- Database backup and maintenance procedures\n- Troubleshooting common issues\n\n**UI-Based Guide** (HOSTINGER_UI_DEPLOYMENT_GUIDE.md):\n- Visual walkthrough using Hostinger's hPanel\n- Browser Terminal usage for installations\n- hPanel-based domain DNS configuration\n- Visual firewall setup\n- Snapshot management\n- No SSH client required - everything through web browser\n- Perfect for users preferring GUI over command-line\n\n**Technology Stack:**\n- **OS:** Ubuntu 22.04/24.04\n- **Process Manager:** PM2 (auto-restart, clustering)\n- **Web Server:** Nginx (reverse proxy)\n- **Database:** PostgreSQL 14+ with pgvector extension\n- **SSL:** Let's Encrypt (free certificates)\n- **Node.js:** v20.x LTS\n\n**Production Features:**\n- Automatic restart on crashes\n- Zero-downtime deployments\n- SSL/TLS encryption\n- Database backups\n- Resource monitoring\n- Firewall configuration\n- Domain management\n\nThe deployment process is well-documented and can be completed in 1-2 hours depending on experience level.	[-0.006602203,-0.006159803,-0.0625493,-0.0774863,0.05413739,-0.010463163,-0.07589282,0.014446515,-0.0128163565,0.029463008,0.00045246386,-0.012389536,0.069379434,-0.06613648,0.0108684115,-0.010277874,-0.0024421231,-0.024886064,0.0070055267,-0.01702473,-0.011221507,-0.03621639,-0.036952924,-0.03797289,-0.04155613,0.026555276,-0.04379257,0.038272645,-0.027906626,-0.15886562,-0.017213376,0.0114987,-0.016564827,-0.022912458,0.018332716,-0.025283836,-0.03543439,-0.0212266,-0.03598616,-0.014843371,0.0111029595,0.006427634,-0.033040125,-0.025952067,0.02938222,-0.037018593,0.059287466,-0.015784893,0.018939354,-0.000907583,0.04064446,0.011885844,-0.045227736,-0.029989792,0.002771405,-0.042628285,0.10272367,0.053204678,-0.029846398,-0.02775937,0.037207995,-0.04522801,-0.14340577,0.08920617,0.030501923,0.035698142,-0.031893168,-0.09678588,-0.020196926,0.041817192,0.013964105,0.0075621,-0.07294936,0.03547566,0.045140807,0.019521462,0.031683292,0.03833398,0.0387665,-0.018595671,0.01563526,-0.037634283,-0.020980002,-0.008926571,-0.032112114,-0.025948618,0.020819126,0.036815606,0.0043820073,-0.0054627713,0.06211878,-0.0244112,0.03221379,-0.01037652,0.02387467,0.019852113,-0.02814617,-0.024049442,-0.006671569,0.31579748,0.0056832833,-0.008059464,-0.001035387,0.014753098,-0.021889992,0.01613594,-0.026071737,0.018460818,0.05466343,0.0334441,-0.010659845,0.035439916,0.010226951,-0.07331117,-0.0030016282,0.02596143,-0.05307868,0.05769752,0.006421793,0.027850203,0.07428539,0.011822763,0.054182373,0.09382861,0.045619406,0.04115198,0.054447543,0.010747482,0.012226502,0.054559473,-0.023325335,-0.024450704,0.010896863,0.006587962,0.045337923,-0.021429297,-0.0059290705,0.0538803,0.021593599,0.028360778,0.005019083,0.0489963,0.04713789,-0.02074432,0.010874539,0.09874854,0.00718764,0.05069392,-0.046863697,-0.054683495,-0.025856048,0.032233506,0.007781533,-0.015059621,0.040569764,0.04061383,0.05645632,0.053165372,-0.053487677,-0.012962665,-0.011962846,-0.06799743,0.02688748,0.14958452,0.016189422,-0.12816325,-0.02027316,0.005391574,0.021929834,0.007544934,-0.062110957,0.0010456889,-0.0043467465,-0.025039997,-0.03510046,-0.024285052,-0.012938299,0.042128075,0.054458655,0.07319781,-0.02795886,-0.060923945,-0.031637765,-0.051131643,-0.022933973,-0.022357715,-0.0693999,-0.00715644,-0.008641386,-0.016430497,-0.03776784,-0.034584805,-0.017388698,0.011208199,0.0836212,0.053592935,-0.0574328,0.01096699,0.050580386,-0.015640989,0.060521454,0.10856443,-0.006314735,0.04064707,-0.031612836,0.0103480425,0.048151102,-0.05520812,-0.032081723,0.031516414,-0.036844976,0.105552554,-0.019371305,0.03003385,-0.05912302,0.028236693,0.010670893,0.055600896,-0.0050407164,0.041800138,-0.010304465,0.024122743,-0.019188084,-0.30093783,-0.051990148,-0.018883154,0.06542372,0.027928287,-0.048207853,-0.043062482,0.08306379,-0.007025617,0.029304365,0.08291551,-0.047150794,0.06798629,-0.043757588,-0.051747113,0.018883914,-0.026733993,-0.07062584,-0.0010878402,0.0024503043,-0.015288891,-0.02686926,-0.039427444,-0.020443741,0.12898028,-0.0077036167,0.08619546,-0.06840616,0.07525041,-0.055412862,0.029413126,0.04059354,-0.006372161,-0.20077911,0.01182342,-0.026982447,0.00844312,-0.019937335,-0.0040440457,0.01858102,-0.02305509,0.033066478,0.0057757013,-0.110241964,0.07301108,0.0043666605,-0.02504557,0.05889077,-0.0072734053,0.012821192,-0.0024558345,0.021786602,0.039015695,0.016687175,0.0075321556,-0.00988591,-0.012131505,-0.02239575,-0.04173437,0.06845342,0.02403089,-0.02395031,-0.0023336518,-0.032570794,0.034410123,-0.02200443,0.04646474,0.03267216,0.035166867,-0.014425098,-0.054119274,0.06332958,0.0018404382,0.058219984,0.052802302,-0.03130595,-0.025186224,-0.032771055,-0.00974322,-0.014177218,-0.023100704,-0.039925214,0.02715199,0.02424763,0.05026764,0.04957757,-0.045326266,0.039414696,0.0064981417,0.067796506,-0.046194624,-0.008533727,-0.07907016,-0.035577904,-0.01360965,-0.017544463,-0.23086184,-0.015036015,-0.0006639402,-0.03220231,0.029611573,-0.05100385,0.04793674,-0.03414053,-0.012959525,0.010247006,0.037942324,0.022941697,-0.02791215,-0.019923512,0.0014054315,-0.0018106163,0.07473484,-0.038606286,-0.049866166,-0.00019799029,0.022616861,-0.04107378,0.08983963,0.07978225,-0.018476231,0.006081278,-0.057940006,0.035992477,0.004202609,0.032376792,-0.0064212056,-0.056286845,-0.019602515,-0.026979983,-0.0104915025,0.05120403,-0.01636853,-0.0034920843,-0.07437015,-0.03284942,-0.045109283,-0.07948593,0.034652192,0.07644973,-0.028876366,0.0034990148,-0.076888606,-0.04395037,0.024288977,0.014901377,-0.10422769,0.0052413563,-0.018565945,0.07292952,0.021452077,0.037279032,0.08553973,-0.0019965502,-0.04639554,0.04132308,0.04536662,-0.08090039,0.07053252,0.044280145,-0.0010232488]	{"title": "How is LicenseIQ deployed?", "category": "Deployment"}	2025-11-11 22:43:38.556516	2025-11-11 22:43:38.556516
\.


--
-- Data for Name: user_navigation_overrides; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.user_navigation_overrides (id, user_id, nav_item_key, is_enabled, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: neondb_owner
--

COPY public.users (id, email, first_name, last_name, profile_image_url, role, is_active, created_at, updated_at, username, password) FROM stdin;
993f41bb-59e7-4a3a-ad0d-25e808ca81c7	admin@licenceiq.com	System	Administrator	\N	admin	t	2025-09-05 14:21:19.795215	2025-09-05 14:21:19.795215	admin	ba8f14f711d9fc4778ed45c6828a2e847b0a17474e9456fc1156a07d0180c33d85bd21c9929ad37fecedf62eabb63b78209b8aeeae58c89aa66518be284c2966.c284ef4d946e44e4ed060ef6ba260cc5
8430b90f-51ce-4b83-bcef-cfdebfb0aa35	owner@licenceiq.com	Company	Owner	\N	owner	t	2025-09-05 14:21:21.218866	2025-09-05 14:21:21.218866	owner	b1de5549189f1c63520d5675aa2e28819259ae8368ab1e9abe5b0017b0234c5b8ff39e90e810aa7301cfe5518a939bd796273f628fea13abb42ba0474556c4bf.6610e82753cc28eabf487397eab7a0e7
e8d057be-f50c-4097-ad01-34c6878ad68b	editor@licenceiq.com	Content	Editor	\N	editor	t	2025-09-05 14:21:22.560947	2025-09-05 14:21:22.560947	editor	63bf70e25094c038028b0913f3d6abc1f016c8d934e7d5f6ed2a6eceb9005d93f9c12c7c648c699e82f98cabb073cf01c3a23ade8949056a2891a7b85e772fb1.cd218db3174ffe9349d1d095f5e96401
86ed9fd5-69bc-4751-a26b-0c7e79813e0f	viewer@licenceiq.com	Basic	Viewer	\N	viewer	t	2025-09-05 14:21:24.083283	2025-09-05 14:21:24.083283	viewer	7eb76e32d678866e16a1b36fee3e7f384b7f31bdf7ca5b3a9bd3505e5003919428d6596fd54c9990f3c938823b04215466c0ff9005c61ca53263106b642ffe9d.755a166f8b4b4df16f7aea8c3962341a
fe4ad658-4662-4417-a204-74d4af654eee	auditor@licenceiq.com	Compliance	Auditor	\N	admin	t	2025-09-05 14:21:25.694389	2025-09-05 15:19:25.732	auditor	ecadbb8955473d020ac6b8d47aa27171b39c3e2dc36a1b2987c9bc5da5e9b8038058ed75730d93d4bf7f6bb0ba7d5dbe4258d458e2d5267f59d0ffc072ca2c45.cc25703da5827206a688a516e3fbb1bd
buG0AJVN5-	testuser_buG0AJVN5-@example.com	Test	User	\N	admin	t	2025-09-11 14:10:09.618592	2025-09-11 14:10:09.618592	testuser_buG0AJVN5-	testpassword123
47214137	kmlnrao@yahoo.com	Narasimha	K	\N	editor	t	2025-09-05 13:58:42.723944	2025-09-11 18:32:47.925	\N	temp
829a0f0c-8ecb-4294-95f8-0b8f129f9d50	testadmin@example.com	Test	Admin	\N	admin	t	2025-09-11 20:00:29.404387	2025-09-11 20:13:14.023	testadmin	fec29586e35b1d15c7134ba89890246e2789806237615e8379c7d93ad4743bcb6f1207db8324a9159bd92ef8f341172f919cfb3075a042c646cb3665f0f95297.a3917eb582bcd42464f58e5dabab3f68
52532e6a-8614-429c-a281-1293a7ff4db9	testuser123@example.com	Test	User	\N	admin	t	2025-09-11 20:23:02.311767	2025-09-11 20:23:02.311767	testuser123	aa5331da35c2956714240bf9f0419c3497f21557d3e13c416f16b5dd171acb1d233c4cf525edb1a6e18f00b9863dc6a3da7a9b2f36f5e04353883251b8ac36af.fb9c981afc21abafe0bff16e616028f6
27405ef9-a742-4fa2-b37c-ff9d197571ff	testuser@example.com	Test	User	\N	admin	t	2025-09-15 18:28:37.102315	2025-09-15 18:28:37.102315	testuser	f27d4c91b3f9a7b0d8e2a1c5e7f3b9d2c4e6a8b0f2d4c6e8a0b2d4f6c8e0a2b4d6c8.abcdef123456
80552359-92b2-4956-8a1b-deb479622ab1	playwright_test@example.com	Playwright	Test	\N	admin	t	2025-09-15 18:29:08.12009	2025-09-15 18:29:08.12009	playwright_test	e4a6422690ff8520b6b10565591e400519019cf16d7fa76ed157136019054c7d70913b80cf86816dbba7a32921b3df3bd1f822e72cfbdf58c9a27389c9188556.c0837dd5a2f7e85aed87fe1bfdac7e42
b343f532-edb9-4a39-a38f-805bae49f380	localadmin@example.com	Local	Admin	\N	admin	t	2025-09-16 00:14:29.727579	2025-09-16 00:14:29.727579	localadmin	401f316d84560c1dd60d664ef026eada084969edf3236b86a9f6f2c37be7e9f51028b7af95513737c00685e25e9db747573b257e6caf7a5772d5ec405716df10.ac098ba0c90476f1b8ae144c4fbcd408
cd7e3584-911f-40e6-bbd9-f9996fe58336	admin_new@example.com	Admin	User	\N	admin	t	2025-09-16 00:15:05.899051	2025-09-16 00:15:05.899051	admin_new	6dc99e21de49a08c2332a70f99258fae1984c27bce40bec5b720f934ec9c6d72e4dfc8a8e5a13048eb8ed59ef89dd641823ab2f6f43c937bb94c6a26cc2270b5.ce1b60e7d8bcd6cf261de4759de4740f
19eaeefb-9bd0-4548-9c37-1a5f84af8d3d	john@testvendor.com	John	Smith	\N	viewer	t	2025-09-21 22:19:47.441986	2025-09-21 22:19:47.441986	testvendor_user	24f61de9c3e438d814ffa0271b9513923d109213aa937d0a3173dff5fd8670acd2a570a691aec551b8ce03e76b72b1d1cb6ef61bcc6eb506c5933d0a4d885c98.290b300b8fe533eff57d2f7523ba00ba
deafebd3-22fb-4d7b-9426-f019c3203bce	testvendor123@example.com	Test	Vendor	\N	viewer	t	2025-09-21 22:24:24.287155	2025-10-17 22:15:12.355	testvendor123	554a3e1bffb9efebf80f46c6cccb32423e20fc5c04850c2d234499271855e0c812208945da9f64e5368934db12201ce1af592ecbe1f4967894bd6162b1e51663.2384caaa482ca5bab14b3ce5aadfaef2
\.


--
-- Name: audit_trail audit_trail_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.audit_trail
    ADD CONSTRAINT audit_trail_pkey PRIMARY KEY (id);


--
-- Name: business_units business_units_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.business_units
    ADD CONSTRAINT business_units_pkey PRIMARY KEY (org_id);


--
-- Name: companies companies_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (company_id);


--
-- Name: compliance_analysis compliance_analysis_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.compliance_analysis
    ADD CONSTRAINT compliance_analysis_pkey PRIMARY KEY (id);


--
-- Name: contract_analysis contract_analysis_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_analysis
    ADD CONSTRAINT contract_analysis_pkey PRIMARY KEY (id);


--
-- Name: contract_approvals contract_approvals_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_approvals
    ADD CONSTRAINT contract_approvals_pkey PRIMARY KEY (id);


--
-- Name: contract_comparisons contract_comparisons_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_comparisons
    ADD CONSTRAINT contract_comparisons_pkey PRIMARY KEY (id);


--
-- Name: contract_documents contract_documents_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_documents
    ADD CONSTRAINT contract_documents_pkey PRIMARY KEY (id);


--
-- Name: contract_embeddings contract_embeddings_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_embeddings
    ADD CONSTRAINT contract_embeddings_pkey PRIMARY KEY (id);


--
-- Name: contract_graph_edges contract_graph_edges_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_graph_edges
    ADD CONSTRAINT contract_graph_edges_pkey PRIMARY KEY (id);


--
-- Name: contract_graph_nodes contract_graph_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_graph_nodes
    ADD CONSTRAINT contract_graph_nodes_pkey PRIMARY KEY (id);


--
-- Name: contract_obligations contract_obligations_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_obligations
    ADD CONSTRAINT contract_obligations_pkey PRIMARY KEY (id);


--
-- Name: contract_royalty_calculations contract_royalty_calculations_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_royalty_calculations
    ADD CONSTRAINT contract_royalty_calculations_pkey PRIMARY KEY (id);


--
-- Name: contract_versions contract_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_versions
    ADD CONSTRAINT contract_versions_pkey PRIMARY KEY (id);


--
-- Name: contracts contracts_contract_number_unique; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contracts
    ADD CONSTRAINT contracts_contract_number_unique UNIQUE (contract_number);


--
-- Name: contracts contracts_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contracts
    ADD CONSTRAINT contracts_pkey PRIMARY KEY (id);


--
-- Name: data_import_jobs data_import_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.data_import_jobs
    ADD CONSTRAINT data_import_jobs_pkey PRIMARY KEY (id);


--
-- Name: demo_requests demo_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.demo_requests
    ADD CONSTRAINT demo_requests_pkey PRIMARY KEY (id);


--
-- Name: early_access_signups early_access_signups_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.early_access_signups
    ADD CONSTRAINT early_access_signups_pkey PRIMARY KEY (id);


--
-- Name: erp_entities erp_entities_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.erp_entities
    ADD CONSTRAINT erp_entities_pkey PRIMARY KEY (id);


--
-- Name: erp_fields erp_fields_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.erp_fields
    ADD CONSTRAINT erp_fields_pkey PRIMARY KEY (id);


--
-- Name: erp_systems erp_systems_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.erp_systems
    ADD CONSTRAINT erp_systems_pkey PRIMARY KEY (id);


--
-- Name: extraction_runs extraction_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.extraction_runs
    ADD CONSTRAINT extraction_runs_pkey PRIMARY KEY (id);


--
-- Name: financial_analysis financial_analysis_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.financial_analysis
    ADD CONSTRAINT financial_analysis_pkey PRIMARY KEY (id);


--
-- Name: human_review_tasks human_review_tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.human_review_tasks
    ADD CONSTRAINT human_review_tasks_pkey PRIMARY KEY (id);


--
-- Name: imported_erp_records imported_erp_records_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.imported_erp_records
    ADD CONSTRAINT imported_erp_records_pkey PRIMARY KEY (id);


--
-- Name: licenseiq_entities licenseiq_entities_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_entities
    ADD CONSTRAINT licenseiq_entities_pkey PRIMARY KEY (id);


--
-- Name: licenseiq_entities licenseiq_entities_technical_name_key; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_entities
    ADD CONSTRAINT licenseiq_entities_technical_name_key UNIQUE (technical_name);


--
-- Name: licenseiq_entity_records licenseiq_entity_records_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_entity_records
    ADD CONSTRAINT licenseiq_entity_records_pkey PRIMARY KEY (id);


--
-- Name: licenseiq_fields licenseiq_fields_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_fields
    ADD CONSTRAINT licenseiq_fields_pkey PRIMARY KEY (id);


--
-- Name: locations locations_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_pkey PRIMARY KEY (loc_id);


--
-- Name: market_benchmarks market_benchmarks_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.market_benchmarks
    ADD CONSTRAINT market_benchmarks_pkey PRIMARY KEY (id);


--
-- Name: master_data_mappings master_data_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.master_data_mappings
    ADD CONSTRAINT master_data_mappings_pkey PRIMARY KEY (id);


--
-- Name: navigation_permissions navigation_permissions_item_key_key; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.navigation_permissions
    ADD CONSTRAINT navigation_permissions_item_key_key UNIQUE (item_key);


--
-- Name: navigation_permissions navigation_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.navigation_permissions
    ADD CONSTRAINT navigation_permissions_pkey PRIMARY KEY (id);


--
-- Name: performance_metrics performance_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.performance_metrics
    ADD CONSTRAINT performance_metrics_pkey PRIMARY KEY (id);


--
-- Name: role_navigation_permissions role_navigation_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.role_navigation_permissions
    ADD CONSTRAINT role_navigation_permissions_pkey PRIMARY KEY (id);


--
-- Name: roles roles_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (id);


--
-- Name: roles roles_role_name_key; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_role_name_key UNIQUE (role_name);


--
-- Name: royalty_rules royalty_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.royalty_rules
    ADD CONSTRAINT royalty_rules_pkey PRIMARY KEY (id);


--
-- Name: rule_definitions rule_definitions_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_definitions
    ADD CONSTRAINT rule_definitions_pkey PRIMARY KEY (id);


--
-- Name: rule_node_definitions rule_node_definitions_node_type_key; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_node_definitions
    ADD CONSTRAINT rule_node_definitions_node_type_key UNIQUE (node_type);


--
-- Name: rule_node_definitions rule_node_definitions_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_node_definitions
    ADD CONSTRAINT rule_node_definitions_pkey PRIMARY KEY (id);


--
-- Name: rule_validation_events rule_validation_events_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_validation_events
    ADD CONSTRAINT rule_validation_events_pkey PRIMARY KEY (id);


--
-- Name: sales_data sales_data_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.sales_data
    ADD CONSTRAINT sales_data_pkey PRIMARY KEY (id);


--
-- Name: sales_field_mappings sales_field_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.sales_field_mappings
    ADD CONSTRAINT sales_field_mappings_pkey PRIMARY KEY (id);


--
-- Name: semantic_index_entries semantic_index_entries_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.semantic_index_entries
    ADD CONSTRAINT semantic_index_entries_pkey PRIMARY KEY (id);


--
-- Name: session session_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.session
    ADD CONSTRAINT session_pkey PRIMARY KEY (sid);


--
-- Name: sessions sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (sid);


--
-- Name: strategic_analysis strategic_analysis_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.strategic_analysis
    ADD CONSTRAINT strategic_analysis_pkey PRIMARY KEY (id);


--
-- Name: system_embeddings system_embeddings_document_id_key; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.system_embeddings
    ADD CONSTRAINT system_embeddings_document_id_key UNIQUE (document_id);


--
-- Name: system_embeddings system_embeddings_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.system_embeddings
    ADD CONSTRAINT system_embeddings_pkey PRIMARY KEY (id);


--
-- Name: user_navigation_overrides user_navigation_overrides_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.user_navigation_overrides
    ADD CONSTRAINT user_navigation_overrides_pkey PRIMARY KEY (id);


--
-- Name: users users_email_unique; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_unique UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_username_key; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_username_key UNIQUE (username);


--
-- Name: users users_username_unique; Type: CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_username_unique UNIQUE (username);


--
-- Name: IDX_session_expire; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX "IDX_session_expire" ON public.session USING btree (expire);


--
-- Name: business_units_company_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX business_units_company_idx ON public.business_units USING btree (company_id);


--
-- Name: business_units_name_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX business_units_name_idx ON public.business_units USING btree (org_name);


--
-- Name: business_units_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX business_units_status_idx ON public.business_units USING btree (status);


--
-- Name: companies_name_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX companies_name_idx ON public.companies USING btree (company_name);


--
-- Name: companies_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX companies_status_idx ON public.companies USING btree (status);


--
-- Name: contract_approvals_version_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX contract_approvals_version_idx ON public.contract_approvals USING btree (contract_version_id);


--
-- Name: contract_documents_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX contract_documents_contract_idx ON public.contract_documents USING btree (contract_id);


--
-- Name: contract_documents_extraction_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX contract_documents_extraction_idx ON public.contract_documents USING btree (extraction_run_id);


--
-- Name: contract_embeddings_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX contract_embeddings_contract_idx ON public.contract_embeddings USING btree (contract_id);


--
-- Name: contract_embeddings_embedding_hnsw_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX contract_embeddings_embedding_hnsw_idx ON public.contract_embeddings USING hnsw (embedding public.vector_cosine_ops);


--
-- Name: contract_embeddings_type_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX contract_embeddings_type_idx ON public.contract_embeddings USING btree (embedding_type);


--
-- Name: contract_versions_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX contract_versions_contract_idx ON public.contract_versions USING btree (contract_id);


--
-- Name: contract_versions_state_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX contract_versions_state_idx ON public.contract_versions USING btree (approval_state);


--
-- Name: data_import_jobs_customer_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX data_import_jobs_customer_idx ON public.data_import_jobs USING btree (customer_id);


--
-- Name: data_import_jobs_mapping_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX data_import_jobs_mapping_idx ON public.data_import_jobs USING btree (mapping_id);


--
-- Name: data_import_jobs_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX data_import_jobs_status_idx ON public.data_import_jobs USING btree (status);


--
-- Name: demo_requests_email_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX demo_requests_email_idx ON public.demo_requests USING btree (email);


--
-- Name: demo_requests_plan_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX demo_requests_plan_idx ON public.demo_requests USING btree (plan_tier);


--
-- Name: demo_requests_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX demo_requests_status_idx ON public.demo_requests USING btree (status);


--
-- Name: early_access_email_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX early_access_email_idx ON public.early_access_signups USING btree (email);


--
-- Name: early_access_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX early_access_status_idx ON public.early_access_signups USING btree (status);


--
-- Name: erp_entities_system_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX erp_entities_system_idx ON public.erp_entities USING btree (system_id);


--
-- Name: erp_entities_type_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX erp_entities_type_idx ON public.erp_entities USING btree (entity_type);


--
-- Name: erp_fields_entity_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX erp_fields_entity_idx ON public.erp_fields USING btree (entity_id);


--
-- Name: erp_systems_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX erp_systems_status_idx ON public.erp_systems USING btree (status);


--
-- Name: erp_systems_vendor_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX erp_systems_vendor_idx ON public.erp_systems USING btree (vendor);


--
-- Name: extraction_runs_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX extraction_runs_contract_idx ON public.extraction_runs USING btree (contract_id);


--
-- Name: extraction_runs_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX extraction_runs_status_idx ON public.extraction_runs USING btree (status);


--
-- Name: field_mappings_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX field_mappings_contract_idx ON public.sales_field_mappings USING btree (contract_id);


--
-- Name: field_mappings_source_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX field_mappings_source_idx ON public.sales_field_mappings USING btree (source_field_name);


--
-- Name: graph_edges_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX graph_edges_contract_idx ON public.contract_graph_edges USING btree (contract_id);


--
-- Name: graph_edges_source_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX graph_edges_source_idx ON public.contract_graph_edges USING btree (source_node_id);


--
-- Name: graph_edges_target_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX graph_edges_target_idx ON public.contract_graph_edges USING btree (target_node_id);


--
-- Name: graph_nodes_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX graph_nodes_contract_idx ON public.contract_graph_nodes USING btree (contract_id);


--
-- Name: graph_nodes_extraction_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX graph_nodes_extraction_idx ON public.contract_graph_nodes USING btree (extraction_run_id);


--
-- Name: graph_nodes_type_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX graph_nodes_type_idx ON public.contract_graph_nodes USING btree (node_type);


--
-- Name: imported_records_customer_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX imported_records_customer_idx ON public.imported_erp_records USING btree (customer_id);


--
-- Name: imported_records_embedding_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX imported_records_embedding_idx ON public.imported_erp_records USING hnsw (embedding public.vector_cosine_ops);


--
-- Name: imported_records_job_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX imported_records_job_idx ON public.imported_erp_records USING btree (job_id);


--
-- Name: imported_records_mapping_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX imported_records_mapping_idx ON public.imported_erp_records USING btree (mapping_id);


--
-- Name: licenseiq_fields_entity_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX licenseiq_fields_entity_idx ON public.licenseiq_fields USING btree (entity_id);


--
-- Name: licenseiq_records_entity_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX licenseiq_records_entity_idx ON public.licenseiq_entity_records USING btree (entity_id);


--
-- Name: licenseiq_records_grp_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX licenseiq_records_grp_idx ON public.licenseiq_entity_records USING btree (grp_id);


--
-- Name: licenseiq_records_loc_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX licenseiq_records_loc_idx ON public.licenseiq_entity_records USING btree (loc_id);


--
-- Name: licenseiq_records_org_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX licenseiq_records_org_idx ON public.licenseiq_entity_records USING btree (org_id);


--
-- Name: locations_company_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX locations_company_idx ON public.locations USING btree (company_id);


--
-- Name: locations_name_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX locations_name_idx ON public.locations USING btree (loc_name);


--
-- Name: locations_org_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX locations_org_idx ON public.locations USING btree (org_id);


--
-- Name: locations_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX locations_status_idx ON public.locations USING btree (status);


--
-- Name: master_data_mappings_customer_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX master_data_mappings_customer_idx ON public.master_data_mappings USING btree (customer_id);


--
-- Name: master_data_mappings_entity_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX master_data_mappings_entity_idx ON public.master_data_mappings USING btree (entity_type);


--
-- Name: master_data_mappings_erp_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX master_data_mappings_erp_idx ON public.master_data_mappings USING btree (erp_system);


--
-- Name: master_data_mappings_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX master_data_mappings_status_idx ON public.master_data_mappings USING btree (status);


--
-- Name: nav_perm_item_key_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX nav_perm_item_key_idx ON public.navigation_permissions USING btree (item_key);


--
-- Name: review_tasks_assigned_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX review_tasks_assigned_idx ON public.human_review_tasks USING btree (assigned_to);


--
-- Name: review_tasks_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX review_tasks_contract_idx ON public.human_review_tasks USING btree (contract_id);


--
-- Name: review_tasks_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX review_tasks_status_idx ON public.human_review_tasks USING btree (status);


--
-- Name: role_nav_perm_item_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX role_nav_perm_item_idx ON public.role_navigation_permissions USING btree (nav_item_key);


--
-- Name: role_nav_perm_role_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX role_nav_perm_role_idx ON public.role_navigation_permissions USING btree (role);


--
-- Name: roles_name_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX roles_name_idx ON public.roles USING btree (role_name);


--
-- Name: rule_definitions_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX rule_definitions_contract_idx ON public.rule_definitions USING btree (contract_id);


--
-- Name: rule_definitions_status_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX rule_definitions_status_idx ON public.rule_definitions USING btree (validation_status);


--
-- Name: semantic_index_contract_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX semantic_index_contract_idx ON public.semantic_index_entries USING btree (contract_id);


--
-- Name: semantic_index_type_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX semantic_index_type_idx ON public.semantic_index_entries USING btree (index_type);


--
-- Name: system_embeddings_category_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX system_embeddings_category_idx ON public.system_embeddings USING btree (category);


--
-- Name: system_embeddings_document_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX system_embeddings_document_idx ON public.system_embeddings USING btree (document_id);


--
-- Name: system_embeddings_embedding_hnsw_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX system_embeddings_embedding_hnsw_idx ON public.system_embeddings USING hnsw (embedding public.vector_cosine_ops);


--
-- Name: user_nav_override_item_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX user_nav_override_item_idx ON public.user_navigation_overrides USING btree (nav_item_key);


--
-- Name: user_nav_override_user_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX user_nav_override_user_idx ON public.user_navigation_overrides USING btree (user_id);


--
-- Name: validation_events_rule_idx; Type: INDEX; Schema: public; Owner: neondb_owner
--

CREATE INDEX validation_events_rule_idx ON public.rule_validation_events USING btree (rule_definition_id);


--
-- Name: audit_trail audit_trail_user_id_users_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.audit_trail
    ADD CONSTRAINT audit_trail_user_id_users_id_fk FOREIGN KEY (user_id) REFERENCES public.users(id);


--
-- Name: business_units business_units_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.business_units
    ADD CONSTRAINT business_units_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(company_id) ON DELETE CASCADE;


--
-- Name: business_units business_units_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.business_units
    ADD CONSTRAINT business_units_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: business_units business_units_last_updated_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.business_units
    ADD CONSTRAINT business_units_last_updated_by_fkey FOREIGN KEY (last_updated_by) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: companies companies_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: companies companies_last_updated_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_last_updated_by_fkey FOREIGN KEY (last_updated_by) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: compliance_analysis compliance_analysis_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.compliance_analysis
    ADD CONSTRAINT compliance_analysis_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_analysis contract_analysis_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_analysis
    ADD CONSTRAINT contract_analysis_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_approvals contract_approvals_approver_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_approvals
    ADD CONSTRAINT contract_approvals_approver_id_fkey FOREIGN KEY (approver_id) REFERENCES public.users(id);


--
-- Name: contract_approvals contract_approvals_contract_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_approvals
    ADD CONSTRAINT contract_approvals_contract_version_id_fkey FOREIGN KEY (contract_version_id) REFERENCES public.contract_versions(id) ON DELETE CASCADE;


--
-- Name: contract_comparisons contract_comparisons_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_comparisons
    ADD CONSTRAINT contract_comparisons_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_documents contract_documents_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_documents
    ADD CONSTRAINT contract_documents_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_embeddings contract_embeddings_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_embeddings
    ADD CONSTRAINT contract_embeddings_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_graph_edges contract_graph_edges_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_graph_edges
    ADD CONSTRAINT contract_graph_edges_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_graph_edges contract_graph_edges_source_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_graph_edges
    ADD CONSTRAINT contract_graph_edges_source_node_id_fkey FOREIGN KEY (source_node_id) REFERENCES public.contract_graph_nodes(id);


--
-- Name: contract_graph_edges contract_graph_edges_target_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_graph_edges
    ADD CONSTRAINT contract_graph_edges_target_node_id_fkey FOREIGN KEY (target_node_id) REFERENCES public.contract_graph_nodes(id);


--
-- Name: contract_graph_nodes contract_graph_nodes_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_graph_nodes
    ADD CONSTRAINT contract_graph_nodes_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_graph_nodes contract_graph_nodes_source_document_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_graph_nodes
    ADD CONSTRAINT contract_graph_nodes_source_document_id_fkey FOREIGN KEY (source_document_id) REFERENCES public.contract_documents(id);


--
-- Name: contract_obligations contract_obligations_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_obligations
    ADD CONSTRAINT contract_obligations_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_royalty_calculations contract_royalty_calculations_approved_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_royalty_calculations
    ADD CONSTRAINT contract_royalty_calculations_approved_by_fkey FOREIGN KEY (approved_by) REFERENCES public.users(id);


--
-- Name: contract_royalty_calculations contract_royalty_calculations_calculated_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_royalty_calculations
    ADD CONSTRAINT contract_royalty_calculations_calculated_by_fkey FOREIGN KEY (calculated_by) REFERENCES public.users(id);


--
-- Name: contract_royalty_calculations contract_royalty_calculations_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_royalty_calculations
    ADD CONSTRAINT contract_royalty_calculations_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_royalty_calculations contract_royalty_calculations_rejected_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_royalty_calculations
    ADD CONSTRAINT contract_royalty_calculations_rejected_by_fkey FOREIGN KEY (rejected_by) REFERENCES public.users(id);


--
-- Name: contract_versions contract_versions_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_versions
    ADD CONSTRAINT contract_versions_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: contract_versions contract_versions_editor_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contract_versions
    ADD CONSTRAINT contract_versions_editor_id_fkey FOREIGN KEY (editor_id) REFERENCES public.users(id);


--
-- Name: contracts contracts_contract_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contracts
    ADD CONSTRAINT contracts_contract_owner_id_fkey FOREIGN KEY (contract_owner_id) REFERENCES public.users(id);


--
-- Name: contracts contracts_uploaded_by_users_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.contracts
    ADD CONSTRAINT contracts_uploaded_by_users_id_fk FOREIGN KEY (uploaded_by) REFERENCES public.users(id);


--
-- Name: data_import_jobs data_import_jobs_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.data_import_jobs
    ADD CONSTRAINT data_import_jobs_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- Name: data_import_jobs data_import_jobs_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.data_import_jobs
    ADD CONSTRAINT data_import_jobs_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.contracts(id);


--
-- Name: data_import_jobs data_import_jobs_mapping_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.data_import_jobs
    ADD CONSTRAINT data_import_jobs_mapping_id_fkey FOREIGN KEY (mapping_id) REFERENCES public.master_data_mappings(id) ON DELETE CASCADE;


--
-- Name: erp_entities erp_entities_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.erp_entities
    ADD CONSTRAINT erp_entities_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- Name: erp_entities erp_entities_system_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.erp_entities
    ADD CONSTRAINT erp_entities_system_id_fkey FOREIGN KEY (system_id) REFERENCES public.erp_systems(id) ON DELETE CASCADE;


--
-- Name: erp_fields erp_fields_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.erp_fields
    ADD CONSTRAINT erp_fields_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.erp_entities(id) ON DELETE CASCADE;


--
-- Name: erp_systems erp_systems_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.erp_systems
    ADD CONSTRAINT erp_systems_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- Name: extraction_runs extraction_runs_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.extraction_runs
    ADD CONSTRAINT extraction_runs_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: extraction_runs extraction_runs_triggered_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.extraction_runs
    ADD CONSTRAINT extraction_runs_triggered_by_fkey FOREIGN KEY (triggered_by) REFERENCES public.users(id);


--
-- Name: financial_analysis financial_analysis_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.financial_analysis
    ADD CONSTRAINT financial_analysis_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: human_review_tasks human_review_tasks_assigned_to_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.human_review_tasks
    ADD CONSTRAINT human_review_tasks_assigned_to_fkey FOREIGN KEY (assigned_to) REFERENCES public.users(id);


--
-- Name: human_review_tasks human_review_tasks_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.human_review_tasks
    ADD CONSTRAINT human_review_tasks_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: human_review_tasks human_review_tasks_extraction_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.human_review_tasks
    ADD CONSTRAINT human_review_tasks_extraction_run_id_fkey FOREIGN KEY (extraction_run_id) REFERENCES public.extraction_runs(id);


--
-- Name: human_review_tasks human_review_tasks_reviewed_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.human_review_tasks
    ADD CONSTRAINT human_review_tasks_reviewed_by_fkey FOREIGN KEY (reviewed_by) REFERENCES public.users(id);


--
-- Name: imported_erp_records imported_erp_records_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.imported_erp_records
    ADD CONSTRAINT imported_erp_records_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.contracts(id);


--
-- Name: imported_erp_records imported_erp_records_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.imported_erp_records
    ADD CONSTRAINT imported_erp_records_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.data_import_jobs(id) ON DELETE CASCADE;


--
-- Name: imported_erp_records imported_erp_records_mapping_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.imported_erp_records
    ADD CONSTRAINT imported_erp_records_mapping_id_fkey FOREIGN KEY (mapping_id) REFERENCES public.master_data_mappings(id);


--
-- Name: licenseiq_entity_records licenseiq_entity_records_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_entity_records
    ADD CONSTRAINT licenseiq_entity_records_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- Name: licenseiq_entity_records licenseiq_entity_records_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_entity_records
    ADD CONSTRAINT licenseiq_entity_records_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.licenseiq_entities(id) ON DELETE CASCADE;


--
-- Name: licenseiq_entity_records licenseiq_entity_records_grp_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_entity_records
    ADD CONSTRAINT licenseiq_entity_records_grp_id_fkey FOREIGN KEY (grp_id) REFERENCES public.companies(company_id) ON DELETE RESTRICT;


--
-- Name: licenseiq_entity_records licenseiq_entity_records_loc_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_entity_records
    ADD CONSTRAINT licenseiq_entity_records_loc_id_fkey FOREIGN KEY (loc_id) REFERENCES public.locations(loc_id) ON DELETE RESTRICT;


--
-- Name: licenseiq_entity_records licenseiq_entity_records_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_entity_records
    ADD CONSTRAINT licenseiq_entity_records_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.business_units(org_id) ON DELETE RESTRICT;


--
-- Name: licenseiq_fields licenseiq_fields_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.licenseiq_fields
    ADD CONSTRAINT licenseiq_fields_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.licenseiq_entities(id) ON DELETE CASCADE;


--
-- Name: locations locations_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(company_id) ON DELETE CASCADE;


--
-- Name: locations locations_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: locations locations_last_updated_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_last_updated_by_fkey FOREIGN KEY (last_updated_by) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: locations locations_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.business_units(org_id) ON DELETE CASCADE;


--
-- Name: master_data_mappings master_data_mappings_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.master_data_mappings
    ADD CONSTRAINT master_data_mappings_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- Name: master_data_mappings master_data_mappings_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.master_data_mappings
    ADD CONSTRAINT master_data_mappings_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.contracts(id);


--
-- Name: performance_metrics performance_metrics_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.performance_metrics
    ADD CONSTRAINT performance_metrics_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: role_navigation_permissions role_navigation_permissions_nav_item_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.role_navigation_permissions
    ADD CONSTRAINT role_navigation_permissions_nav_item_key_fkey FOREIGN KEY (nav_item_key) REFERENCES public.navigation_permissions(item_key) ON DELETE CASCADE;


--
-- Name: royalty_rules royalty_rules_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.royalty_rules
    ADD CONSTRAINT royalty_rules_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: rule_definitions rule_definitions_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_definitions
    ADD CONSTRAINT rule_definitions_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: rule_definitions rule_definitions_extraction_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_definitions
    ADD CONSTRAINT rule_definitions_extraction_run_id_fkey FOREIGN KEY (extraction_run_id) REFERENCES public.extraction_runs(id);


--
-- Name: rule_definitions rule_definitions_linked_graph_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_definitions
    ADD CONSTRAINT rule_definitions_linked_graph_node_id_fkey FOREIGN KEY (linked_graph_node_id) REFERENCES public.contract_graph_nodes(id);


--
-- Name: rule_validation_events rule_validation_events_rule_definition_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_validation_events
    ADD CONSTRAINT rule_validation_events_rule_definition_id_fkey FOREIGN KEY (rule_definition_id) REFERENCES public.rule_definitions(id);


--
-- Name: rule_validation_events rule_validation_events_validator_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.rule_validation_events
    ADD CONSTRAINT rule_validation_events_validator_id_fkey FOREIGN KEY (validator_id) REFERENCES public.users(id);


--
-- Name: sales_data sales_data_matched_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.sales_data
    ADD CONSTRAINT sales_data_matched_contract_id_contracts_id_fk FOREIGN KEY (matched_contract_id) REFERENCES public.contracts(id) ON DELETE SET NULL;


--
-- Name: sales_field_mappings sales_field_mappings_approved_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.sales_field_mappings
    ADD CONSTRAINT sales_field_mappings_approved_by_fkey FOREIGN KEY (approved_by) REFERENCES public.users(id);


--
-- Name: sales_field_mappings sales_field_mappings_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.sales_field_mappings
    ADD CONSTRAINT sales_field_mappings_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE SET NULL;


--
-- Name: semantic_index_entries semantic_index_entries_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.semantic_index_entries
    ADD CONSTRAINT semantic_index_entries_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: strategic_analysis strategic_analysis_contract_id_contracts_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.strategic_analysis
    ADD CONSTRAINT strategic_analysis_contract_id_contracts_id_fk FOREIGN KEY (contract_id) REFERENCES public.contracts(id) ON DELETE CASCADE;


--
-- Name: user_navigation_overrides user_navigation_overrides_nav_item_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.user_navigation_overrides
    ADD CONSTRAINT user_navigation_overrides_nav_item_key_fkey FOREIGN KEY (nav_item_key) REFERENCES public.navigation_permissions(item_key) ON DELETE CASCADE;


--
-- Name: user_navigation_overrides user_navigation_overrides_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: neondb_owner
--

ALTER TABLE ONLY public.user_navigation_overrides
    ADD CONSTRAINT user_navigation_overrides_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: neondb_owner
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: cloud_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE cloud_admin IN SCHEMA public GRANT ALL ON SEQUENCES TO neon_superuser WITH GRANT OPTION;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: cloud_admin
--

ALTER DEFAULT PRIVILEGES FOR ROLE cloud_admin IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO neon_superuser WITH GRANT OPTION;


--
-- PostgreSQL database dump complete
--

