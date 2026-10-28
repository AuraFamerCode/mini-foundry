//! Integrity primitives for physical object and link modeling.
//!
//! This crate is a pure validation/planning core, not a persistence adapter.
//! A caller must execute a returned primary-key migration plan together with
//! edit/writeback purges, index rebuild, and ontology publication in one
//! authoritative durable transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Stable identity token for a primary-key value, exposed for callers that
/// deduplicate rows across service boundaries (canonical decimal identity:
/// `+1`, `1` and `1.00` produce the same token).
pub fn canonical_primary_key_token(value: &str, data_type: DataType) -> Result<String, ModelError> {
    primary_key_token(value, data_type)
}

fn primary_key_property(object_type: &ObjectType) -> Result<&Property, ModelError> {
    let mut matches = object_type
        .properties
        .iter()
        .filter(|property| property.name == object_type.primary_key);
    let property = matches.next().ok_or_else(|| {
        ModelError::new(
            "BAD_OBJECT_TYPE",
            format!(
                "primary key '{}' is not a declared property",
                object_type.primary_key
            ),
        )
    })?;
    if matches.next().is_some() {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            format!(
                "primary key property '{}' is duplicated",
                object_type.primary_key
            ),
        ));
    }
    Ok(property)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelError {
    pub code: &'static str,
    pub message: String,
}

impl ModelError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ModelError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String,
    Integer,
    Number,
    Boolean,
    Date,
    Timestamp,
    Json,
}

impl DataType {
    fn is_supported_key(self) -> bool {
        matches!(self, Self::String | Self::Integer | Self::Number)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Property {
    pub name: String,
    pub data_type: DataType,
    /// `None` represents an action-only or otherwise non-physical property.
    pub backing_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// Connectors canonicalize cell values before passing them to this core.
    /// A missing map entry is null. JSON text must be syntax-validated by the
    /// connector before the row is constructed.
    pub values: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectType {
    pub name: String,
    pub backing_dataset: Option<String>,
    pub primary_key: String,
    pub title_key: String,
    pub properties: Vec<Property>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkBacking {
    ForeignKey {
        source_type: String,
        fk_property: String,
        target_type: String,
    },
    JoinTable {
        dataset: String,
        left_column: String,
        right_column: String,
    },
    ObjectBacked {
        manifest_type: String,
        left_property: String,
        right_property: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    OneToOne,
    ManyToOne,
    OneToMany,
    ManyToMany,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkType {
    pub name: String,
    pub from_type: String,
    pub to_type: String,
    pub cardinality: Cardinality,
    pub backing: LinkBacking,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKeyChangePlan {
    pub object_type: String,
    pub old_key: String,
    pub new_key: String,
    pub edit_dataset: String,
    pub writeback_dataset: Option<String>,
    pub links_revalidated: Vec<String>,
    pub destructive_confirmation_required: bool,
}

/// Complete context required to validate a destructive identity change.
#[derive(Debug, Clone, Copy)]
pub struct PhysicalModel<'a> {
    pub object_types: &'a [ObjectType],
    pub links: &'a [LinkType],
    pub datasets: &'a [DatasetSchema],
}

/// Validate explicit identity/display keys, one physical backing dataset,
/// property-to-column contracts, row types, and unique non-null object keys.
pub fn validate_object_type(
    object_type: &ObjectType,
    dataset: Option<&DatasetSchema>,
) -> Result<(), ModelError> {
    if object_type.name.trim().is_empty() {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            "object type name is required",
        ));
    }
    if object_type.primary_key.trim().is_empty() || object_type.title_key.trim().is_empty() {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            "primary key and title key must both be explicit",
        ));
    }
    if object_type.properties.is_empty() {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            "object type must declare at least one property",
        ));
    }

    let mut property_names = BTreeSet::new();
    for property in &object_type.properties {
        if property.name.trim().is_empty() || !property_names.insert(property.name.as_str()) {
            return Err(ModelError::new(
                "BAD_OBJECT_TYPE",
                format!("property name '{}' is empty or duplicated", property.name),
            ));
        }
    }
    if !property_names.contains(object_type.primary_key.as_str()) {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            format!(
                "primary key '{}' is not a declared property",
                object_type.primary_key
            ),
        ));
    }
    if !property_names.contains(object_type.title_key.as_str()) {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            format!(
                "title key '{}' is not a declared property",
                object_type.title_key
            ),
        ));
    }
    if !primary_key_property(object_type)?
        .data_type
        .is_supported_key()
    {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            "primary key property must be string, integer, or number",
        ));
    }

    match (&object_type.backing_dataset, dataset) {
        (None, None) => return Ok(()),
        (Some(expected), Some(actual)) if expected == &actual.name => {}
        (Some(expected), Some(actual)) => {
            return Err(ModelError::new(
                "ONE_DATASET_PER_OBJECT_TYPE",
                format!(
                    "'{}' is backed by '{}', not '{}'",
                    object_type.name, actual.name, expected
                ),
            ));
        }
        (Some(expected), None) => {
            return Err(ModelError::new(
                "NOT_FOUND",
                format!("backing dataset '{}' was not supplied", expected),
            ));
        }
        (None, Some(_)) => {
            return Err(ModelError::new(
                "ONE_DATASET_PER_OBJECT_TYPE",
                "an action-populated object type cannot implicitly acquire a backing dataset",
            ));
        }
    }

    let Some(dataset) = dataset else {
        return Ok(());
    };
    let columns = column_map(dataset)?;
    for property in &object_type.properties {
        let column_name = match property.backing_column.as_deref() {
            Some(name) => name,
            None if property.name == object_type.primary_key
                || property.name == object_type.title_key =>
            {
                return Err(ModelError::new(
                    "BAD_OBJECT_TYPE",
                    format!(
                        "special property '{}' must map to a physical column",
                        property.name
                    ),
                ));
            }
            None => continue,
        };
        let physical_type = columns.get(column_name).ok_or_else(|| {
            ModelError::new(
                "OntologyMetadata:MissingBackingColumn",
                format!(
                    "property '{}' maps to missing column '{}'",
                    property.name, column_name
                ),
            )
        })?;
        if property.data_type != *physical_type {
            return Err(ModelError::new(
                "OntologyMetadata:IncompatibleFoundryFieldSchemaForPropertyType",
                format!(
                    "column '{}' is {:?}, property '{}' is declared {:?}",
                    column_name, physical_type, property.name, property.data_type
                ),
            ));
        }
    }
    validate_dataset_rows(dataset)?;
    validate_primary_key_values(object_type, dataset)
}

/// Enforce nonempty, typed, unique primary-key values for every backing row.
pub fn validate_primary_keys(
    object_type: &ObjectType,
    dataset: &DatasetSchema,
) -> Result<(), ModelError> {
    if object_type.backing_dataset.as_deref() != Some(dataset.name.as_str()) {
        return Err(ModelError::new(
            "ONE_DATASET_PER_OBJECT_TYPE",
            format!(
                "dataset '{}' is not the backing dataset for '{}'",
                dataset.name, object_type.name
            ),
        ));
    }
    let property = primary_key_property(object_type)?;
    if !property.data_type.is_supported_key() {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            "primary key property must be string, integer, or number",
        ));
    }
    let column = property.backing_column.as_deref().ok_or_else(|| {
        ModelError::new(
            "BAD_OBJECT_TYPE",
            "primary key must map to a physical column",
        )
    })?;
    let columns = validate_dataset_rows(dataset)?;
    match columns.get(column) {
        Some(data_type) if *data_type == property.data_type => {}
        Some(_) => {
            return Err(ModelError::new(
                "OntologyMetadata:IncompatibleFoundryFieldSchemaForPropertyType",
                format!(
                    "primary key column '{}' does not match its declared property type",
                    column
                ),
            ));
        }
        None => {
            return Err(ModelError::new(
                "OntologyMetadata:MissingBackingColumn",
                format!("primary key maps to missing column '{}'", column),
            ));
        }
    }
    validate_primary_key_values(object_type, dataset)
}

fn validate_primary_key_values(
    object_type: &ObjectType,
    dataset: &DatasetSchema,
) -> Result<(), ModelError> {
    let property = primary_key_property(object_type)?;
    let column = property.backing_column.as_deref().ok_or_else(|| {
        ModelError::new(
            "BAD_OBJECT_TYPE",
            "primary key must map to a physical column",
        )
    })?;
    let mut seen = BTreeSet::new();
    for (row_index, row) in dataset.rows.iter().enumerate() {
        let key = row.values.get(column).ok_or_else(|| {
            ModelError::new(
                "OntologyMetadata:MissingPrimaryKeyValue",
                format!(
                    "row {} has no value for key column '{}'",
                    row_index + 1,
                    column
                ),
            )
        })?;
        if key.is_empty() {
            return Err(ModelError::new(
                "OntologyMetadata:MissingPrimaryKeyValue",
                format!(
                    "row {} has an empty key in column '{}'",
                    row_index + 1,
                    column
                ),
            ));
        }
        let identity = primary_key_token(key, property.data_type)?;
        if !seen.insert(identity) {
            return Err(ModelError::new(
                "OntologyMetadata:DuplicatePrimaryKey",
                format!(
                    "duplicate primary key in dataset '{}' at row {}",
                    dataset.name,
                    row_index + 1
                ),
            ));
        }
    }
    Ok(())
}

/// Validate the link's schema, cardinality, and physical references. A valid
/// schema with a dangling FK/join endpoint is rejected rather than indexed.
pub fn validate_link(
    link: &LinkType,
    object_types: &[ObjectType],
    datasets: &[DatasetSchema],
) -> Result<(), ModelError> {
    validate_link_schema(link, object_types, datasets)?;
    validate_link_references(link, object_types, datasets)
}

/// Validate the complete in-memory ontology snapshot, including uniqueness of
/// names and every declared dataset, object type, and link.
pub fn validate_physical_model(model: PhysicalModel<'_>) -> Result<(), ModelError> {
    let mut object_type_names = BTreeSet::new();
    for object_type in model.object_types {
        if object_type.name.trim().is_empty() {
            return Err(ModelError::new(
                "BAD_OBJECT_TYPE",
                "object type name is required",
            ));
        }
        if !object_type_names.insert(object_type.name.as_str()) {
            return Err(ModelError::new(
                "BAD_ONTOLOGY",
                format!(
                    "object type '{}' is declared more than once",
                    object_type.name
                ),
            ));
        }
    }

    let mut dataset_names = BTreeSet::new();
    for dataset in model.datasets {
        if !dataset_names.insert(dataset.name.as_str()) {
            return Err(ModelError::new(
                "BAD_ONTOLOGY",
                format!("dataset '{}' is declared more than once", dataset.name),
            ));
        }
        validate_dataset_rows(dataset)?;
    }

    for object_type in model.object_types {
        let dataset = match object_type.backing_dataset.as_deref() {
            Some(name) => Some(dataset_named(model.datasets, name)?),
            None => None,
        };
        validate_object_type(object_type, dataset)?;
    }

    let mut link_names = BTreeSet::new();
    for link in model.links {
        if link.name.trim().is_empty() {
            return Err(ModelError::new("BAD_LINK", "link name is required"));
        }
        if !link_names.insert(link.name.as_str()) {
            return Err(ModelError::new(
                "BAD_ONTOLOGY",
                format!("link '{}' is declared more than once", link.name),
            ));
        }
        validate_link(link, model.object_types, model.datasets)?;
    }
    Ok(())
}

fn validate_link_schema(
    link: &LinkType,
    object_types: &[ObjectType],
    datasets: &[DatasetSchema],
) -> Result<(), ModelError> {
    if link.name.trim().is_empty() {
        return Err(ModelError::new("BAD_LINK", "link name is required"));
    }
    let from = object_type_named(object_types, &link.from_type)?;
    let to = object_type_named(object_types, &link.to_type)?;

    match &link.backing {
        LinkBacking::ForeignKey {
            source_type,
            fk_property,
            target_type,
        } => {
            if source_type != &link.from_type || target_type != &link.to_type {
                return Err(ModelError::new(
                    "BAD_LINK",
                    "FK backing endpoints must match the link endpoints",
                ));
            }
            if !matches!(
                link.cardinality,
                Cardinality::OneToOne | Cardinality::ManyToOne
            ) {
                return Err(ModelError::new(
                    "BAD_LINK",
                    "a single-column foreign key supports one-to-one or many-to-one cardinality",
                ));
            }
            let source_dataset = physical_dataset(from, datasets)?;
            let target_dataset = physical_dataset(to, datasets)?;
            let fk = property_named(from, fk_property)?;
            let fk_column_name = fk.backing_column.as_deref().ok_or_else(|| {
                ModelError::new(
                    "BAD_LINK",
                    format!(
                        "FK property '{}' has no physical backing column",
                        fk_property
                    ),
                )
            })?;
            let source_columns = column_map(source_dataset)?;
            let fk_column = source_columns.get(fk_column_name).ok_or_else(|| {
                ModelError::new(
                    "BAD_LINK_SCHEMA",
                    format!("FK backing column '{}' does not exist", fk_column_name),
                )
            })?;
            let target_key = primary_key_property(to)?;
            let target_column_name = target_key.backing_column.as_deref().ok_or_else(|| {
                ModelError::new(
                    "BAD_OBJECT_TYPE",
                    "target primary key must map to a physical column",
                )
            })?;
            let target_columns = column_map(target_dataset)?;
            let target_column = target_columns.get(target_column_name).ok_or_else(|| {
                ModelError::new(
                    "BAD_LINK_SCHEMA",
                    "target primary key backing column is missing",
                )
            })?;
            if fk.data_type != target_key.data_type
                || fk.data_type != *fk_column
                || fk.data_type != *target_column
            {
                return Err(ModelError::new("BAD_LINK_SCHEMA", "FK, physical FK column, and target primary key must have the same declared type"));
            }
        }
        LinkBacking::JoinTable {
            dataset,
            left_column,
            right_column,
        } => {
            if object_types
                .iter()
                .any(|object_type| object_type.backing_dataset.as_deref() == Some(dataset))
            {
                return Err(ModelError::new(
                    "BAD_LINK_SCHEMA",
                    "a join-table dataset cannot also be an object backing dataset",
                ));
            }
            let join = dataset_named(datasets, dataset)?;
            validate_dataset_rows(join)?;
            validate_endpoint_column(join, left_column, from)?;
            validate_endpoint_column(join, right_column, to)?;
        }
        LinkBacking::ObjectBacked {
            manifest_type,
            left_property,
            right_property,
        } => {
            let manifest_type = object_type_named(object_types, manifest_type)?;
            let manifest = physical_dataset(manifest_type, datasets)?;
            validate_manifest_endpoint(manifest_type, left_property, from)?;
            validate_manifest_endpoint(manifest_type, right_property, to)?;
            let left = property_named(manifest_type, left_property)?;
            let right = property_named(manifest_type, right_property)?;
            let manifest_columns = column_map(manifest)?;
            for property in [left, right] {
                let column = property.backing_column.as_deref().ok_or_else(|| {
                    ModelError::new(
                        "BAD_LINK_SCHEMA",
                        "object-backed endpoint property has no backing column",
                    )
                })?;
                if !manifest_columns.contains_key(column) {
                    return Err(ModelError::new(
                        "BAD_LINK_SCHEMA",
                        format!("manifest endpoint column '{}' does not exist", column),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_link_references(
    link: &LinkType,
    object_types: &[ObjectType],
    datasets: &[DatasetSchema],
) -> Result<(), ModelError> {
    let from = object_type_named(object_types, &link.from_type)?;
    let to = object_type_named(object_types, &link.to_type)?;
    match &link.backing {
        LinkBacking::ForeignKey { fk_property, .. } => {
            let source = physical_dataset(from, datasets)?;
            let target = physical_dataset(to, datasets)?;
            let fk = property_named(from, fk_property)?;
            let fk_column = fk.backing_column.as_deref().ok_or_else(|| {
                ModelError::new(
                    "BAD_LINK",
                    format!("FK property '{}' has no backing column", fk_property),
                )
            })?;
            let target_key = primary_key_property(to)?;
            let target_column = target_key.backing_column.as_deref().ok_or_else(|| {
                ModelError::new(
                    "BAD_OBJECT_TYPE",
                    "target primary key has no backing column",
                )
            })?;
            let target_keys = pk_values(to, target)?;
            let mut one_to_one_targets = BTreeSet::new();
            for (row_index, row) in source.rows.iter().enumerate() {
                let Some(value) = row.values.get(fk_column) else {
                    continue;
                };
                let token = primary_key_token(value, fk.data_type)?;
                if !target_keys.contains(&token) {
                    return Err(ModelError::new(
                        "BROKEN_LINK_REFERENCE",
                        format!(
                            "FK '{}' on row {} references a missing key in '{}.{}'",
                            fk_column,
                            row_index + 1,
                            to.name,
                            target_column
                        ),
                    ));
                }
                if link.cardinality == Cardinality::OneToOne && !one_to_one_targets.insert(token) {
                    return Err(ModelError::new(
                        "LINK_CARDINALITY_VIOLATION",
                        format!(
                            "one-to-one link '{}' has multiple sources for one target",
                            link.name
                        ),
                    ));
                }
            }
        }
        LinkBacking::JoinTable {
            dataset,
            left_column,
            right_column,
        } => {
            let join = dataset_named(datasets, dataset)?;
            let from_keys = pk_values(from, physical_dataset(from, datasets)?)?;
            let to_keys = pk_values(to, physical_dataset(to, datasets)?)?;
            let mut pairs = BTreeSet::new();
            let mut from_seen = BTreeSet::new();
            let mut to_seen = BTreeSet::new();
            for (row_index, row) in join.rows.iter().enumerate() {
                let left = endpoint_value(row, left_column, from, row_index)?;
                let right = endpoint_value(row, right_column, to, row_index)?;
                if !from_keys.contains(&left) || !to_keys.contains(&right) {
                    return Err(ModelError::new(
                        "BROKEN_LINK_REFERENCE",
                        format!(
                            "join row {} in '{}' has an endpoint missing from an object dataset",
                            row_index + 1,
                            join.name
                        ),
                    ));
                }
                if !pairs.insert((left.clone(), right.clone())) {
                    return Err(ModelError::new(
                        "DUPLICATE_LINK_REFERENCE",
                        format!(
                            "join dataset '{}' contains a duplicate endpoint pair",
                            join.name
                        ),
                    ));
                }
                validate_cardinality(link, &left, &right, &mut from_seen, &mut to_seen)?;
            }
        }
        LinkBacking::ObjectBacked {
            manifest_type,
            left_property,
            right_property,
        } => {
            let manifest_type = object_type_named(object_types, manifest_type)?;
            let manifest = physical_dataset(manifest_type, datasets)?;
            let left = property_named(manifest_type, left_property)?;
            let right = property_named(manifest_type, right_property)?;
            let left_column = required_column(left, left_property)?;
            let right_column = required_column(right, right_property)?;
            let from_keys = pk_values(from, physical_dataset(from, datasets)?)?;
            let to_keys = pk_values(to, physical_dataset(to, datasets)?)?;
            let mut from_seen = BTreeSet::new();
            let mut to_seen = BTreeSet::new();
            for (row_index, row) in manifest.rows.iter().enumerate() {
                let from_key = endpoint_value(row, left_column, from, row_index)?;
                let to_key = endpoint_value(row, right_column, to, row_index)?;
                if !from_keys.contains(&from_key) || !to_keys.contains(&to_key) {
                    return Err(ModelError::new(
                        "BROKEN_LINK_REFERENCE",
                        format!(
                            "manifest row {} in '{}' points to a missing endpoint",
                            row_index + 1,
                            manifest.name
                        ),
                    ));
                }
                validate_cardinality(link, &from_key, &to_key, &mut from_seen, &mut to_seen)?;
            }
        }
    }
    Ok(())
}

fn validate_cardinality(
    link: &LinkType,
    from_key: &str,
    to_key: &str,
    from_seen: &mut BTreeSet<String>,
    to_seen: &mut BTreeSet<String>,
) -> Result<(), ModelError> {
    // Cardinality is directional from `from_type` to `to_type`. A many-to-one
    // edge has one target per source, while a one-to-many edge has one source
    // per target.
    let unique_from = matches!(
        link.cardinality,
        Cardinality::OneToOne | Cardinality::ManyToOne
    );
    let unique_to = matches!(
        link.cardinality,
        Cardinality::OneToOne | Cardinality::OneToMany
    );
    if unique_from && !from_seen.insert(from_key.to_owned()) {
        return Err(ModelError::new(
            "LINK_CARDINALITY_VIOLATION",
            format!("link '{}' violates source-side cardinality", link.name),
        ));
    }
    if unique_to && !to_seen.insert(to_key.to_owned()) {
        return Err(ModelError::new(
            "LINK_CARDINALITY_VIOLATION",
            format!("link '{}' violates target-side cardinality", link.name),
        ));
    }
    Ok(())
}

fn endpoint_value(
    row: &Row,
    column: &str,
    endpoint: &ObjectType,
    row_index: usize,
) -> Result<String, ModelError> {
    let value = row.values.get(column).ok_or_else(|| {
        ModelError::new(
            "BAD_LINK_REFERENCE",
            format!(
                "link row {} lacks endpoint column '{}'",
                row_index + 1,
                column
            ),
        )
    })?;
    primary_key_token(value, primary_key_property(endpoint)?.data_type)
}

fn validate_endpoint_column(
    dataset: &DatasetSchema,
    column_name: &str,
    endpoint: &ObjectType,
) -> Result<(), ModelError> {
    let column = column_map(dataset)?
        .get(column_name)
        .copied()
        .ok_or_else(|| {
            ModelError::new(
                "BAD_LINK_SCHEMA",
                format!(
                    "dataset '{}' lacks endpoint column '{}'",
                    dataset.name, column_name
                ),
            )
        })?;
    if column != primary_key_property(endpoint)?.data_type {
        return Err(ModelError::new(
            "BAD_LINK_SCHEMA",
            format!(
                "join column '{}' does not exactly match '{}.{}'",
                column_name, endpoint.name, endpoint.primary_key
            ),
        ));
    }
    Ok(())
}

fn validate_manifest_endpoint(
    manifest: &ObjectType,
    property_name: &str,
    endpoint: &ObjectType,
) -> Result<(), ModelError> {
    let property = property_named(manifest, property_name)?;
    if property.data_type != primary_key_property(endpoint)?.data_type {
        return Err(ModelError::new(
            "BAD_LINK_SCHEMA",
            format!(
                "manifest property '{}' does not exactly match endpoint '{}.{}'",
                property_name, endpoint.name, endpoint.primary_key
            ),
        ));
    }
    if property.backing_column.is_none() {
        return Err(ModelError::new(
            "BAD_LINK_SCHEMA",
            format!(
                "manifest property '{}' has no physical backing column",
                property_name
            ),
        ));
    }
    Ok(())
}

fn object_type_named<'a>(
    object_types: &'a [ObjectType],
    name: &str,
) -> Result<&'a ObjectType, ModelError> {
    let mut found = object_types
        .iter()
        .filter(|object_type| object_type.name == name);
    let object_type = found
        .next()
        .ok_or_else(|| ModelError::new("NOT_FOUND", format!("object type '{}' not found", name)))?;
    if found.next().is_some() {
        return Err(ModelError::new(
            "BAD_ONTOLOGY",
            format!("object type '{}' is declared more than once", name),
        ));
    }
    Ok(object_type)
}

fn property_named<'a>(object_type: &'a ObjectType, name: &str) -> Result<&'a Property, ModelError> {
    let mut found = object_type
        .properties
        .iter()
        .filter(|property| property.name == name);
    let property = found.next().ok_or_else(|| {
        ModelError::new(
            "BAD_LINK",
            format!(
                "property '{}' is not declared on '{}'",
                name, object_type.name
            ),
        )
    })?;
    if found.next().is_some() {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            format!(
                "property '{}' is duplicated on '{}'",
                name, object_type.name
            ),
        ));
    }
    Ok(property)
}

fn physical_dataset<'a>(
    object_type: &ObjectType,
    datasets: &'a [DatasetSchema],
) -> Result<&'a DatasetSchema, ModelError> {
    let name = object_type.backing_dataset.as_ref().ok_or_else(|| {
        ModelError::new(
            "PHYSICAL_LINK_REQUIRES_BACKING_DATASET",
            format!(
                "object type '{}' has no physical backing dataset",
                object_type.name
            ),
        )
    })?;
    let mut found = datasets.iter().filter(|dataset| &dataset.name == name);
    let dataset = found.next().ok_or_else(|| {
        ModelError::new("NOT_FOUND", format!("backing dataset '{}' not found", name))
    })?;
    if found.next().is_some() {
        return Err(ModelError::new(
            "BAD_ONTOLOGY",
            format!("dataset '{}' is declared more than once", name),
        ));
    }
    validate_object_type(object_type, Some(dataset))?;
    Ok(dataset)
}

fn dataset_named<'a>(
    datasets: &'a [DatasetSchema],
    name: &str,
) -> Result<&'a DatasetSchema, ModelError> {
    let mut found = datasets.iter().filter(|dataset| dataset.name == name);
    let dataset = found
        .next()
        .ok_or_else(|| ModelError::new("NOT_FOUND", format!("dataset '{}' not found", name)))?;
    if found.next().is_some() {
        return Err(ModelError::new(
            "BAD_ONTOLOGY",
            format!("dataset '{}' is declared more than once", name),
        ));
    }
    Ok(dataset)
}
fn column_map(dataset: &DatasetSchema) -> Result<BTreeMap<&str, DataType>, ModelError> {
    let mut columns = BTreeMap::new();
    for column in &dataset.columns {
        if column.name.trim().is_empty()
            || columns
                .insert(column.name.as_str(), column.data_type)
                .is_some()
        {
            return Err(ModelError::new(
                "BAD_DATASET_SCHEMA",
                format!(
                    "dataset '{}' has an empty or duplicate column name",
                    dataset.name
                ),
            ));
        }
    }
    Ok(columns)
}

fn required_column<'a>(property: &'a Property, label: &str) -> Result<&'a str, ModelError> {
    property.backing_column.as_deref().ok_or_else(|| {
        ModelError::new(
            "BAD_LINK_SCHEMA",
            format!("property '{}' has no physical backing column", label),
        )
    })
}

fn pk_values(
    object_type: &ObjectType,
    dataset: &DatasetSchema,
) -> Result<BTreeSet<String>, ModelError> {
    validate_primary_keys(object_type, dataset)?;
    let property = primary_key_property(object_type)?;
    let column = required_column(property, &property.name)?;
    dataset
        .rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let value = row.values.get(column).ok_or_else(|| {
                ModelError::new(
                    "OntologyMetadata:MissingPrimaryKeyValue",
                    format!("row {} is missing key '{}'", index + 1, column),
                )
            })?;
            primary_key_token(value, property.data_type)
        })
        .collect()
}

fn primary_key_token(value: &str, data_type: DataType) -> Result<String, ModelError> {
    match data_type {
        DataType::String => Ok(format!("s:{}", value)),
        DataType::Integer => value
            .parse::<i64>()
            .map(|number| format!("i:{}", number))
            .map_err(|_| {
                ModelError::new(
                    "BAD_DATASET_VALUE",
                    "primary key is not a signed 64-bit integer",
                )
            }),
        DataType::Number => canonical_decimal(value),
        _ => Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            "primary key must be string, integer, or number",
        )),
    }
}

/// Decimal identity without binary floating-point rounding: 1, 1.0 and 10e-1
/// canonicalize to the same identity token; very large/small numbers do not
/// need to be expanded into enormous strings.
fn canonical_decimal(value: &str) -> Result<String, ModelError> {
    let invalid = || ModelError::new("BAD_DATASET_VALUE", "value is not a finite decimal number");
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if unsigned.is_empty() {
        return Err(invalid());
    }
    let (mantissa, exponent_text) = match unsigned.find(['e', 'E']) {
        Some(index) if unsigned[index + 1..].find(['e', 'E']).is_none() => {
            (&unsigned[..index], Some(&unsigned[index + 1..]))
        }
        Some(_) => return Err(invalid()),
        None => (unsigned, None),
    };
    let exponent: i64 = match exponent_text {
        Some(text) if !text.is_empty() => text.parse().map_err(|_| invalid())?,
        Some(_) => return Err(invalid()),
        None => 0,
    };
    let mut parts = mantissa.split('.');
    let whole = parts.next().ok_or_else(invalid)?;
    let fraction = parts.next();
    if parts.next().is_some()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|digits| !digits.bytes().all(|byte| byte.is_ascii_digit()))
        || (whole.is_empty() && fraction.is_none())
    {
        return Err(invalid());
    }
    let fraction = fraction.unwrap_or("");
    if whole.is_empty() && fraction.is_empty() {
        return Err(invalid());
    }
    let mut digits = format!("{}{}", whole, fraction);
    if digits.is_empty() {
        return Err(invalid());
    }
    let Some(first_nonzero) = digits.find(|character| character != '0') else {
        return Ok("n:0".to_owned());
    };
    digits.drain(..first_nonzero);
    let mut scale = exponent
        .checked_sub(i64::try_from(fraction.len()).map_err(|_| invalid())?)
        .ok_or_else(invalid)?;
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
        scale = scale.checked_add(1).ok_or_else(invalid)?;
    }
    Ok(format!(
        "n:{}{}e{}",
        if negative { "-" } else { "" },
        digits,
        scale
    ))
}

fn validate_dataset_rows(dataset: &DatasetSchema) -> Result<BTreeMap<&str, DataType>, ModelError> {
    if dataset.name.trim().is_empty() {
        return Err(ModelError::new(
            "BAD_DATASET_SCHEMA",
            "dataset name is required",
        ));
    }
    let columns = column_map(dataset)?;
    for (row_index, row) in dataset.rows.iter().enumerate() {
        for (name, value) in &row.values {
            let data_type = columns.get(name.as_str()).ok_or_else(|| {
                ModelError::new(
                    "BAD_DATASET_SCHEMA",
                    format!("row {} has undeclared column '{}'", row_index + 1, name),
                )
            })?;
            let valid = match data_type {
                DataType::String | DataType::Json => true,
                DataType::Integer => value.parse::<i64>().is_ok(),
                DataType::Number => canonical_decimal(value).is_ok(),
                DataType::Boolean => matches!(value.as_str(), "true" | "false"),
                DataType::Date => validate_date(value),
                DataType::Timestamp => valid_timestamp(value),
            };
            if !valid {
                return Err(ModelError::new(
                    "BAD_DATASET_VALUE",
                    format!(
                        "row {} value for '{}' is not {:?}",
                        row_index + 1,
                        name,
                        data_type
                    ),
                ));
            }
        }
    }
    Ok(columns)
}

fn validate_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes[..4].iter().all(u8::is_ascii_digit)
        || !bytes[5..7].iter().all(u8::is_ascii_digit)
        || !bytes[8..].iter().all(u8::is_ascii_digit)
    {
        return false;
    }
    let Ok(year) = value[..4].parse::<i32>() else {
        return false;
    };
    let Ok(month) = value[5..7].parse::<u32>() else {
        return false;
    };
    let Ok(day) = value[8..10].parse::<u32>() else {
        return false;
    };
    if year < 1 || !(1..=12).contains(&month) {
        return false;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let maximum = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    (1..=maximum).contains(&day)
}

fn valid_timestamp(value: &str) -> bool {
    let Some((date, remainder)) = value.split_once('T') else {
        return false;
    };
    if !validate_date(date) {
        return false;
    }

    let (clock, zone) = if let Some(clock) = remainder.strip_suffix('Z') {
        (clock, "Z")
    } else {
        let Some(zone_at) = remainder.rfind(['+', '-']) else {
            return false;
        };
        remainder.split_at(zone_at)
    };
    let zone_valid = if zone == "Z" {
        true
    } else if zone.len() == 6
        && matches!(zone.as_bytes()[0], b'+' | b'-')
        && zone.as_bytes()[3] == b':'
        && zone.as_bytes()[1..3].iter().all(u8::is_ascii_digit)
        && zone.as_bytes()[4..6].iter().all(u8::is_ascii_digit)
    {
        matches!((zone[1..3].parse::<u32>().ok(), zone[4..6].parse::<u32>().ok()), (Some(h), Some(m)) if h <= 23 && m <= 59)
    } else {
        false
    };
    if !zone_valid {
        return false;
    }

    let (whole_clock, fraction) = match clock.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (clock, None),
    };
    let bytes = whole_clock.as_bytes();
    if bytes.len() != 8
        || bytes[2] != b':'
        || bytes[5] != b':'
        || !bytes[..2].iter().all(u8::is_ascii_digit)
        || !bytes[3..5].iter().all(u8::is_ascii_digit)
        || !bytes[6..].iter().all(u8::is_ascii_digit)
        || fraction.is_some_and(|value| {
            value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return false;
    }
    let (Ok(hour), Ok(minute), Ok(second)) = (
        whole_clock[0..2].parse::<u32>(),
        whole_clock[3..5].parse::<u32>(),
        whole_clock[6..8].parse::<u32>(),
    ) else {
        return false;
    };
    hour <= 23 && minute <= 59 && second <= 60
}

/// A PK change is destructive. The plan is returned only after the proposed
/// key is valid/unique, all touching physical links resolve against it, and
/// the caller confirms loss of edits/writeback. This function still does not
/// perform the purge or publish the schema.
pub fn plan_primary_key_change(
    object_type: &ObjectType,
    new_key: &str,
    dataset: &DatasetSchema,
    edit_dataset: &str,
    writeback_dataset: Option<&str>,
    confirmed_edit_loss: bool,
    model: PhysicalModel<'_>,
) -> Result<PrimaryKeyChangePlan, ModelError> {
    if new_key == object_type.primary_key {
        return Err(ModelError::new(
            "PRIMARY_KEY_UNCHANGED",
            "proposed primary key is unchanged",
        ));
    }
    if edit_dataset.trim().is_empty() {
        return Err(ModelError::new(
            "BAD_EDIT_DATASET",
            "primary key change requires an explicit edit dataset to purge",
        ));
    }
    if edit_dataset == object_type.backing_dataset.as_deref().unwrap_or("") {
        return Err(ModelError::new(
            "BAD_EDIT_DATASET",
            "edit purge target cannot be the object's backing dataset",
        ));
    }
    if writeback_dataset.is_some_and(|name| name.trim().is_empty()) {
        return Err(ModelError::new(
            "BAD_WRITEBACK_DATASET",
            "writeback dataset name cannot be empty",
        ));
    }
    validate_physical_model(model)?;
    if writeback_dataset == Some(edit_dataset) {
        return Err(ModelError::new(
            "BAD_WRITEBACK_DATASET",
            "edit and writeback datasets must be distinct",
        ));
    }
    let mut protected_datasets: BTreeSet<String> = model
        .object_types
        .iter()
        .filter_map(|candidate| candidate.backing_dataset.clone())
        .collect();
    for link in model.links {
        if let LinkBacking::JoinTable { dataset, .. } = &link.backing {
            protected_datasets.insert(dataset.clone());
        }
    }
    if protected_datasets.contains(edit_dataset) {
        return Err(ModelError::new(
            "BAD_EDIT_DATASET",
            "edit purge target cannot be an object or link backing dataset",
        ));
    }
    if writeback_dataset.is_some_and(|name| protected_datasets.contains(name)) {
        return Err(ModelError::new(
            "BAD_WRITEBACK_DATASET",
            "writeback purge target cannot be an object or link backing dataset",
        ));
    }
    if !object_type
        .properties
        .iter()
        .any(|property| property.name == new_key)
    {
        return Err(ModelError::new(
            "BAD_OBJECT_TYPE",
            format!("new primary key '{}' is not a declared property", new_key),
        ));
    }
    let mut proposed = object_type.clone();
    proposed.primary_key = new_key.to_owned();
    validate_object_type(&proposed, Some(dataset))?;

    let current_type = object_type_named(model.object_types, &object_type.name)?;
    if current_type != object_type {
        return Err(ModelError::new(
            "STALE_ONTOLOGY_SNAPSHOT",
            format!(
                "object type '{}' differs from the supplied physical model",
                object_type.name
            ),
        ));
    }
    let current_dataset = dataset_named(model.datasets, &dataset.name)?;
    if current_dataset != dataset {
        return Err(ModelError::new(
            "STALE_DATASET_SNAPSHOT",
            format!(
                "dataset '{}' differs from the supplied physical model",
                dataset.name
            ),
        ));
    }

    let mut proposed_types = model.object_types.to_vec();
    let proposed_type = proposed_types
        .iter_mut()
        .find(|candidate| candidate.name == object_type.name)
        .expect("the unique object type was checked above");
    *proposed_type = proposed.clone();
    let datasets = model.datasets;

    validate_physical_model(PhysicalModel {
        object_types: &proposed_types,
        links: model.links,
        datasets,
    })?;
    let links_revalidated = model
        .links
        .iter()
        .filter(|link| link.from_type == object_type.name || link.to_type == object_type.name)
        .map(|link| link.name.clone())
        .collect();
    if !confirmed_edit_loss {
        return Err(ModelError::new(
            "PRIMARY_KEY_CHANGE_DESTRUCTIVE",
            format!("changing '{}' identity requires explicit confirmation to purge edits and writeback", object_type.name),
        ));
    }

    Ok(PrimaryKeyChangePlan {
        object_type: object_type.name.clone(),
        old_key: object_type.primary_key.clone(),
        new_key: new_key.to_owned(),
        edit_dataset: edit_dataset.to_owned(),
        writeback_dataset: writeback_dataset.map(str::to_owned),
        links_revalidated,
        destructive_confirmation_required: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part_model() -> (ObjectType, DatasetSchema) {
        let object_type = ObjectType {
            name: "Part".into(),
            backing_dataset: Some("parts".into()),
            primary_key: "part_id".into(),
            title_key: "part_name".into(),
            properties: vec![
                Property {
                    name: "part_id".into(),
                    data_type: DataType::String,
                    backing_column: Some("part_id".into()),
                },
                Property {
                    name: "part_name".into(),
                    data_type: DataType::String,
                    backing_column: Some("part_name".into()),
                },
                Property {
                    name: "legacy_code".into(),
                    data_type: DataType::String,
                    backing_column: Some("legacy_code".into()),
                },
            ],
        };
        let rows = vec![
            row(&[
                ("part_id", "P-1"),
                ("part_name", "Pump"),
                ("legacy_code", "A"),
            ]),
            row(&[
                ("part_id", "P-2"),
                ("part_name", "Valve"),
                ("legacy_code", "B"),
            ]),
        ];
        let dataset = DatasetSchema {
            name: "parts".into(),
            columns: vec![
                Column {
                    name: "part_id".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "part_name".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "legacy_code".into(),
                    data_type: DataType::String,
                },
            ],
            rows,
        };
        (object_type, dataset)
    }

    fn row(values: &[(&str, &str)]) -> Row {
        Row {
            values: values
                .iter()
                .map(|(key, value)| ((*key).into(), (*value).into()))
                .collect(),
        }
    }

    fn company_model() -> (ObjectType, DatasetSchema) {
        let object_type = ObjectType {
            name: "Company".into(),
            backing_dataset: Some("companies".into()),
            primary_key: "company_id".into(),
            title_key: "company_name".into(),
            properties: vec![
                Property {
                    name: "company_id".into(),
                    data_type: DataType::String,
                    backing_column: Some("company_id".into()),
                },
                Property {
                    name: "company_name".into(),
                    data_type: DataType::String,
                    backing_column: Some("company_name".into()),
                },
            ],
        };
        let dataset = DatasetSchema {
            name: "companies".into(),
            columns: vec![
                Column {
                    name: "company_id".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "company_name".into(),
                    data_type: DataType::String,
                },
            ],
            rows: vec![row(&[("company_id", "C-1"), ("company_name", "Acme")])],
        };
        (object_type, dataset)
    }

    fn employee_model() -> (ObjectType, DatasetSchema) {
        let object_type = ObjectType {
            name: "Employee".into(),
            backing_dataset: Some("employees".into()),
            primary_key: "employee_id".into(),
            title_key: "employee_name".into(),
            properties: vec![
                Property {
                    name: "employee_id".into(),
                    data_type: DataType::String,
                    backing_column: Some("employee_id".into()),
                },
                Property {
                    name: "employee_name".into(),
                    data_type: DataType::String,
                    backing_column: Some("employee_name".into()),
                },
                Property {
                    name: "company_id".into(),
                    data_type: DataType::String,
                    backing_column: Some("company_id".into()),
                },
            ],
        };
        let dataset = DatasetSchema {
            name: "employees".into(),
            columns: vec![
                Column {
                    name: "employee_id".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "employee_name".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "company_id".into(),
                    data_type: DataType::String,
                },
            ],
            rows: vec![
                row(&[
                    ("employee_id", "E-1"),
                    ("employee_name", "Ada"),
                    ("company_id", "C-1"),
                ]),
                row(&[
                    ("employee_id", "E-2"),
                    ("employee_name", "Lin"),
                    ("company_id", "C-1"),
                ]),
            ],
        };
        (object_type, dataset)
    }

    fn fk_link(cardinality: Cardinality) -> LinkType {
        LinkType {
            name: "EmployeeCompany".into(),
            from_type: "Employee".into(),
            to_type: "Company".into(),
            cardinality,
            backing: LinkBacking::ForeignKey {
                source_type: "Employee".into(),
                fk_property: "company_id".into(),
                target_type: "Company".into(),
            },
        }
    }

    fn physical_model<'a>(
        types: &'a [ObjectType],
        links: &'a [LinkType],
        datasets: &'a [DatasetSchema],
    ) -> PhysicalModel<'a> {
        PhysicalModel {
            object_types: types,
            links,
            datasets,
        }
    }

    #[test]
    fn complete_physical_model_rejects_duplicate_names_and_bad_unreferenced_data() {
        let (part, parts) = part_model();
        let duplicate_type = vec![part.clone(), part.clone()];
        let duplicate_type_error = validate_physical_model(physical_model(
            &duplicate_type,
            &[],
            std::slice::from_ref(&parts),
        ))
        .unwrap_err();
        assert_eq!(duplicate_type_error.code, "BAD_ONTOLOGY");

        let invalid_unreferenced_dataset = DatasetSchema {
            name: "unused".into(),
            columns: vec![Column {
                name: "count".into(),
                data_type: DataType::Integer,
            }],
            rows: vec![row(&[("count", "not-an-integer")])],
        };
        let bad_data_error = validate_physical_model(physical_model(
            std::slice::from_ref(&part),
            &[],
            &[parts, invalid_unreferenced_dataset],
        ))
        .unwrap_err();
        assert_eq!(bad_data_error.code, "BAD_DATASET_VALUE");
    }

    #[test]
    fn title_key_is_independent_of_primary_key() {
        let (object_type, dataset) = part_model();
        assert_ne!(object_type.primary_key, object_type.title_key);
        assert_eq!(validate_object_type(&object_type, Some(&dataset)), Ok(()));
    }

    #[test]
    fn duplicate_pk_fails_closed() {
        let (object_type, mut dataset) = part_model();
        dataset.rows[1]
            .values
            .insert("part_id".into(), "P-1".into());
        assert_eq!(
            validate_primary_keys(&object_type, &dataset)
                .unwrap_err()
                .code,
            "OntologyMetadata:DuplicatePrimaryKey"
        );
    }

    #[test]
    fn numeric_pk_equality_is_exact_without_float_rounding() {
        let object_type = ObjectType {
            name: "Numeric".into(),
            backing_dataset: Some("numeric".into()),
            primary_key: "id".into(),
            title_key: "id".into(),
            properties: vec![Property {
                name: "id".into(),
                data_type: DataType::Number,
                backing_column: Some("id".into()),
            }],
        };
        let mut dataset = DatasetSchema {
            name: "numeric".into(),
            columns: vec![Column {
                name: "id".into(),
                data_type: DataType::Number,
            }],
            rows: vec![row(&[("id", "1")]), row(&[("id", "1.00")])],
        };
        assert_eq!(
            validate_primary_keys(&object_type, &dataset)
                .unwrap_err()
                .code,
            "OntologyMetadata:DuplicatePrimaryKey"
        );
        dataset.rows[0].values.insert("id".into(), "+1".into());
        assert_eq!(
            validate_primary_keys(&object_type, &dataset)
                .unwrap_err()
                .code,
            "OntologyMetadata:DuplicatePrimaryKey"
        );
        dataset.rows[0].values.insert("id".into(), "1".into());
        dataset.rows[1]
            .values
            .insert("id".into(), "1.0000000000000001".into());
        assert!(validate_primary_keys(&object_type, &dataset).is_ok());
        dataset.rows[0].values.insert("id".into(), "1".into());

        for malformed in ["-", ".", "1e", "1e+", "1e--1", "1.2.3"] {
            dataset.rows.truncate(1);
            dataset.rows[0].values.insert("id".into(), malformed.into());
            assert_eq!(
                validate_primary_keys(&object_type, &dataset)
                    .unwrap_err()
                    .code,
                "BAD_DATASET_VALUE",
                "{malformed:?} must not be accepted as a decimal primary key"
            );
        }
    }

    #[test]
    fn date_and_timestamp_values_require_valid_iso_forms() {
        let mut dataset = DatasetSchema {
            name: "events".into(),
            columns: vec![
                Column {
                    name: "day".into(),
                    data_type: DataType::Date,
                },
                Column {
                    name: "created_at".into(),
                    data_type: DataType::Timestamp,
                },
            ],
            rows: vec![row(&[
                ("day", "2024-02-29"),
                ("created_at", "2024-02-29T23:59:60.125+05:30"),
            ])],
        };
        assert!(validate_dataset_rows(&dataset).is_ok());

        dataset.rows[0]
            .values
            .insert("day".into(), "2023-02-29".into());
        assert_eq!(
            validate_dataset_rows(&dataset).unwrap_err().code,
            "BAD_DATASET_VALUE"
        );
        dataset.rows[0]
            .values
            .insert("day".into(), "2024-02-29".into());
        dataset.rows[0]
            .values
            .insert("created_at".into(), "2024-02-29 12:00:00Z".into());
        assert_eq!(
            validate_dataset_rows(&dataset).unwrap_err().code,
            "BAD_DATASET_VALUE"
        );
        dataset.rows[0]
            .values
            .insert("created_at".into(), "2024-02-29T12:00:00+é:00".into());
        assert_eq!(
            validate_dataset_rows(&dataset).unwrap_err().code,
            "BAD_DATASET_VALUE"
        );
    }

    #[test]
    fn cardinality_constraints_are_directional() {
        let many_to_one = fk_link(Cardinality::ManyToOne);
        let (mut from_seen, mut to_seen) = (BTreeSet::new(), BTreeSet::new());
        assert!(
            validate_cardinality(&many_to_one, "from-1", "to-1", &mut from_seen, &mut to_seen)
                .is_ok()
        );
        assert!(
            validate_cardinality(&many_to_one, "from-2", "to-1", &mut from_seen, &mut to_seen)
                .is_ok()
        );
        assert_eq!(
            validate_cardinality(&many_to_one, "from-1", "to-2", &mut from_seen, &mut to_seen)
                .unwrap_err()
                .code,
            "LINK_CARDINALITY_VIOLATION"
        );

        let one_to_many = LinkType {
            cardinality: Cardinality::OneToMany,
            ..many_to_one.clone()
        };
        let (mut from_seen, mut to_seen) = (BTreeSet::new(), BTreeSet::new());
        assert!(
            validate_cardinality(&one_to_many, "from-1", "to-1", &mut from_seen, &mut to_seen)
                .is_ok()
        );
        assert!(
            validate_cardinality(&one_to_many, "from-1", "to-2", &mut from_seen, &mut to_seen)
                .is_ok()
        );
        assert_eq!(
            validate_cardinality(&one_to_many, "from-2", "to-2", &mut from_seen, &mut to_seen)
                .unwrap_err()
                .code,
            "LINK_CARDINALITY_VIOLATION"
        );
    }

    #[test]
    fn physical_schema_mismatch_has_stable_typed_error() {
        let (object_type, mut dataset) = part_model();
        dataset.columns[0].data_type = DataType::Integer;
        assert_eq!(
            validate_object_type(&object_type, Some(&dataset))
                .unwrap_err()
                .code,
            "OntologyMetadata:IncompatibleFoundryFieldSchemaForPropertyType"
        );
    }

    #[test]
    fn duplicate_dataset_columns_fail_closed() {
        let (object_type, mut dataset) = part_model();
        dataset.columns.push(dataset.columns[0].clone());
        assert_eq!(
            validate_object_type(&object_type, Some(&dataset))
                .unwrap_err()
                .code,
            "BAD_DATASET_SCHEMA"
        );
    }

    #[test]
    fn one_object_type_cannot_be_backed_by_a_second_dataset() {
        let (object_type, mut dataset) = part_model();
        dataset.name = "another".into();
        assert_eq!(
            validate_object_type(&object_type, Some(&dataset))
                .unwrap_err()
                .code,
            "ONE_DATASET_PER_OBJECT_TYPE"
        );
    }

    #[test]
    fn fk_resolves_and_rejects_dangling_references() {
        let (employee, mut employees) = employee_model();
        let (company, companies) = company_model();
        let types = vec![employee.clone(), company.clone()];
        let datasets = vec![employees.clone(), companies.clone()];
        assert!(validate_link(&fk_link(Cardinality::ManyToOne), &types, &datasets).is_ok());
        employees.rows[0]
            .values
            .insert("company_id".into(), "MISSING".into());
        let datasets = vec![employees, companies];
        assert_eq!(
            validate_link(&fk_link(Cardinality::ManyToOne), &types, &datasets)
                .unwrap_err()
                .code,
            "BROKEN_LINK_REFERENCE"
        );
    }

    #[test]
    fn absent_fk_is_nullable_but_empty_fk_is_dangling() {
        let (employee, mut employees) = employee_model();
        let (company, companies) = company_model();
        let types = [employee.clone(), company.clone()];

        employees.rows[0]
            .values
            .insert("company_id".into(), "".into());
        assert_eq!(
            validate_link(
                &fk_link(Cardinality::ManyToOne),
                &types,
                &[employees.clone(), companies.clone()]
            )
            .unwrap_err()
            .code,
            "BROKEN_LINK_REFERENCE"
        );

        employees.rows[0].values.remove("company_id");
        assert!(validate_link(
            &fk_link(Cardinality::ManyToOne),
            &types,
            &[employees, companies]
        )
        .is_ok());
    }

    #[test]
    fn one_to_one_fk_rejects_repeated_target() {
        let (employee, employees) = employee_model();
        let (company, companies) = company_model();
        assert_eq!(
            validate_link(
                &fk_link(Cardinality::OneToOne),
                &[employee, company],
                &[employees, companies]
            )
            .unwrap_err()
            .code,
            "LINK_CARDINALITY_VIOLATION"
        );
    }

    #[test]
    fn join_table_enforces_declared_cardinality_and_endpoint_integrity() {
        let (employee, employees) = employee_model();
        let (company, companies) = company_model();
        let join = DatasetSchema {
            name: "employee_company".into(),
            columns: vec![
                Column {
                    name: "employee_id".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "company_id".into(),
                    data_type: DataType::String,
                },
            ],
            rows: vec![row(&[("employee_id", "E-1"), ("company_id", "C-1")])],
        };
        let link = LinkType {
            name: "Membership".into(),
            from_type: "Employee".into(),
            to_type: "Company".into(),
            cardinality: Cardinality::ManyToMany,
            backing: LinkBacking::JoinTable {
                dataset: "employee_company".into(),
                left_column: "employee_id".into(),
                right_column: "company_id".into(),
            },
        };
        assert!(validate_link(
            &link,
            &[employee.clone(), company.clone()],
            &[employees.clone(), companies.clone(), join.clone()]
        )
        .is_ok());
        let one_to_many = LinkType {
            cardinality: Cardinality::OneToMany,
            ..link.clone()
        };
        assert!(validate_link(
            &one_to_many,
            &[employee.clone(), company.clone()],
            &[employees.clone(), companies.clone(), join.clone()]
        )
        .is_ok());
        let invalid_cardinality = DatasetSchema {
            rows: vec![
                row(&[("employee_id", "E-1"), ("company_id", "C-1")]),
                row(&[("employee_id", "E-2"), ("company_id", "C-1")]),
            ],
            ..join.clone()
        };
        assert_eq!(
            validate_link(
                &one_to_many,
                &[employee.clone(), company.clone()],
                &[employees.clone(), companies.clone(), invalid_cardinality]
            )
            .unwrap_err()
            .code,
            "LINK_CARDINALITY_VIOLATION"
        );
        let dangling = DatasetSchema {
            rows: vec![row(&[("employee_id", "MISSING"), ("company_id", "C-1")])],
            ..join
        };
        assert_eq!(
            validate_link(
                &link,
                &[employee, company],
                &[employees, companies, dangling]
            )
            .unwrap_err()
            .code,
            "BROKEN_LINK_REFERENCE"
        );
    }

    #[test]
    fn foreign_key_backing_rejects_many_to_many_and_one_to_many() {
        let (employee, employees) = employee_model();
        let (company, companies) = company_model();
        for cardinality in [Cardinality::OneToMany, Cardinality::ManyToMany] {
            let error = validate_link(
                &fk_link(cardinality),
                &[employee.clone(), company.clone()],
                &[employees.clone(), companies.clone()],
            )
            .unwrap_err();
            assert_eq!(error.code, "BAD_LINK");
        }
    }

    #[test]
    fn object_backed_link_validates_manifest_identity_and_endpoints() {
        let (employee, employees) = employee_model();
        let (company, companies) = company_model();
        let manifest_type = ObjectType {
            name: "Assignment".into(),
            backing_dataset: Some("assignments".into()),
            primary_key: "assignment_id".into(),
            title_key: "assignment_id".into(),
            properties: vec![
                Property {
                    name: "assignment_id".into(),
                    data_type: DataType::String,
                    backing_column: Some("assignment_id".into()),
                },
                Property {
                    name: "employee_id".into(),
                    data_type: DataType::String,
                    backing_column: Some("employee_id".into()),
                },
                Property {
                    name: "company_id".into(),
                    data_type: DataType::String,
                    backing_column: Some("company_id".into()),
                },
            ],
        };
        let manifest = DatasetSchema {
            name: "assignments".into(),
            columns: vec![
                Column {
                    name: "assignment_id".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "employee_id".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "company_id".into(),
                    data_type: DataType::String,
                },
            ],
            rows: vec![
                row(&[
                    ("assignment_id", "A-1"),
                    ("employee_id", "E-1"),
                    ("company_id", "C-1"),
                ]),
                row(&[
                    ("assignment_id", "A-2"),
                    ("employee_id", "E-2"),
                    ("company_id", "C-1"),
                ]),
            ],
        };
        let link = LinkType {
            name: "EmployeeAssignments".into(),
            from_type: "Employee".into(),
            to_type: "Company".into(),
            cardinality: Cardinality::ManyToOne,
            backing: LinkBacking::ObjectBacked {
                manifest_type: "Assignment".into(),
                left_property: "employee_id".into(),
                right_property: "company_id".into(),
            },
        };
        let types = vec![employee, company, manifest_type];
        let datasets = vec![employees, companies, manifest];
        assert!(validate_link(&link, &types, &datasets).is_ok());
    }

    #[test]
    fn pk_migration_rejects_join_rows_that_still_reference_old_key() {
        let (part, parts) = part_model();
        let technician = ObjectType {
            name: "Technician".into(),
            backing_dataset: Some("technicians".into()),
            primary_key: "tech_id".into(),
            title_key: "tech_id".into(),
            properties: vec![Property {
                name: "tech_id".into(),
                data_type: DataType::String,
                backing_column: Some("tech_id".into()),
            }],
        };
        let technicians = DatasetSchema {
            name: "technicians".into(),
            columns: vec![Column {
                name: "tech_id".into(),
                data_type: DataType::String,
            }],
            rows: vec![row(&[("tech_id", "T-1")])],
        };
        let join = DatasetSchema {
            name: "part_technicians".into(),
            columns: vec![
                Column {
                    name: "part_code".into(),
                    data_type: DataType::String,
                },
                Column {
                    name: "tech_id".into(),
                    data_type: DataType::String,
                },
            ],
            rows: vec![row(&[("part_code", "OLD-PK"), ("tech_id", "T-1")])],
        };
        let link = LinkType {
            name: "PartTechnicians".into(),
            from_type: "Part".into(),
            to_type: "Technician".into(),
            cardinality: Cardinality::ManyToMany,
            backing: LinkBacking::JoinTable {
                dataset: "part_technicians".into(),
                left_column: "part_code".into(),
                right_column: "tech_id".into(),
            },
        };
        let model_types = vec![part.clone(), technician];
        let model_datasets = vec![parts.clone(), technicians, join];
        let error = plan_primary_key_change(
            &part,
            "legacy_code",
            &parts,
            "edits__Part",
            None,
            true,
            physical_model(&model_types, std::slice::from_ref(&link), &model_datasets),
        )
        .unwrap_err();
        assert_eq!(error.code, "BROKEN_LINK_REFERENCE");
    }

    #[test]
    fn pk_change_refuses_object_datasets_as_purge_targets() {
        let (part, parts) = part_model();
        let model_types = vec![part.clone()];
        let model_datasets = vec![parts.clone()];
        let model = physical_model(&model_types, &[], &model_datasets);

        let edit_target =
            plan_primary_key_change(&part, "legacy_code", &parts, "parts", None, true, model)
                .unwrap_err();
        assert_eq!(edit_target.code, "BAD_EDIT_DATASET");

        let writeback_target = plan_primary_key_change(
            &part,
            "legacy_code",
            &parts,
            "edits__Part",
            Some("parts"),
            true,
            model,
        )
        .unwrap_err();
        assert_eq!(writeback_target.code, "BAD_WRITEBACK_DATASET");
    }

    #[test]
    fn pk_change_requires_explicit_confirmation_and_fresh_inputs() {
        let (part, parts) = part_model();
        let model_types = vec![part.clone()];
        let model_datasets = vec![parts.clone()];
        let model = physical_model(&model_types, &[], &model_datasets);

        let unconfirmed = plan_primary_key_change(
            &part,
            "legacy_code",
            &parts,
            "edits__Part",
            None,
            false,
            model,
        )
        .unwrap_err();
        assert_eq!(unconfirmed.code, "PRIMARY_KEY_CHANGE_DESTRUCTIVE");

        let mut stale_parts = parts.clone();
        stale_parts.rows[0]
            .values
            .insert("legacy_code".into(), "changed-after-read".into());
        let stale = plan_primary_key_change(
            &part,
            "legacy_code",
            &stale_parts,
            "edits__Part",
            None,
            true,
            model,
        )
        .unwrap_err();
        assert_eq!(stale.code, "STALE_DATASET_SNAPSHOT");
    }

    #[test]
    fn confirmed_pk_migration_plans_purge_after_link_validation() {
        let (part, parts) = part_model();
        let model_types = vec![part.clone()];
        let model_datasets = vec![parts.clone()];
        let plan = plan_primary_key_change(
            &part,
            "legacy_code",
            &parts,
            "edits__Part",
            Some("writeback__Part"),
            true,
            physical_model(&model_types, &[], &model_datasets),
        )
        .unwrap();
        assert_eq!(plan.old_key, "part_id");
        assert_eq!(plan.new_key, "legacy_code");
        assert_eq!(plan.writeback_dataset.as_deref(), Some("writeback__Part"));
        assert!(plan.destructive_confirmation_required);
    }
}
