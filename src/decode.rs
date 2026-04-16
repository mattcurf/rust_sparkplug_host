// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sparkplug_rs::{
    DataType, Payload, payload,
    protobuf::{Enum, Message},
};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecodedPayload {
    pub timestamp: Option<u64>,
    pub seq: Option<u64>,
    pub uuid: Option<String>,
    pub body: Option<Vec<u8>>,
    pub metrics: Vec<DecodedMetric>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecodedMetric {
    pub name: Option<String>,
    pub alias: Option<u64>,
    pub timestamp: Option<u64>,
    pub datatype: u32,
    pub datatype_name: String,
    pub quality: Option<i32>,
    pub is_historical: bool,
    pub is_transient: bool,
    pub is_null: bool,
    pub metadata: Option<DecodedMetaData>,
    pub properties: Option<DecodedPropertySet>,
    pub value: DecodedMetricValue,
}

impl DecodedMetric {
    pub fn placeholder(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            alias: None,
            timestamp: None,
            datatype: 0,
            datatype_name: datatype_name(0),
            quality: None,
            is_historical: false,
            is_transient: false,
            is_null: false,
            metadata: None,
            properties: None,
            value: DecodedMetricValue::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value")]
pub enum DecodedMetricValue {
    Signed(i64),
    Unsigned(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(String),
    DateTime(u64),
    Bytes(Vec<u8>),
    DataSet(DecodedDataSet),
    Template(DecodedTemplate),
    Int8Array(Vec<i8>),
    Int16Array(Vec<i16>),
    Int32Array(Vec<i32>),
    Int64Array(Vec<i64>),
    UInt8Array(Vec<u8>),
    UInt16Array(Vec<u16>),
    UInt32Array(Vec<u32>),
    UInt64Array(Vec<u64>),
    FloatArray(Vec<f32>),
    DoubleArray(Vec<f64>),
    BooleanArray(Vec<bool>),
    StringArray(Vec<String>),
    DateTimeArray(Vec<u64>),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecodedMetaData {
    pub is_multi_part: bool,
    pub content_type: Option<String>,
    pub size: Option<u64>,
    pub seq: Option<u64>,
    pub file_name: Option<String>,
    pub file_type: Option<String>,
    pub md5: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecodedPropertySet {
    pub entries: Vec<DecodedPropertyEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecodedPropertyEntry {
    pub key: Option<String>,
    pub value: DecodedPropertyValue,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value")]
pub enum DecodedPropertyValue {
    Signed(i64),
    Unsigned(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(String),
    DateTime(u64),
    PropertySet(DecodedPropertySet),
    PropertySetList(Vec<DecodedPropertySet>),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecodedDataSet {
    pub num_of_columns: Option<u64>,
    pub columns: Vec<String>,
    pub types: Vec<u32>,
    pub type_names: Vec<String>,
    pub rows: Vec<Vec<DecodedDataSetValue>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value")]
pub enum DecodedDataSetValue {
    Signed(i64),
    Unsigned(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(String),
    DateTime(u64),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecodedTemplate {
    pub version: Option<String>,
    pub template_ref: Option<String>,
    pub is_definition: bool,
    pub metrics: Vec<DecodedMetric>,
    pub parameters: Vec<DecodedTemplateParameter>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DecodedTemplateParameter {
    pub name: Option<String>,
    pub datatype: u32,
    pub datatype_name: String,
    pub value: DecodedTemplateParameterValue,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value")]
pub enum DecodedTemplateParameterValue {
    Signed(i64),
    Unsigned(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(String),
    DateTime(u64),
    Null,
}

pub fn decode_payload(bytes: &[u8]) -> Result<DecodedPayload> {
    let payload = Payload::parse_from_bytes(bytes).context("failed to decode Sparkplug payload")?;
    decode_payload_message(&payload)
}

pub fn decode_payload_with_metric_datatypes<F>(
    bytes: &[u8],
    resolve_datatype: F,
) -> Result<DecodedPayload>
where
    F: Fn(Option<&str>, Option<u64>) -> Option<u32>,
{
    let payload = Payload::parse_from_bytes(bytes).context("failed to decode Sparkplug payload")?;
    decode_payload_message_with_metric_datatypes(&payload, resolve_datatype)
}

pub fn decode_payload_message(payload: &Payload) -> Result<DecodedPayload> {
    decode_payload_message_with_metric_datatypes(payload, |_, _| None)
}

fn decode_payload_message_with_metric_datatypes<F>(
    payload: &Payload,
    resolve_datatype: F,
) -> Result<DecodedPayload>
where
    F: Fn(Option<&str>, Option<u64>) -> Option<u32>,
{
    Ok(DecodedPayload {
        timestamp: payload.timestamp,
        seq: payload.seq,
        uuid: payload.uuid.clone(),
        body: payload.body.clone(),
        metrics: payload
            .metrics
            .iter()
            .map(|metric| {
                decode_metric(
                    metric,
                    resolve_datatype(metric.name.as_deref(), metric.alias),
                )
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

fn decode_metric(
    metric: &payload::Metric,
    fallback_datatype: Option<u32>,
) -> Result<DecodedMetric> {
    let datatype = match metric.datatype.or(fallback_datatype) {
        Some(datatype) => datatype,
        None => {
            if let Some(name) = metric.name.as_deref() {
                bail!("metric '{name}' missing datatype")
            }
            if let Some(alias) = metric.alias {
                bail!("metric alias {alias} missing datatype and no prior birth definition")
            }
            bail!("metric missing datatype")
        }
    };
    let properties = metric
        .properties
        .as_ref()
        .map(decode_property_set)
        .transpose()?;

    Ok(DecodedMetric {
        name: metric.name.clone(),
        alias: metric.alias,
        timestamp: metric.timestamp,
        datatype,
        datatype_name: datatype_name(datatype),
        quality: extract_quality(properties.as_ref())?,
        is_historical: metric.is_historical(),
        is_transient: metric.is_transient(),
        is_null: metric.is_null(),
        metadata: metric.metadata.as_ref().map(decode_metadata).transpose()?,
        properties,
        value: decode_metric_value(metric, datatype)?,
    })
}

fn decode_metric_value(metric: &payload::Metric, datatype: u32) -> Result<DecodedMetricValue> {
    let data_type = resolve_datatype(datatype)?;

    if metric.is_null() {
        return Ok(DecodedMetricValue::Null);
    }

    match data_type {
        DataType::Unknown => bail!("metric datatype Unknown is not supported"),
        DataType::Int8 => Ok(DecodedMetricValue::Signed(
            metric.int_value() as i32 as i8 as i64
        )),
        DataType::Int16 => Ok(DecodedMetricValue::Signed(
            metric.int_value() as i32 as i16 as i64
        )),
        DataType::Int32 => Ok(DecodedMetricValue::Signed(metric.int_value() as i32 as i64)),
        DataType::Int64 => Ok(DecodedMetricValue::Signed(metric.long_value() as i64)),
        DataType::UInt8 => Ok(DecodedMetricValue::Unsigned(metric.int_value() as u8 as u64)),
        DataType::UInt16 => Ok(DecodedMetricValue::Unsigned(
            metric.int_value() as u16 as u64
        )),
        DataType::UInt32 => Ok(DecodedMetricValue::Unsigned(metric.int_value() as u64)),
        DataType::UInt64 => Ok(DecodedMetricValue::Unsigned(metric.long_value())),
        DataType::Float => {
            if !metric.has_float_value() {
                bail!("metric datatype Float missing float_value")
            }
            Ok(DecodedMetricValue::Float(metric.float_value()))
        }
        DataType::Double => {
            if !metric.has_double_value() {
                bail!("metric datatype Double missing double_value")
            }
            Ok(DecodedMetricValue::Double(metric.double_value()))
        }
        DataType::Boolean => {
            if !metric.has_boolean_value() {
                bail!("metric datatype Boolean missing boolean_value")
            }
            Ok(DecodedMetricValue::Boolean(metric.boolean_value()))
        }
        DataType::String | DataType::Text | DataType::UUID => {
            if !metric.has_string_value() {
                bail!(
                    "metric datatype {} missing string_value",
                    data_type_name(&data_type)
                )
            }
            Ok(DecodedMetricValue::String(
                metric.string_value().to_string(),
            ))
        }
        DataType::DateTime => Ok(DecodedMetricValue::DateTime(metric.long_value())),
        DataType::DataSet => {
            if !metric.has_dataset_value() {
                bail!("metric datatype DataSet missing dataset_value")
            }
            Ok(DecodedMetricValue::DataSet(decode_data_set(
                metric.dataset_value(),
            )?))
        }
        DataType::Bytes | DataType::File => {
            if !metric.has_bytes_value() {
                bail!(
                    "metric datatype {} missing bytes_value",
                    data_type_name(&data_type)
                )
            }
            Ok(DecodedMetricValue::Bytes(metric.bytes_value().to_vec()))
        }
        DataType::Template => {
            if !metric.has_template_value() {
                bail!("metric datatype Template missing template_value")
            }
            Ok(DecodedMetricValue::Template(decode_template(
                metric.template_value(),
            )?))
        }
        DataType::PropertySet | DataType::PropertySetList => bail!(
            "metric datatype {} is only valid inside PropertyValue",
            data_type_name(&data_type)
        ),
        DataType::Int8Array => Ok(DecodedMetricValue::Int8Array(
            metric
                .bytes_value()
                .iter()
                .map(|byte| *byte as i8)
                .collect(),
        )),
        DataType::Int16Array => Ok(DecodedMetricValue::Int16Array(read_fixed_array(
            metric.bytes_value(),
            2,
            |chunk| i16::from_le_bytes([chunk[0], chunk[1]]),
        )?)),
        DataType::Int32Array => Ok(DecodedMetricValue::Int32Array(read_fixed_array(
            metric.bytes_value(),
            4,
            |chunk| i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]),
        )?)),
        DataType::Int64Array => Ok(DecodedMetricValue::Int64Array(read_fixed_array(
            metric.bytes_value(),
            8,
            |chunk| {
                i64::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ])
            },
        )?)),
        DataType::UInt8Array => Ok(DecodedMetricValue::UInt8Array(
            metric.bytes_value().to_vec(),
        )),
        DataType::UInt16Array => Ok(DecodedMetricValue::UInt16Array(read_fixed_array(
            metric.bytes_value(),
            2,
            |chunk| u16::from_le_bytes([chunk[0], chunk[1]]),
        )?)),
        DataType::UInt32Array => Ok(DecodedMetricValue::UInt32Array(read_fixed_array(
            metric.bytes_value(),
            4,
            |chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]),
        )?)),
        DataType::UInt64Array => Ok(DecodedMetricValue::UInt64Array(read_fixed_array(
            metric.bytes_value(),
            8,
            |chunk| {
                u64::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ])
            },
        )?)),
        DataType::FloatArray => Ok(DecodedMetricValue::FloatArray(read_fixed_array(
            metric.bytes_value(),
            4,
            |chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]),
        )?)),
        DataType::DoubleArray => Ok(DecodedMetricValue::DoubleArray(read_fixed_array(
            metric.bytes_value(),
            8,
            |chunk| {
                f64::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ])
            },
        )?)),
        DataType::BooleanArray => Ok(DecodedMetricValue::BooleanArray(decode_boolean_array(
            metric.bytes_value(),
        )?)),
        DataType::StringArray => Ok(DecodedMetricValue::StringArray(decode_string_array(
            metric.bytes_value(),
        )?)),
        DataType::DateTimeArray => Ok(DecodedMetricValue::DateTimeArray(read_fixed_array(
            metric.bytes_value(),
            8,
            |chunk| {
                u64::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ])
            },
        )?)),
    }
}

fn decode_metadata(metadata: &payload::MetaData) -> Result<DecodedMetaData> {
    Ok(DecodedMetaData {
        is_multi_part: metadata.is_multi_part(),
        content_type: metadata.content_type.clone(),
        size: metadata.size,
        seq: metadata.seq,
        file_name: metadata.file_name.clone(),
        file_type: metadata.file_type.clone(),
        md5: metadata.md5.clone(),
        description: metadata.description.clone(),
    })
}

fn decode_property_set(property_set: &payload::PropertySet) -> Result<DecodedPropertySet> {
    let value_count = property_set.values.len();
    let key_count = property_set.keys.len();
    let entry_count = value_count.max(key_count);
    let mut entries = Vec::with_capacity(entry_count);

    for index in 0..entry_count {
        entries.push(DecodedPropertyEntry {
            key: property_set.keys.get(index).cloned(),
            value: decode_property_value(
                property_set
                    .values
                    .get(index)
                    .with_context(|| format!("property set missing value at index {index}"))?,
            )?,
        });
    }

    Ok(DecodedPropertySet { entries })
}

fn decode_property_value(value: &payload::PropertyValue) -> Result<DecodedPropertyValue> {
    if value.is_null() {
        return Ok(DecodedPropertyValue::Null);
    }

    let datatype = value.type_.unwrap_or_default();
    let data_type = resolve_datatype(datatype)?;

    match data_type {
        DataType::Unknown => bail!("property datatype Unknown is not supported"),
        DataType::Int8 => Ok(DecodedPropertyValue::Signed(
            value.int_value() as i32 as i8 as i64
        )),
        DataType::Int16 => Ok(DecodedPropertyValue::Signed(
            value.int_value() as i32 as i16 as i64,
        )),
        DataType::Int32 => Ok(DecodedPropertyValue::Signed(value.int_value() as i32 as i64)),
        DataType::Int64 => Ok(DecodedPropertyValue::Signed(value.long_value() as i64)),
        DataType::UInt8 => Ok(DecodedPropertyValue::Unsigned(
            value.int_value() as u8 as u64
        )),
        DataType::UInt16 => Ok(DecodedPropertyValue::Unsigned(
            value.int_value() as u16 as u64
        )),
        DataType::UInt32 => Ok(DecodedPropertyValue::Unsigned(value.int_value() as u64)),
        DataType::UInt64 => Ok(DecodedPropertyValue::Unsigned(value.long_value())),
        DataType::Float => {
            if !value.has_float_value() {
                bail!("property datatype Float missing float_value")
            }
            Ok(DecodedPropertyValue::Float(value.float_value()))
        }
        DataType::Double => {
            if !value.has_double_value() {
                bail!("property datatype Double missing double_value")
            }
            Ok(DecodedPropertyValue::Double(value.double_value()))
        }
        DataType::Boolean => {
            if !value.has_boolean_value() {
                bail!("property datatype Boolean missing boolean_value")
            }
            Ok(DecodedPropertyValue::Boolean(value.boolean_value()))
        }
        DataType::String | DataType::Text | DataType::UUID => {
            if !value.has_string_value() {
                bail!(
                    "property datatype {} missing string_value",
                    data_type_name(&data_type)
                )
            }
            Ok(DecodedPropertyValue::String(
                value.string_value().to_string(),
            ))
        }
        DataType::DateTime => Ok(DecodedPropertyValue::DateTime(value.long_value())),
        DataType::PropertySet => {
            if !value.has_propertyset_value() {
                bail!("property datatype PropertySet missing propertyset_value")
            }
            Ok(DecodedPropertyValue::PropertySet(decode_property_set(
                value.propertyset_value(),
            )?))
        }
        DataType::PropertySetList => {
            if !value.has_propertysets_value() {
                bail!("property datatype PropertySetList missing propertysets_value")
            }
            Ok(DecodedPropertyValue::PropertySetList(
                value
                    .propertysets_value()
                    .propertyset
                    .iter()
                    .map(decode_property_set)
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
        DataType::DataSet
        | DataType::Bytes
        | DataType::File
        | DataType::Template
        | DataType::Int8Array
        | DataType::Int16Array
        | DataType::Int32Array
        | DataType::Int64Array
        | DataType::UInt8Array
        | DataType::UInt16Array
        | DataType::UInt32Array
        | DataType::UInt64Array
        | DataType::FloatArray
        | DataType::DoubleArray
        | DataType::BooleanArray
        | DataType::StringArray
        | DataType::DateTimeArray => bail!(
            "property datatype {} is not supported inside PropertyValue",
            data_type_name(&data_type)
        ),
    }
}

fn decode_data_set(data_set: &payload::DataSet) -> Result<DecodedDataSet> {
    let type_names = data_set
        .types
        .iter()
        .map(|value| datatype_name(*value))
        .collect();
    let rows = data_set
        .rows
        .iter()
        .map(|row| {
            row.elements
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    decode_data_set_value(value, data_set.types.get(index).copied())
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(DecodedDataSet {
        num_of_columns: data_set.num_of_columns,
        columns: data_set.columns.clone(),
        types: data_set.types.clone(),
        type_names,
        rows,
    })
}

fn decode_data_set_value(
    value: &payload::data_set::DataSetValue,
    datatype: Option<u32>,
) -> Result<DecodedDataSetValue> {
    let data_type = datatype
        .map(resolve_datatype)
        .transpose()?
        .unwrap_or(DataType::Unknown);

    match data_type {
        DataType::Unknown => {
            if value.has_string_value() {
                Ok(DecodedDataSetValue::String(
                    value.string_value().to_string(),
                ))
            } else if value.has_boolean_value() {
                Ok(DecodedDataSetValue::Boolean(value.boolean_value()))
            } else if value.has_double_value() {
                Ok(DecodedDataSetValue::Double(value.double_value()))
            } else if value.has_float_value() {
                Ok(DecodedDataSetValue::Float(value.float_value()))
            } else if value.has_long_value() {
                Ok(DecodedDataSetValue::Unsigned(value.long_value()))
            } else if value.has_int_value() {
                Ok(DecodedDataSetValue::Unsigned(value.int_value() as u64))
            } else {
                Ok(DecodedDataSetValue::Null)
            }
        }
        DataType::Int8 => Ok(DecodedDataSetValue::Signed(
            value.int_value() as i32 as i8 as i64
        )),
        DataType::Int16 => Ok(DecodedDataSetValue::Signed(
            value.int_value() as i32 as i16 as i64
        )),
        DataType::Int32 => Ok(DecodedDataSetValue::Signed(value.int_value() as i32 as i64)),
        DataType::Int64 => Ok(DecodedDataSetValue::Signed(value.long_value() as i64)),
        DataType::UInt8 => Ok(DecodedDataSetValue::Unsigned(value.int_value() as u8 as u64)),
        DataType::UInt16 => Ok(DecodedDataSetValue::Unsigned(
            value.int_value() as u16 as u64
        )),
        DataType::UInt32 => Ok(DecodedDataSetValue::Unsigned(value.int_value() as u64)),
        DataType::UInt64 => Ok(DecodedDataSetValue::Unsigned(value.long_value())),
        DataType::Float => Ok(DecodedDataSetValue::Float(value.float_value())),
        DataType::Double => Ok(DecodedDataSetValue::Double(value.double_value())),
        DataType::Boolean => Ok(DecodedDataSetValue::Boolean(value.boolean_value())),
        DataType::String | DataType::Text | DataType::UUID => Ok(DecodedDataSetValue::String(
            value.string_value().to_string(),
        )),
        DataType::DateTime => Ok(DecodedDataSetValue::DateTime(value.long_value())),
        _ => bail!(
            "dataset column datatype {} is not supported",
            data_type_name(&data_type)
        ),
    }
}

fn decode_template(template: &payload::Template) -> Result<DecodedTemplate> {
    Ok(DecodedTemplate {
        version: template.version.clone(),
        template_ref: template.template_ref.clone(),
        is_definition: template.is_definition(),
        metrics: template
            .metrics
            .iter()
            .map(|metric| decode_metric(metric, None))
            .collect::<Result<Vec<_>>>()?,
        parameters: template
            .parameters
            .iter()
            .map(decode_template_parameter)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn decode_template_parameter(
    parameter: &payload::template::Parameter,
) -> Result<DecodedTemplateParameter> {
    let datatype = parameter
        .type_
        .context("template parameter missing datatype")?;
    let data_type = resolve_datatype(datatype)?;
    let value = match data_type {
        DataType::Int8 => {
            DecodedTemplateParameterValue::Signed(parameter.int_value() as i32 as i8 as i64)
        }
        DataType::Int16 => {
            DecodedTemplateParameterValue::Signed(parameter.int_value() as i32 as i16 as i64)
        }
        DataType::Int32 => {
            DecodedTemplateParameterValue::Signed(parameter.int_value() as i32 as i64)
        }
        DataType::Int64 => DecodedTemplateParameterValue::Signed(parameter.long_value() as i64),
        DataType::UInt8 => {
            DecodedTemplateParameterValue::Unsigned(parameter.int_value() as u8 as u64)
        }
        DataType::UInt16 => {
            DecodedTemplateParameterValue::Unsigned(parameter.int_value() as u16 as u64)
        }
        DataType::UInt32 => DecodedTemplateParameterValue::Unsigned(parameter.int_value() as u64),
        DataType::UInt64 => DecodedTemplateParameterValue::Unsigned(parameter.long_value()),
        DataType::Float => DecodedTemplateParameterValue::Float(parameter.float_value()),
        DataType::Double => DecodedTemplateParameterValue::Double(parameter.double_value()),
        DataType::Boolean => DecodedTemplateParameterValue::Boolean(parameter.boolean_value()),
        DataType::String | DataType::Text | DataType::UUID => {
            DecodedTemplateParameterValue::String(parameter.string_value().to_string())
        }
        DataType::DateTime => DecodedTemplateParameterValue::DateTime(parameter.long_value()),
        _ => bail!(
            "template parameter datatype {} is not supported",
            data_type_name(&data_type)
        ),
    };

    Ok(DecodedTemplateParameter {
        name: parameter.name.clone(),
        datatype,
        datatype_name: data_type_name(&data_type).to_string(),
        value,
    })
}

fn extract_quality(properties: Option<&DecodedPropertySet>) -> Result<Option<i32>> {
    let Some(properties) = properties else {
        return Ok(None);
    };

    for entry in &properties.entries {
        if entry.key.as_deref() != Some("Quality") {
            continue;
        }

        return match entry.value {
            DecodedPropertyValue::Signed(value) => Ok(Some(value as i32)),
            _ => bail!("Quality property must decode to a signed integer"),
        };
    }

    Ok(None)
}

fn resolve_datatype(datatype: u32) -> Result<DataType> {
    <DataType as Enum>::from_i32(datatype as i32)
        .with_context(|| format!("unsupported Sparkplug datatype {datatype}"))
}

fn datatype_name(datatype: u32) -> String {
    <DataType as Enum>::from_i32(datatype as i32)
        .map(|value| data_type_name(&value).to_string())
        .unwrap_or_else(|| format!("UNKNOWN({datatype})"))
}

fn data_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Unknown => "Unknown",
        DataType::Int8 => "Int8",
        DataType::Int16 => "Int16",
        DataType::Int32 => "Int32",
        DataType::Int64 => "Int64",
        DataType::UInt8 => "UInt8",
        DataType::UInt16 => "UInt16",
        DataType::UInt32 => "UInt32",
        DataType::UInt64 => "UInt64",
        DataType::Float => "Float",
        DataType::Double => "Double",
        DataType::Boolean => "Boolean",
        DataType::String => "String",
        DataType::DateTime => "DateTime",
        DataType::Text => "Text",
        DataType::UUID => "UUID",
        DataType::DataSet => "DataSet",
        DataType::Bytes => "Bytes",
        DataType::File => "File",
        DataType::Template => "Template",
        DataType::PropertySet => "PropertySet",
        DataType::PropertySetList => "PropertySetList",
        DataType::Int8Array => "Int8Array",
        DataType::Int16Array => "Int16Array",
        DataType::Int32Array => "Int32Array",
        DataType::Int64Array => "Int64Array",
        DataType::UInt8Array => "UInt8Array",
        DataType::UInt16Array => "UInt16Array",
        DataType::UInt32Array => "UInt32Array",
        DataType::UInt64Array => "UInt64Array",
        DataType::FloatArray => "FloatArray",
        DataType::DoubleArray => "DoubleArray",
        DataType::BooleanArray => "BooleanArray",
        DataType::StringArray => "StringArray",
        DataType::DateTimeArray => "DateTimeArray",
    }
}

fn read_fixed_array<T>(bytes: &[u8], width: usize, read: impl Fn(&[u8]) -> T) -> Result<Vec<T>> {
    if bytes.len() % width != 0 {
        bail!(
            "array payload length {} is not a multiple of element width {}",
            bytes.len(),
            width
        );
    }

    Ok(bytes.chunks_exact(width).map(read).collect())
}

fn decode_boolean_array(bytes: &[u8]) -> Result<Vec<bool>> {
    if bytes.len() < 4 {
        bail!("boolean array payload must include a 4-byte element count prefix")
    }

    let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut values = Vec::with_capacity(count);

    for byte in &bytes[4..] {
        for bit in 0..8 {
            if values.len() == count {
                break;
            }

            let mask = 1 << (7 - bit);
            values.push(byte & mask != 0);
        }
    }

    if values.len() != count {
        bail!(
            "boolean array declared {count} values but only {} bits were provided",
            values.len()
        )
    }

    Ok(values)
}

fn decode_string_array(bytes: &[u8]) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut start = 0usize;

    for (index, byte) in bytes.iter().enumerate() {
        if *byte != 0 {
            continue;
        }

        values.push(
            String::from_utf8(bytes[start..index].to_vec())
                .context("string array contains invalid UTF-8")?,
        );
        start = index + 1;
    }

    if start < bytes.len() {
        values.push(
            String::from_utf8(bytes[start..].to_vec())
                .context("string array contains invalid UTF-8")?,
        );
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use sparkplug_rs::protobuf::{Enum, Message, MessageField};
    use sparkplug_rs::{DataType, Payload, payload};

    use super::{
        DecodedMetricValue, DecodedPropertyValue, decode_payload_message,
        decode_payload_with_metric_datatypes,
    };

    #[test]
    fn decodes_complex_metric_types() {
        let mut payload = Payload::new();

        let mut array_metric = payload::Metric::new();
        array_metric.set_name("motor/currents".to_string());
        array_metric.set_datatype(DataType::UInt16Array.value() as u32);
        array_metric.set_bytes_value(vec![0x34, 0x12, 0x78, 0x56]);
        payload.metrics.push(array_metric);

        let mut dataset_metric = payload::Metric::new();
        dataset_metric.set_name("table".to_string());
        dataset_metric.set_datatype(DataType::DataSet.value() as u32);
        let mut data_set = payload::DataSet::new();
        data_set.set_num_of_columns(2);
        data_set.columns.push("name".to_string());
        data_set.columns.push("value".to_string());
        data_set.types.push(DataType::String.value() as u32);
        data_set.types.push(DataType::Int32.value() as u32);
        let mut row = payload::data_set::Row::new();
        let mut string_value = payload::data_set::DataSetValue::new();
        string_value.set_string_value("pump-a".to_string());
        row.elements.push(string_value);
        let mut int_value = payload::data_set::DataSetValue::new();
        int_value.set_int_value(42);
        row.elements.push(int_value);
        data_set.rows.push(row);
        dataset_metric.set_dataset_value(data_set);
        payload.metrics.push(dataset_metric);

        let mut template_metric = payload::Metric::new();
        template_metric.set_name("template".to_string());
        template_metric.set_datatype(DataType::Template.value() as u32);
        let mut template = payload::Template::new();
        template.set_version("1.0.0".to_string());
        template.set_is_definition(true);
        let mut parameter = payload::template::Parameter::new();
        parameter.set_name("line".to_string());
        parameter.set_type(DataType::String.value() as u32);
        parameter.set_string_value("north".to_string());
        template.parameters.push(parameter);
        template_metric.set_template_value(template);
        payload.metrics.push(template_metric);

        let decoded = decode_payload_message(&payload).unwrap();

        assert_eq!(
            decoded.metrics[0].value,
            DecodedMetricValue::UInt16Array(vec![0x1234, 0x5678])
        );
        assert_eq!(decoded.metrics[1].datatype_name, "DataSet");
        assert_eq!(decoded.metrics[2].datatype_name, "Template");
    }

    #[test]
    fn decodes_quality_and_null_metrics() {
        let mut payload = Payload::new();
        let mut metric = payload::Metric::new();
        metric.set_name("temperature".to_string());
        metric.set_alias(7);
        metric.set_datatype(DataType::Float.value() as u32);
        metric.set_is_null(true);

        let mut properties = payload::PropertySet::new();
        properties.keys.push("Quality".to_string());
        let mut quality = payload::PropertyValue::new();
        quality.set_type(DataType::Int32.value() as u32);
        quality.set_int_value(500);
        properties.values.push(quality);
        metric.properties = MessageField::some(properties);

        payload.metrics.push(metric);

        let decoded = decode_payload_message(&payload).unwrap();
        let metric = &decoded.metrics[0];

        assert_eq!(metric.quality, Some(500));
        assert_eq!(metric.value, DecodedMetricValue::Null);
        assert_eq!(
            metric
                .properties
                .as_ref()
                .unwrap()
                .entries
                .first()
                .unwrap()
                .value,
            DecodedPropertyValue::Signed(500)
        );
    }

    #[test]
    fn decodes_alias_only_data_metrics_using_known_datatype() {
        let mut payload = Payload::new();
        payload.set_timestamp(101);
        payload.set_seq(1);

        let mut metric = payload::Metric::new();
        metric.set_alias(7);
        metric.set_timestamp(101);
        metric.set_float_value(26.5);
        payload.metrics.push(metric);

        let bytes = payload.write_to_bytes().unwrap();
        let decoded = decode_payload_with_metric_datatypes(&bytes, |name, alias| {
            assert_eq!(name, None);
            assert_eq!(alias, Some(7));
            Some(DataType::Float.value() as u32)
        })
        .unwrap();

        assert_eq!(decoded.metrics[0].datatype_name, "Float");
        assert_eq!(decoded.metrics[0].value, DecodedMetricValue::Float(26.5));
    }
}
