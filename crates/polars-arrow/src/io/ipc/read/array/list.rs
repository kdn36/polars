use std::collections::VecDeque;
use std::io::{Read, Seek};

use polars_error::{PolarsResult, polars_err};

use super::super::super::IpcField;
use super::super::deserialize::{read, skip};
use super::super::read_basic::*;
use super::super::{Compression, Dictionaries, IpcBuffer, Node, Version};
use crate::array::ListArray;
use crate::buffer::Buffer;
use crate::datatypes::ArrowDataType;
use crate::io::ipc::read::array::{try_get_array_length, try_get_field_node};
use crate::offset::Offset;

#[allow(clippy::too_many_arguments)]
pub fn read_list<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    variadic_buffer_counts: &mut VecDeque<usize>,
    dtype: ArrowDataType,
    ipc_field: &IpcField,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    dictionaries: &Dictionaries,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<Compression>,
    limit: Option<usize>,
    version: Version,
    scratch: &mut Vec<u8>,
) -> PolarsResult<ListArray<O>>
where
    Vec<u8>: TryInto<O::Bytes>,
{
    let field_node = try_get_field_node(field_nodes, &dtype)?;

    let validity = read_validity(
        buffers,
        field_node,
        reader,
        block_offset,
        is_little_endian,
        compression,
        limit,
        scratch,
    )?;

    let length = try_get_array_length(field_node, limit)?;

    let offsets = read_buffer::<O, _>(
        buffers,
        1 + length,
        reader,
        block_offset,
        is_little_endian,
        compression,
        scratch,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| PolarsResult::Ok(Buffer::<O>::from(vec![O::default()])))?;

    let last_offset = offsets.last().unwrap().to_usize();

    let field = ListArray::<O>::get_child_field(&dtype);

    let values = read(
        field_nodes,
        variadic_buffer_counts,
        field,
        &ipc_field.fields[0],
        buffers,
        reader,
        dictionaries,
        block_offset,
        is_little_endian,
        compression,
        Some(last_offset),
        version,
        scratch,
    )?;
    ListArray::try_new(dtype, offsets.try_into()?, values, validity)
}

pub fn skip_list<O: Offset>(
    field_nodes: &mut VecDeque<Node>,
    dtype: &ArrowDataType,
    buffers: &mut VecDeque<IpcBuffer>,
    variadic_buffer_counts: &mut VecDeque<usize>,
) -> PolarsResult<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        polars_err!(
            oos = "IPC: unable to fetch the field for list. The file or stream is corrupted."
        )
    })?;

    let _ = buffers
        .pop_front()
        .ok_or_else(|| polars_err!(oos = "IPC: missing validity buffer."))?;
    let _ = buffers
        .pop_front()
        .ok_or_else(|| polars_err!(oos = "IPC: missing offsets buffer."))?;

    let dtype = ListArray::<O>::get_child_type(dtype);

    skip(field_nodes, dtype, buffers, variadic_buffer_counts)
}
