use crate::errors::PQRSError;
use crate::errors::PQRSError::CouldNotOpenFile;
use arrow::{
    array::{Array, ArrayBuilder, ArrayRef, StringBuilder},
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use clap::ValueEnum;
use log::debug;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::cmp::min;
use std::fs::File;
use std::ops::Add;
use std::path::Path;
use walkdir::DirEntry;

// calculate the sizes in bytes for one KiB, MiB, GiB, TiB, PiB
static ONE_KI_B: i64 = 1024;
static ONE_MI_B: i64 = ONE_KI_B * 1024;
static ONE_GI_B: i64 = ONE_MI_B * 1024;
static ONE_TI_B: i64 = ONE_GI_B * 1024;
static ONE_PI_B: i64 = ONE_TI_B * 1024;

/// Output formats supported. Only cat command support CSV format.
#[derive(Copy, Clone, Debug)]
pub enum Formats {
    Default,
    Csv(NestedFieldFormat),
    CsvNoHeader(NestedFieldFormat),
    Json,
}

impl std::fmt::Display for Formats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// How to handle nested field types in formats that do not support them, like CSV.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum NestedFieldFormat {
    /// Output an error message and abort.
    Error,
    /// Omit columns with nested field types.
    Omit,
    /// Encode nested fields as JSON.
    Json,
}

impl std::fmt::Display for NestedFieldFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Check if a particular path is present on the filesystem
pub fn check_path_present<P: AsRef<Path>>(file_path: P) -> bool {
    Path::new(file_path.as_ref()).exists()
}

/// Open the file based on the pat and return the File object, else return error
pub fn open_file<P: AsRef<Path>>(file_name: P) -> Result<File, PQRSError> {
    let file_name = file_name.as_ref();
    let path = Path::new(file_name);
    let file = match File::open(path) {
        Err(_) => return Err(CouldNotOpenFile(file_name.to_path_buf())),
        Ok(f) => f,
    };

    Ok(file)
}

/// Check if the given entry in the walking tree is a hidden file
pub fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

/// Print the given number of records in either json or json-like format
pub fn print_rows(
    file: File,
    num_records: Option<usize>,
    format: Formats,
) -> Result<(), PQRSError> {
    let mut left = num_records;

    match format {
        Formats::Default => {
            let parquet_reader = SerializedFileReader::new(file)?;
            let mut iter = parquet_reader.get_row_iter(None)?;

            let mut start: usize = 0;
            let end: usize = num_records.unwrap_or(0);
            // if num_records is None, print all the files
            let all_records = num_records.is_none();

            while all_records || start < end {
                match iter.next() {
                    Some(row) => match row {
                        Ok(rowval) => print_row(&rowval, format),
                        Err(_) => todo!(),
                    },
                    None => break,
                }
                start += 1;
            }
        }
        Formats::Json => {
            let arrow_reader = ArrowReaderBuilder::try_new(file)?;
            let batch_reader = arrow_reader.with_batch_size(8192).build()?;
            let mut writer = arrow::json::LineDelimitedWriter::new(std::io::stdout());

            for maybe_batch in batch_reader {
                if left == Some(0) {
                    break;
                }

                let mut batch = maybe_batch?;
                if let Some(l) = left {
                    if batch.num_rows() <= l {
                        left = Some(l - batch.num_rows());
                    } else {
                        let n = min(batch.num_rows(), l);
                        batch = batch.slice(0, n);
                        left = Some(0);
                    }
                };

                writer.write(&batch)?;
            }

            writer.finish()?;
        }
        Formats::Csv(nested_format) => {
            let arrow_reader = ArrowReaderBuilder::try_new(file)?;
            let batch_reader = arrow_reader.with_batch_size(8192).build()?;
            let mut writer = arrow::csv::Writer::new(std::io::stdout());

            for maybe_batch in batch_reader {
                if left == Some(0) {
                    break;
                }

                let batch = maybe_batch?;
                let mut batch = nested_fields_to_json(batch, nested_format)?;
                if let Some(l) = left {
                    if batch.num_rows() <= l {
                        left = Some(l - batch.num_rows());
                    } else {
                        let n = min(batch.num_rows(), l);
                        batch = batch.slice(0, n);
                        left = Some(0);
                    }
                };

                writer.write(&batch)?;
            }
        }
        Formats::CsvNoHeader(nested_format) => {
            let arrow_reader = ArrowReaderBuilder::try_new(file)?;
            let batch_reader = arrow_reader.with_batch_size(8192).build()?;
            let writer_builder = arrow::csv::WriterBuilder::new();
            let mut writer = writer_builder.with_header(false).build(std::io::stdout());

            for maybe_batch in batch_reader {
                if left == Some(0) {
                    break;
                }

                let batch = maybe_batch?;
                let mut batch = nested_fields_to_json(batch, nested_format)?;
                if let Some(l) = left {
                    if batch.num_rows() <= l {
                        left = Some(l - batch.num_rows());
                    } else {
                        let n = min(batch.num_rows(), l);
                        batch = batch.slice(0, n);
                        left = Some(0);
                    }
                };

                writer.write(&batch)?;
            }
        }
    }
    Ok(())
}

/// Encodes nested field types to string columns in JSON encoding for use in CSV output.
fn nested_fields_to_json(
    batch: RecordBatch,
    nested_format: NestedFieldFormat,
) -> Result<RecordBatch, PQRSError> {
    let schema = batch.schema();
    let columns = batch.columns();

    if matches!(nested_format, NestedFieldFormat::Error) {
        let nested_fields = schema
            .fields()
            .iter()
            .filter(|field| field.data_type().is_nested())
            .collect::<Vec<_>>();
        if nested_fields.is_empty() {
            return Ok(batch);
        } else {
            return Err(PQRSError::NestedFieldsError(
                nested_fields
                    .iter()
                    .map(|field| field.name().to_owned())
                    .collect(),
            ));
        }
    }

    let mut new_fields = Vec::new();
    let mut new_columns = Vec::<ArrayRef>::new();
    for (field, column) in std::iter::zip(schema.fields(), columns) {
        if field.data_type().is_nested() {
            match nested_format {
                NestedFieldFormat::Error => {
                    unreachable!();
                }
                NestedFieldFormat::Omit => {}
                NestedFieldFormat::Json => {
                    new_fields.push(FieldRef::new(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    )));
                    new_columns.push(array_to_json(ArrayRef::clone(column)));
                }
            }
        } else {
            new_fields.push(FieldRef::clone(field));
            new_columns.push(ArrayRef::clone(column));
        }
    }

    let new_schema = SchemaRef::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    Ok(RecordBatch::try_new(new_schema, new_columns).unwrap())
}

/// Encodes an array as JSON, returning a `StringArray`.
fn array_to_json(array: ArrayRef) -> ArrayRef {
    let array_len = array.len();

    // arrow_json doesn't have a way to encode a single value or even a single array. So we encode
    // a batch with one column, then split the result into lines.
    let batch = RecordBatch::try_from_iter([("", array)]).unwrap();

    let mut buf = Vec::new();
    let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let buf_str = String::from_utf8(buf).unwrap();
    let mut builder = StringBuilder::with_capacity(array_len, 0);
    for line in buf_str.lines() {
        // Format is {"":VALUE}
        builder.append_value(&line[4..line.len() - 1]);
    }
    assert_eq!(array_len, builder.len());

    ArrayBuilder::finish(&mut builder)
}

/// Print the random sample of given size in either json or json-like format
pub fn print_rows_random(
    file: File,
    sample_size: usize,
    format: Formats,
) -> Result<(), PQRSError> {
    let parquet_reader = SerializedFileReader::new(file.try_clone()?)?;
    let iter = parquet_reader.get_row_iter(None)?;

    // find the number of records present in the file
    let total_records_in_file: i64 = get_row_count(file)?;
    // push all the indexes into the vector initially
    let mut indexes = (0..total_records_in_file).collect::<Vec<_>>();
    debug!("Original indexes: {:?}", indexes);

    // shuffle the indexes to randomize the vector
    let mut rng = thread_rng();
    indexes.shuffle(&mut rng);
    debug!("Shuffled indexes: {:?}", indexes);

    // take only the given number of records from the vector
    indexes = indexes.into_iter().take(sample_size).collect::<Vec<_>>();

    debug!("Sampled indexes: {:?}", indexes);

    for (start, row) in (0_i64..).zip(iter) {
        if indexes.contains(&start) {
            match row {
                Ok(rowval) => print_row(&rowval, format),
                Err(_) => todo!(),
            }
        }
    }

    Ok(())
}

/// A representation of Parquet file in a form that can be used for merging
#[derive(Debug)]
pub struct ParquetData {
    /// The schema of the parquet file
    pub schema: Schema,
    /// Collection of the record batches in the parquet file
    pub batches: Vec<RecordBatch>,
    /// The number of rows present in the parquet file
    pub rows: usize,
}

impl Add for ParquetData {
    type Output = Self;

    /// Combine two given parquet files
    fn add(mut self, mut rhs: Self) -> Self::Output {
        // the combined data contains data from both the structs
        let mut combined_data = Vec::new();
        combined_data.append(&mut self.batches);
        combined_data.append(&mut rhs.batches);

        Self {
            // the schema from the lhs is maintained, the assumption is that this
            // method is used only on files that share the same schema
            schema: self.schema,
            batches: combined_data,
            rows: self.rows + rhs.rows,
        }
    }
}

/// Return the row batches, rows and schema for a given parquet file
pub fn get_row_batches(file: File) -> Result<ParquetData, PQRSError> {
    let arrow_reader = ArrowReaderBuilder::try_new(file)?;

    let schema = Schema::clone(arrow_reader.schema());
    let record_batch_reader = arrow_reader.with_batch_size(1024).build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();

    let mut rows = 0;
    for maybe_batch in record_batch_reader {
        let record_batch = maybe_batch?;
        rows += record_batch.num_rows();

        batches.push(record_batch);
    }

    Ok(ParquetData {
        schema,
        batches,
        rows,
    })
}

/// Print the given parquet rows in json or json-like format
fn print_row(row: &Row, format: Formats) {
    match format {
        Formats::Default => println!("{}", row),
        Formats::Csv(_) => println!("Unsupported! {}", row),
        Formats::CsvNoHeader(_) => println!("Unsupported! {}", row),
        Formats::Json => println!("{}", row.to_json_value()),
    }
}

/// Return the number of rows in the given parquet file
pub fn get_row_count(file: File) -> Result<i64, PQRSError> {
    let parquet_reader = SerializedFileReader::new(file)?;
    let row_group_metadata = parquet_reader.metadata().row_groups();
    // The parquet file is made up of blocks (also called row groups)
    // The row group metadata contains information about all the row groups present in the data
    // Each row group maintains the number of rows present in the block
    // Summing across all the row groups contains the total number of rows present in the file
    let total_num_rows = row_group_metadata.iter().map(|rg| rg.num_rows()).sum();

    Ok(total_num_rows)
}

/// Return the uncompressed and compressed size of the given file
pub fn get_size(file: File) -> Result<(i64, i64), PQRSError> {
    let parquet_reader = SerializedFileReader::new(file)?;
    let row_group_metadata = parquet_reader.metadata().row_groups();

    // Parquet format compresses data at a column level.
    // To calculate the size of the file (compressed or uncompressed), we need to sum
    // across all the row groups present in the parquet file. This is similar to how
    // we calculate the row count in the method above.
    // Do note that this size does not take the footer size into consideration.
    let uncompressed_size = row_group_metadata
        .iter()
        .map(|rg| rg.total_byte_size())
        .sum();
    let compressed_size = row_group_metadata
        .iter()
        .map(|rg| rg.compressed_size())
        .sum();

    Ok((uncompressed_size, compressed_size))
}

/// Pretty print the given size using human readable format
pub fn get_pretty_size(bytes: i64) -> String {
    if bytes / ONE_KI_B < 1 {
        return format!("{} Bytes", bytes);
    }

    if bytes / ONE_MI_B < 1 {
        return format!("{:.3} KiB", bytes / ONE_KI_B);
    }

    if bytes / ONE_GI_B < 1 {
        return format!("{:.3} MiB", bytes / ONE_MI_B);
    }

    if bytes / ONE_TI_B < 1 {
        return format!("{:.3} GiB", bytes / ONE_GI_B);
    }

    if bytes / ONE_PI_B < 1 {
        return format!("{:.3} TiB", bytes / ONE_TI_B);
    }

    format!("{:.3} PiB", bytes / ONE_PI_B)
}
