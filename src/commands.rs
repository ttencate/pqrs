use crate::errors::PQRSError;
use crate::errors::PQRSError::FileNotFound;
use crate::utils::{check_path_present, get_pretty_size, get_row_count, get_size};
use crate::utils::{open_file, print_rows};
use clap::ArgMatches;
use log::debug;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::{print_file_metadata, print_parquet_metadata};
use std::option::Option::Some;

pub fn run_command(matches: ArgMatches) -> Result<(), PQRSError> {
    match matches.subcommand() {
        ("cat", Some(m)) => cat(m),
        ("head", Some(m)) => head(m),
        ("schema", Some(m)) => schema(m),
        ("rowcount", Some(m)) => rowcount(m),
        ("size", Some(m)) => size(m),
        _ => Ok(()),
    }
}

fn cat(matches: &ArgMatches) -> Result<(), PQRSError> {
    let file_names: Vec<&str> = matches.values_of("files").unwrap().collect();
    let use_json = matches.is_present("json");
    debug!("The file names to read are: {:#?}", &file_names);
    debug!("Use JSON Output format: {:#?}", &use_json);

    // make sure all files are present before printing any data
    for file_name in &file_names {
        if !check_path_present(*file_name) {
            return Err(FileNotFound(String::from(*file_name)));
        }
    }

    for file_name in file_names {
        let file = open_file(file_name)?;
        print_rows(file, None, use_json)?;
    }

    Ok(())
}

fn head(matches: &ArgMatches) -> Result<(), PQRSError> {
    let file_name = matches.value_of("file").unwrap();
    let num_records = matches.value_of("records").unwrap().parse()?;
    let use_json = matches.is_present("json");
    debug!("The file name to read is: {:#?}", &file_name);
    debug!("Number of records to print: {:#?}", &num_records);
    debug!("Use JSON Output format: {:#?}", &use_json);

    if !check_path_present(file_name) {
        return Err(FileNotFound(String::from(file_name)));
    }

    let file = open_file(file_name)?;
    print_rows(file, Some(num_records), use_json)?;

    Ok(())
}

fn schema(matches: &ArgMatches) -> Result<(), PQRSError> {
    let file_names: Vec<&str> = matches.values_of("files").unwrap().collect();
    let use_detailed = matches.is_present("detailed");

    debug!("The file names to read are: {:#?}", &file_names);
    debug!("Print Detailed output: {:#?}", &use_detailed);

    // make sure all files are present before printing any data
    for file_name in &file_names {
        if !check_path_present(*file_name) {
            return Err(FileNotFound(String::from(*file_name)));
        }
    }

    for file_name in file_names {
        let file = open_file(file_name)?;
        match SerializedFileReader::new(file) {
            Err(e) => return Err(PQRSError::ParquetError(e)),
            Ok(parquet_reader) => {
                let metadata = parquet_reader.metadata();
                println!("Metadata for file: {}", &file_name);
                println!();
                if use_detailed {
                    print_parquet_metadata(&mut std::io::stdout(), &metadata);
                } else {
                    print_file_metadata(&mut std::io::stdout(), &metadata.file_metadata());
                }
            }
        }
    }

    Ok(())
}

fn rowcount(matches: &ArgMatches) -> Result<(), PQRSError> {
    let file_names: Vec<&str> = matches.values_of("files").unwrap().collect();
    debug!("The file names to read are: {:#?}", &file_names);

    // make sure all files are present before printing any data
    for file_name in &file_names {
        if !check_path_present(*file_name) {
            return Err(FileNotFound(String::from(*file_name)));
        }
    }

    for file_name in file_names {
        let file = open_file(file_name)?;
        let row_count = get_row_count(file)?;
        println!("File Name: {}: {} rows", &file_name, &row_count);
    }

    Ok(())
}

fn size(matches: &ArgMatches) -> Result<(), PQRSError> {
    let file_names: Vec<&str> = matches.values_of("files").unwrap().collect();
    debug!("The file names to read are: {:#?}", &file_names);

    // make sure all files are present before printing any data
    for file_name in &file_names {
        if !check_path_present(*file_name) {
            return Err(FileNotFound(String::from(*file_name)));
        }
    }

    println!("Size in Bytes:");
    for file_name in file_names {
        let file = open_file(file_name)?;
        let size_info = get_size(file)?;

        println!();
        println!("File Name: {}", &file_name);
        println!("Uncompressed Size: {}", size_info.0);
        println!("Compressed Size: {}", size_info.1);
        println!();
        println!("Uncompressed Size: {}", get_pretty_size(size_info.0));
        println!("Compressed Size: {}", get_pretty_size(size_info.1));
    }

    Ok(())
}