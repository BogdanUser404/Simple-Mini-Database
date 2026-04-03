//(C) Bogdan Yachmenev 2026
//License MPL-2.0

use std::io::{Read, Write};

/// The maximum number of columns per row for flat indexing.
/// This allows us to maintain O(1) access speed.
const MAX_COL: usize = 65536;

/// Supported data types stored in the database cells.
#[derive(Debug, PartialEq, Hash, Clone)] 
pub enum Value {
	String(Box<String>),
	Binary(Box<Vec<u8>>),
	Int8(i8),
	Int16(i16),
	Int32(i32),
	Int64(i64),
	Uint8(u8),
	Uint16(u16),
	Uint32(u32),
	Uint64(u64),
	Bool(bool),
}

/// Metadata types for reading data back from the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
	String,
	Binary,
	Int8,
	Int16,
	Int32,
	Int64,
	Uint8,
	Uint16,
	Uint32,
	Uint64,
	Bool,
}

/// A coordinate-based index for database cells.
#[derive(Clone, Hash, Default, Copy, PartialEq)]
pub struct Index {
	pub col: u16,
	pub row: u16,
}

/// The core Database structure.
/// Uses a flat Vec<Vec<u8>> for O(1) data access and linear vectors for name mapping.
pub struct Database {
	data: Vec<Vec<u8>>,
	_filepath: String,
	row_names: Vec<(String, u16)>,
	col_names: Vec<(String, u16)>,
}

pub trait DataBase {
	fn write(&mut self, index: Index, data: Value) -> Result<(), String>;
	fn read(&self, index: Index, data_type: DataType) -> Option<Value>;
	fn delete(&mut self, index: Index);
	fn open(path_to: &str) -> Self;
	fn save(&self, path: &str) -> std::io::Result<()>;
}

impl Database {
	/// Resolves human-readable names to a numeric Index.
	/// If the name doesn't exist, it registers a new ID automatically.
	pub fn get_id(&mut self, row_name: &str, col_name: &str) -> Index {
		let row = self.row_names.iter()
			.find(|(name, _)| name == row_name)
			.map(|(_, id)| *id)
			.unwrap_or_else(|| {
				let id = self.row_names.len() as u16;
				self.row_names.push((row_name.to_string(), id));
				id
			});

		let col = self.col_names.iter()
			.find(|(name, _)| name == col_name)
			.map(|(_, id)| *id)
			.unwrap_or_else(|| {
				let id = self.col_names.len() as u16;
				self.col_names.push((col_name.to_string(), id));
				id
			});

		Index { row, col }
	}
}

impl DataBase for Database {
	/// Writes data to a specific index. Complexity: O(1) (excluding potential resize).
	fn write(&mut self, index: Index, data: Value) -> Result<(), String> {
		let target_index = (index.row as usize * MAX_COL) + index.col as usize;

		if target_index >= self.data.len() {
			self.data.resize(target_index + 1, Vec::new());
		}

		let bytes = match data {
			Value::String(s) => (*s).clone().into_bytes(),
			Value::Binary(b) => (*b).clone(),
			Value::Int64(i)  => i.to_be_bytes().to_vec(),
			Value::Int32(i)  => i.to_be_bytes().to_vec(),
			Value::Int16(i)  => i.to_be_bytes().to_vec(),
			Value::Int8(i)   => vec![i as u8],
			Value::Uint64(u) => u.to_be_bytes().to_vec(),
			Value::Uint32(u) => u.to_be_bytes().to_vec(),
			Value::Uint16(u) => u.to_be_bytes().to_vec(),
			Value::Uint8(u)  => vec![u],
			Value::Bool(b)   => vec![if b { 1 } else { 0 }],
		};

		self.data[target_index] = bytes;
		Ok(())
	}

	/// Reads data from a specific index. Complexity: O(1).
	fn read(&self, index: Index, data_type: DataType) -> Option<Value> {
		let target_index = (index.row as usize * MAX_COL) + index.col as usize;
		let raw = self.data.get(target_index)?;
		
		if raw.is_empty() {
			return None;
		}

		match data_type {
			DataType::String => {
				let s = String::from_utf8(raw.clone()).expect("UTF-8 Error");
				Some(Value::String(Box::new(s)))
			},
			DataType::Binary => Some(Value::Binary(Box::new(raw.clone()))),
			DataType::Int64 => {
				let b: [u8; 8] = raw.as_slice().try_into().ok()?;
				Some(Value::Int64(i64::from_be_bytes(b)))
			},
			DataType::Int32 => {
				let b: [u8; 4] = raw.as_slice().try_into().ok()?;
				Some(Value::Int32(i32::from_be_bytes(b)))
			},
			DataType::Int8 => Some(Value::Int8(raw[0] as i8)),
			DataType::Uint8 => Some(Value::Uint8(raw[0])),
			DataType::Bool => Some(Value::Bool(raw[0] != 0)),
			_ => panic!("Type parser not fully implemented"),
		}
	}

	/// Clears data at a specific index without removing the index entry.
	fn delete(&mut self, index: Index) {
		let target_index = (index.row as usize * MAX_COL) + index.col as usize;
		if target_index < self.data.len() {
			self.data[target_index].clear();
		}
	}

	/// Opens a database file, validates the BYK magic number, and loads dictionaries + data.
	fn open(path_to: &str) -> Self {
		let mut data_store = Vec::new();
		let mut rows = Vec::new();
		let mut cols = Vec::new();

		if let Ok(mut file) = std::fs::File::open(path_to) {
			let mut buffer = Vec::new();
			file.read_to_end(&mut buffer).expect("Read failed");

			// Validate Magic Number: BYK (0x42 0x59 0x4B) + Version 01
			if buffer.len() < 4 || &buffer[0..4] != &[0x42, 0x59, 0x4B, 0x01] {
				panic!("BYK Format Error: Invalid magic number or version");
			}

			let mut cursor = 4;

			// Load row and column name dictionaries
			for target_dict in vec![&mut rows, &mut cols] {
				let count = u16::from_be_bytes([buffer[cursor], buffer[cursor+1]]) as usize;
				cursor += 2;
				for _ in 0..count {
					let id = u16::from_be_bytes([buffer[cursor], buffer[cursor+1]]);
					let len = buffer[cursor+2] as usize;
					let name = String::from_utf8_lossy(&buffer[cursor+3..cursor+3+len]).to_string();
					target_dict.push((name, id));
					cursor += 3 + len;
				}
			}

			// Load cell data using the [ED 65 6E 64 DE] sentinel marker
			let marker = [0xED, 0x65, 0x6E, 0x64, 0xDE];
			while cursor + 4 <= buffer.len() {
				let c = u16::from_be_bytes([buffer[cursor], buffer[cursor+1]]) as usize;
				let r = u16::from_be_bytes([buffer[cursor+2], buffer[cursor+3]]) as usize;
				cursor += 4;

				let target_pos = (r * MAX_COL) + c;

				if let Some(pos) = buffer[cursor..].windows(5).position(|w| w == marker) {
					let end_pos = cursor + pos;
					let raw_data = buffer[cursor..end_pos].to_vec();

					if target_pos >= data_store.len() {
						data_store.resize(target_pos + 1, Vec::new());
					}
					data_store[target_pos] = raw_data;
					cursor = end_pos + 5;
				} else { break; }
			}
		}

		Database { data: data_store, _filepath: path_to.to_string(), row_names: rows, col_names: cols }
	}

	/// Saves the database to disk, including magic number, name dictionaries, and data cells.
	fn save(&self, path: &str) -> std::io::Result<()> {
		let mut file = std::fs::File::create(path)?;

		// Write Header: BYK v01
		file.write_all(&[0x42, 0x59, 0x4B, 0x01])?;

		// Write dictionaries (row_names and col_names)
		for dict in &[&self.row_names, &self.col_names] {
			file.write_all(&(dict.len() as u16).to_be_bytes())?;
			for (name, id) in *dict {
				file.write_all(&id.to_be_bytes())?;
				file.write_all(&(name.len() as u8).to_be_bytes())?;
				file.write_all(name.as_bytes())?;
			}
		}

		// Write cell data with index metadata and sentinel markers
		for (idx, content) in self.data.iter().enumerate() {
			if content.is_empty() { continue; }
			let r = (idx / MAX_COL) as u16;
			let c = (idx % MAX_COL) as u16;
			
			file.write_all(&c.to_be_bytes())?;
			file.write_all(&r.to_be_bytes())?;
			file.write_all(content)?;
			file.write_all(&[0xED, 0x65, 0x6E, 0x64, 0xDE])?;
		}
		
		file.flush()?;
		Ok(())
	}
}
