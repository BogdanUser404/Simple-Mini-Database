//(C) Bogdan Yachmenev 2026
//License MPL-2.0

use std::io::{Read, Write};

/// Supported data types stored in the database.
/// All variants (except Empty) are stored in the .sdb format with a type suffix.
#[derive(Debug, PartialEq, Clone)] 
pub enum Value {
	Empty,
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

impl Value {
	/// Returns a unique type identifier for the .sdb v02 format.
	fn type_id(&self) -> u8 {
		match self {
			Value::Empty => 0,
			Value::String(_) => 1,
			Value::Binary(_) => 2,
			Value::Int8(_) => 3,
			Value::Int16(_) => 4,
			Value::Int32(_) => 5,
			Value::Int64(_) => 6,
			Value::Uint8(_) => 7,
			Value::Uint16(_) => 8,
			Value::Uint32(_) => 9,
			Value::Uint64(_) => 10,
			Value::Bool(_) => 11,
		}
	}

	/// Reconstructs a Value from raw bytes based on its type ID.
	fn from_parts(tid: u8, raw: &[u8]) -> Self {
		match tid {
			1 => Value::String(Box::new(String::from_utf8_lossy(raw).to_string())),
			2 => Value::Binary(Box::new(raw.to_vec())),
			3 => Value::Int8(raw[0] as i8),
			4 => Value::Int16(i16::from_be_bytes(raw.try_into().unwrap_or([0; 2]))),
			5 => Value::Int32(i32::from_be_bytes(raw.try_into().unwrap_or([0; 4]))),
			6 => Value::Int64(i64::from_be_bytes(raw.try_into().unwrap_or([0; 8]))),
			7 => Value::Uint8(raw[0]),
			8 => Value::Uint16(u16::from_be_bytes(raw.try_into().unwrap_or([0; 2]))),
			9 => Value::Uint32(u32::from_be_bytes(raw.try_into().unwrap_or([0; 4]))),
			10 => Value::Uint64(u64::from_be_bytes(raw.try_into().unwrap_or([0; 8]))),
			11 => Value::Bool(raw[0] != 0),
			_ => Value::Empty,
		}
	}
}

/// Metadata types for reading data back with type verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
	String, Binary, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64, Bool,
}

/// A coordinate-based index (Row, Column) for database access.
#[derive(Clone, Hash, Default, Copy, PartialEq)]
pub struct Index {
	pub col: u16,
	pub row: u16,
}

/// The core Database structure.
/// Uses Vec<Vec<Value>> for memory efficiency and O(1) access.
pub struct Database {
	data: Vec<Vec<Value>>,
	_filepath: String,
	row_names: Vec<(String, u16)>,
	col_names: Vec<(String, u16)>,
}

/// Common interface for Database operations.
pub trait DataBase {
	/// Writes a Value to the specified Index. Automatically resizes storage.
	fn write(&mut self, index: Index, data: Value) -> Result<(), String>;
	/// Reads a Value from the specified Index. Returns None if Empty or Out of Bounds.
	fn read(&self, index: Index, data_type: DataType) -> Option<Value>;
	/// Marks a cell as Empty at the given Index.
	fn delete(&mut self, index: Index);
	/// Opens a .sdb file with .sdb v02 format validation.
	fn open(path_to: &str) -> Self;
	/// Persists the database to disk in the .sdb v02 format.
	fn save(&self, path: &str) -> std::io::Result<()>;
}

impl Database {
	/// Resolves Row and Column names to an Index. 
	/// Automatically registers new names and assigns IDs.
	pub fn get_id(&mut self, row_name: &str, col_name: &str) -> Index {
		let row = self.row_names.iter().find(|(n, _)| n == row_name).map(|(_, id)| *id)
			.unwrap_or_else(|| {
				let id = self.row_names.len() as u16;
				self.row_names.push((row_name.to_string(), id));
				id
			});
		let col = self.col_names.iter().find(|(n, _)| n == col_name).map(|(_, id)| *id)
			.unwrap_or_else(|| {
				let id = self.col_names.len() as u16;
				self.col_names.push((col_name.to_string(), id));
				id
			});
		Index { row, col }
	}
}

impl DataBase for Database {
	fn write(&mut self, index: Index, data: Value) -> Result<(), String> {
		let r = index.row as usize;
		let c = index.col as usize;

		if r >= self.data.len() { self.data.resize(r + 1, Vec::new()); }
		if c >= self.data[r].len() { self.data[r].resize(c + 1, Value::Empty); }
		
		self.data[r][c] = data;
		Ok(())
	}

	fn read(&self, index: Index, _dt: DataType) -> Option<Value> {
		self.data.get(index.row as usize)?
			.get(index.col as usize)
			.cloned()
			.filter(|v| !matches!(v, Value::Empty))
	}

	fn delete(&mut self, index: Index) {
		if let Some(row) = self.data.get_mut(index.row as usize) {
			if let Some(cell) = row.get_mut(index.col as usize) {
				*cell = Value::Empty;
			}
		}
	}

	fn open(path_to: &str) -> Self {
		let mut data: Vec<Vec<Value>> = Vec::new();
		let (mut rows, mut cols) = (Vec::new(), Vec::new());

		if let Ok(mut file) = std::fs::File::open(path_to) {
			let mut buf = Vec::new();
			file.read_to_end(&mut buf).ok();

			// Magic validation for .sdb v02
			if buf.len() > 4 && &buf[0..4] == &[0x42, 0x59, 0x4B, 0x02] {
				let mut cur = 4;
				for dict in vec![&mut rows, &mut cols] {
					let count = u16::from_be_bytes([buf[cur], buf[cur+1]]) as usize;
					cur += 2;
					for _ in 0..count {
						let id = u16::from_be_bytes([buf[cur], buf[cur+1]]);
						let len = buf[cur+2] as usize;
						let name = String::from_utf8_lossy(&buf[cur+3..cur+3+len]).to_string();
						dict.push((name, id));
						cur += 3 + len;
					}
				}

				let marker = [0xED, 0x65, 0x6E, 0x64, 0xDE];
				while cur + 4 <= buf.len() {
					let c = u16::from_be_bytes([buf[cur], buf[cur+1]]) as usize;
					let r = u16::from_be_bytes([buf[cur+2], buf[cur+3]]) as usize;
					cur += 4;

					if let Some(pos) = buf[cur..].windows(5).position(|w| w == marker) {
						if r >= data.len() { data.resize(r + 1, Vec::new()); }
						if c >= data[r].len() { data[r].resize(c + 1, Value::Empty); }
						
						let tid = buf[cur + pos - 1]; 
						let raw_data = &buf[cur..cur + pos - 1];
						
						data[r][c] = Value::from_parts(tid, raw_data);
						cur += pos + 5;
					} else { break; }
				}
			}
		}
		Database { data, _filepath: path_to.to_string(), row_names: rows, col_names: cols }
	}

	fn save(&self, path: &str) -> std::io::Result<()> {
		let mut file = std::fs::File::create(path)?;
		file.write_all(&[0x42, 0x59, 0x4B, 0x02])?;

		for dict in &[&self.row_names, &self.col_names] {
			file.write_all(&(dict.len() as u16).to_be_bytes())?;
			for (name, id) in *dict {
				file.write_all(&id.to_be_bytes())?;
				file.write_all(&(name.len() as u8).to_be_bytes())?;
				file.write_all(name.as_bytes())?;
			}
		}

		for (r_idx, row) in self.data.iter().enumerate() {
			for (c_idx, val) in row.iter().enumerate() {
				if matches!(val, Value::Empty) { continue; }
				
				file.write_all(&(c_idx as u16).to_be_bytes())?;
				file.write_all(&(r_idx as u16).to_be_bytes())?;

				let b = match val {
					Value::String(s) => s.as_bytes().to_vec(),
					Value::Binary(b) => (**b).clone(),
					Value::Int8(i) => vec![*i as u8],
					Value::Int16(i) => i.to_be_bytes().to_vec(),
					Value::Int32(i) => i.to_be_bytes().to_vec(),
					Value::Int64(i) => i.to_be_bytes().to_vec(),
					Value::Uint8(u) => vec![*u],
					Value::Uint16(u) => u.to_be_bytes().to_vec(),
					Value::Uint32(u) => u.to_be_bytes().to_vec(),
					Value::Uint64(u) => u.to_be_bytes().to_vec(),
					Value::Bool(b) => vec![if *b { 1 } else { 0 }],
					_ => vec![],
				};
				file.write_all(&b)?;
				file.write_all(&[val.type_id()])?; 
				file.write_all(&[0xED, 0x65, 0x6E, 0x64, 0xDE])?;
			}
		}
		Ok(())
	}
}
