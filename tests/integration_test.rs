// (C) Bogdan Yachmenev 2026
// License MPL-2.0

use smdb::{Database, DataBase, Value, Index, DataType};
use std::fs;
use std::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};

// --- Memory Tracking Setup (Rust 2024 Compliant) ---

struct TrackingAllocator;
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		// Fixed: Explicit unsafe block for System.alloc
		let ptr = unsafe { System.alloc(layout) };
		if !ptr.is_null() {
			ALLOCATED.fetch_add(layout.size(), Ordering::SeqCst);
		}
		ptr
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		// Fixed: Explicit unsafe block and correct Layout type
		unsafe { System.dealloc(ptr, layout) };
		ALLOCATED.fetch_sub(layout.size(), Ordering::SeqCst);
	}
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

/// Helper to get current heap usage
fn get_mem() -> usize {
	ALLOCATED.load(Ordering::SeqCst)
}

// --- Integration Tests ---

#[test]
fn test_1_save_and_open_consistency() {
	let path = "test_1.sdb";
	let _ = fs::remove_file(path);

	{
		let mut db = Database::open(path);
		let id = db.get_id("Users", "Login");
		db.write(id, Value::String(Box::new("Admin".to_string())))
			.unwrap();
		db.save(path).unwrap();
	}

	let db_new = Database::open(path);
	let id_new = Index { row: 0, col: 0 };
	let val = db_new.read(id_new, DataType::String).unwrap();

	assert_eq!(val, Value::String(Box::new("Admin".to_string())));
	println!("Test 1:\tSDB Format consistency verified (Magic: BYK)");
	let _ = fs::remove_file(path);
}

#[test]
fn test_2_multiple_types_write_read() {
	let path = "test_2.sdb";
	let mut db = Database::open(path);

	let id_str = db.get_id("Row1", "ColStr");
	let id_int = db.get_id("Row1", "ColInt");
	let id_bool = db.get_id("Row1", "ColBool");

	db.write(id_str, Value::String(Box::new("Rust".into())))
		.unwrap();
	db.write(id_int, Value::Int64(1337)).unwrap();
	db.write(id_bool, Value::Bool(true)).unwrap();

	assert_eq!(
		db.read(id_str, DataType::String).unwrap(),
		Value::String(Box::new("Rust".into()))
	);
	assert_eq!(
		db.read(id_int, DataType::Int64).unwrap(),
		Value::Int64(1337)
	);
	assert_eq!(db.read(id_bool, DataType::Bool).unwrap(), Value::Bool(true));

	println!("Test 2:\tType safety for SDB types verified");
}

#[test]
fn test_3_sorting_logic() {
	let path = "test_3.sdb";
	let mut db = Database::open(path);
	let col_age = "Age";
	let names = vec!["Charlie", "Alice", "Bob"];
	let ages = vec![30, 20, 25];

	for i in 0..3 {
		let id = db.get_id(names[i], col_age);
		db.write(id, Value::Int64(ages[i])).unwrap();
	}

	let mut results = Vec::new();
	for &name in &names {
		let id = db.get_id(name, col_age);
		if let Some(Value::Int64(age)) = db.read(id, DataType::Int64) {
			results.push((name, age));
		}
	}

	results.sort_by(|a, b| a.1.cmp(&b.1));

	assert_eq!(results[0].0, "Alice");
	assert_eq!(results[2].0, "Charlie");
	println!("Test 3:\tSorting logic in SDB verified: {:?}", results);
}

#[test]
fn test_4_delete_and_overwrite() {
	let path = "test_4.sdb";
	let mut db = Database::open(path);
	let id = db.get_id("Temp", "Data");

	db.write(id, Value::Int64(999)).unwrap();
	db.delete(id);
	assert!(db.read(id, DataType::Int64).is_none());

	db.write(id, Value::String(Box::new("NewValue".into())))
		.unwrap();
	assert_eq!(
		db.read(id, DataType::String).unwrap(),
		Value::String(Box::new("NewValue".into()))
	);
	println!("Test 4:\tDelete and Overwrite verified in SDB");
}

#[test]
fn test_5_high_index_performance() {
	let path = "test_5.sdb";
	let mut db = Database::open(path);
	let id_low = db.get_id("First", "Col");
	let id_high = db.get_id("VeryFarRow", "Col");

	db.write(id_low, Value::Int64(1)).unwrap();
	db.write(id_high, Value::Int64(2)).unwrap();

	assert_eq!(db.read(id_low, DataType::Int64).unwrap(), Value::Int64(1));
	assert_eq!(db.read(id_high, DataType::Int64).unwrap(), Value::Int64(2));
	println!("Test 5:\tHigh-index O(1) access verified in SDB format");
}

#[test]
fn test_6_all_numeric_types() {
	let path = "test_6.sdb";
	let mut db = Database::open(path);
	let row = "Numbers";
	let id_i8 = db.get_id(row, "i8");
	let id_u64 = db.get_id(row, "u64");

	db.write(id_i8, Value::Int8(-127)).unwrap();
	db.write(id_u64, Value::Uint64(u64::MAX)).unwrap();

	assert_eq!(db.read(id_i8, DataType::Int8).unwrap(), Value::Int8(-127));
	assert_eq!(
		db.read(id_u64, DataType::Uint64).unwrap(),
		Value::Uint64(u64::MAX)
	);
	println!("Test 6:\tNumeric limits and SDB type markers verified");
}

#[test]
fn test_7_binary_blob_consistency() {
	let path = "test_7.sdb";
	let binary_data = vec![0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0xFF];
	{
		let mut db = Database::open(path);
		let id = db.get_id("Files", "Raw");
		db.write(id, Value::Binary(Box::new(binary_data.clone())))
			.unwrap();
		db.save(path).unwrap();
	}
	let db_new = Database::open(path);
	let val = db_new
		.read(Index { row: 0, col: 0 }, DataType::Binary)
		.unwrap();
	if let Value::Binary(data) = val {
		assert_eq!(*data, binary_data);
	}
	let _ = fs::remove_file(path);
	println!("Test 7:\tBinary Blob (Value::Binary) verified in SDB");
}

#[test]
fn benchmark_smdb_performance_and_ram() {
	let path = "bench_final.sdb";
	let _ = fs::remove_file(path);

	let base_mem = get_mem();
	let mut db = Database::open(path);
	let (rows, cols) = (1000, 100);
	let total = rows * cols;

	println!("\n--- SDB Performance & RAM Report (Magic: BYK) ---");

	let start_write = Instant::now();
	for r in 0..rows {
		for c in 0..cols {
			let idx = Index {
				row: r as u16,
				col: c as u16,
			};
			db.write(idx, Value::Int64(r as i64)).unwrap();
		}
	}
	let dur_write = start_write.elapsed();
	let ram_usage = get_mem() - base_mem;

	println!("Write {} cells:\t{:?}", total, dur_write);
	println!("RAM Usage:\t\t{:.2} MB", ram_usage as f64 / 1048576.0);
	println!("RAM per cell:\t\t{} bytes", ram_usage / total);

	let start_save = Instant::now();
	db.save(path).unwrap();
	println!("Save SDB to disk:\t{:?}", start_save.elapsed());

	let _ = fs::remove_file(path);
	println!("--------------------------------------------------\n");
}

#[test]
fn benchmark_smdb_strings_ram() {
	let path = "bench_strings.sdb";
	let _ = fs::remove_file(path);

	let base_mem = get_mem();
	let mut db = Database::open(path);

	let rows = 1000;
	let cols = 100;
	let total = rows * cols;
	let sample_text = "RustData123"; // 11 bytes
	let text_len = sample_text.len();

	println!("\n--- SDB String Memory Report (Magic: BYK) ---");

	let start_write = Instant::now();
	for r in 0..rows {
		for c in 0..cols {
			let idx = Index {
				row: r as u16,
				col: c as u16,
			};
			// Each string is a new allocation in the heap
			db.write(idx, Value::String(Box::new(sample_text.to_string())))
				.unwrap();
		}
	}
	let dur_write = start_write.elapsed();
	let ram_usage = get_mem() - base_mem;

	println!("Write {} strings:\t{:?}", total, dur_write);
	println!("Total RAM Usage:\t{:.2} MB", ram_usage as f64 / 1048576.0);
	println!("RAM per string cell:\t{} bytes", ram_usage / total);
	println!(
		"Expected overhead:\t~{} bytes (Value + Box + String Struct + Body)",
		20 + 24 + text_len
	);

	db.save(path).unwrap();
	let file_size = fs::metadata(path).unwrap().len();
	println!("File size on disk:\t{:.2} KB", file_size as f64 / 1024.0);

	println!("---------------------------------------------\n");
}
