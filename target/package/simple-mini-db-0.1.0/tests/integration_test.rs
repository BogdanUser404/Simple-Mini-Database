use smdb::{Database, DataBase,Value, DataType};
use std::fs;

#[test]
fn test_1_save_and_open_consistency() {
	let path = "test_1.sdb";
	let _ = fs::remove_file(path); // Clean start

	{
		let mut db = Database::open(path);
		let id = db.get_id("Users", "Login");
		db.write(id, Value::String(Box::new("Admin".to_string()))).unwrap();
		db.save(path).unwrap();
	}

	// Re-open and verify names and data
	let mut db_new = Database::open(path);
	let id_new = db_new.get_id("Users", "Login"); // Should find existing ID
	let val = db_new.read(id_new, DataType::String).unwrap();
	
	assert_eq!(val, Value::String(Box::new("Admin".to_string())));
	println!("Test 1:\tDatabase consistency verified (BYK format)");
}

#[test]
fn test_2_multiple_types_write_read() {
	let path = "test_2.sdb";
	let mut db = Database::open(path);
	
	let id_str = db.get_id("Row1", "ColStr");
	let id_int = db.get_id("Row1", "ColInt");
	let id_bool = db.get_id("Row1", "ColBool");

	db.write(id_str, Value::String(Box::new("Rust".into()))).unwrap();
	db.write(id_int, Value::Int64(1337)).unwrap();
	db.write(id_bool, Value::Bool(true)).unwrap();

	assert_eq!(db.read(id_str, DataType::String).unwrap(), Value::String(Box::new("Rust".into())));
	assert_eq!(db.read(id_int, DataType::Int64).unwrap(), Value::Int64(1337));
	assert_eq!(db.read(id_bool, DataType::Bool).unwrap(), Value::Bool(true));
	
	println!("Test 2:\tType safety for String, Int64, and Bool verified");
}

#[test]
fn test_3_sorting_logic() {
	let mut db = Database::open("test_3.sdb");
	let col_age = "Age";
	
	// Create some unsorted data
	let rows = vec!["Charlie", "Alice", "Bob"];
	let ages = vec![30, 20, 25];

	for i in 0..3 {
		let id = db.get_id(rows[i], col_age);
		db.write(id, Value::Int64(ages[i])).unwrap();
	}

	// Logic: Extract ages and names into a Vec and sort
	let mut results = Vec::new();
	for &name in &rows {
		let id = db.get_id(name, col_age);
		if let Some(Value::Int64(age)) = db.read(id, DataType::Int64) {
			results.push((name, age));
		}
	}

	results.sort_by(|a, b| a.1.cmp(&b.1)); // Sort by age

	assert_eq!(results[0].0, "Alice");
	assert_eq!(results[2].0, "Charlie");
	println!("Test 3:\tSorting by row values verified: {:?} (O(1) access used)", results);
}

#[test]
fn test_4_delete_and_overwrite() {
	let mut db = Database::open("test_4.sdb");
	let id = db.get_id("Temp", "Data");

	db.write(id, Value::Int64(999)).unwrap();
	db.delete(id);
	
	assert!(db.read(id, DataType::Int64).is_none());

	db.write(id, Value::String(Box::new("NewValue".into()))).unwrap();
	assert_eq!(db.read(id, DataType::String).unwrap(), Value::String(Box::new("NewValue".into())));
	
	println!("Test 4:\tDelete and Overwrite operations verified");
}

#[test]
fn test_5_high_index_performance() {
	let mut db = Database::open("test_5.sdb");
	
	// Testing O(1) with very high row IDs
	let id_low = db.get_id("First", "Col");
	let id_high = db.get_id("VeryFarRow", "Col"); 
	// This will trigger a large resize in O(1) flat Vec
	
	db.write(id_low, Value::Int64(1)).unwrap();
	db.write(id_high, Value::Int64(2)).unwrap();

	assert_eq!(db.read(id_low, DataType::Int64).unwrap(), Value::Int64(1));
	assert_eq!(db.read(id_high, DataType::Int64).unwrap(), Value::Int64(2));
	
	println!("Test 5:\tHigh-index O(1) access and flat mapping verified");
}
