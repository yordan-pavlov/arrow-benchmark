//use csv;
use chrono::{NaiveDate, NaiveTime};
use std::{time::Instant, sync::Arc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use arrow::array::{PrimitiveArray, DictionaryArray, StringArray, Array, BooleanArray};
use arrow::datatypes::{Float32Type, Date32Type, Int8Type, Schema, Field, DataType, DateUnit};
use arrow::record_batch::RecordBatch;
use arrow::compute;

// fn main() {
//     let land_registry_records = load_land_registry_data();
//     let record_batch = load_arrow_record_batch(&land_registry_records);
//     for _ in 0..3000 {
//         let timer_start = Instant::now();
//         //let count = filter_with_loop(&land_registry_records);
//         let count = filter_with_arrow_simd_scalar(&record_batch);
//         let timer_end = Instant::now();
//         println!("Found {} records in {}us", count, timer_end.duration_since(timer_start).as_micros());
//     }
// }

fn bench_filter(c: &mut Criterion) {
    let land_registry_records = load_land_registry_data();
    let record_batch = load_arrow_record_batch(&land_registry_records);
    let mut count = 0;

    let mut group = c.benchmark_group("filter");

    group.bench_function("filter with loop", |b| b.iter(|| count = filter_with_loop(black_box(&land_registry_records))));
    println!("found {} records", count);

    group.bench_function("filter with iter", |b| b.iter(|| count = filter_with_iter(black_box(&land_registry_records))));
    println!("found {} records", count);

    group.bench_function("filter with arrow loop", |b| b.iter(|| count = filter_with_arrow_loop(black_box(&record_batch))));
    println!("found {} records", count);

    group.bench_function("filter with arrow NO SIMD", |b| b.iter(|| count = filter_with_arrow_no_simd(black_box(&record_batch))));
    println!("found {} records", count);

    group.bench_function("filter with arrow SIMD (array)", |b| b.iter(|| count = filter_with_arrow_simd_array(black_box(&record_batch))));
    println!("found {} records", count);

    group.bench_function("filter with arrow SIMD (scalar)", |b| b.iter(|| count = filter_with_arrow_simd_scalar(black_box(&record_batch))));
    println!("found {} records", count);

    group.finish();
}

criterion_group!(benches, bench_filter);
criterion_main!(benches);

#[inline]
fn filter_with_loop(land_registry_records: &Vec<LandRegistryRecord>) -> usize
{
    //println!("Filtering land registry records with loop");
    //let date_filter = DateTimeOffset.Parse("2019-01-01");
    let date_filter = NaiveDate::parse_from_str("2019-01-01", "%Y-%m-%d").unwrap();
    let property_type_filter: Vec<String> = (vec![ "D", "S", "T" ]).into_iter().map(|v| String::from(v)).collect();

    let mut count: usize = 0;
    //let filter_start = Instant::now();
    for r in land_registry_records {
        if r.date >= date_filter && r.price > 5000000.0 
            && property_type_filter.contains(&r.property_type) && &r.tenure == "F" {
            count += 1;
        }
    }
    //let filter_end = Instant::now();
    //println!("Found {} records in {}ms", count, filter_end.duration_since(filter_start).as_millis());
    return count;
}

#[inline]
fn filter_with_iter(land_registry_records: &Vec<LandRegistryRecord>) -> usize
{
    //println!("Filtering land registry records with loop");
    //let date_filter = DateTimeOffset.Parse("2019-01-01");
    let date_filter = NaiveDate::parse_from_str("2019-01-01", "%Y-%m-%d").unwrap();
    let property_type_filter: Vec<String> = (vec![ "D", "S", "T" ]).into_iter().map(|v| String::from(v)).collect();

    
    //let filter_start = Instant::now();
    let count = land_registry_records
        .into_iter()
        .filter(|r| r.date >= date_filter && r.price > 5000000.0 
            && property_type_filter.contains(&r.property_type) && &r.tenure == "F")
        .count();
    
    //let filter_end = Instant::now();
    //println!("Found {} records in {}ms", count, filter_end.duration_since(filter_start).as_millis());
    return count;
}

fn filter_with_arrow_loop(land_registry_records: &RecordBatch) -> usize
{
    let price_array = land_registry_records.column(0).as_any().downcast_ref::<PrimitiveArray<Float32Type>>().unwrap();
    let mut select_mask = vec![true; price_array.len()];
    for i in 0..price_array.len() {
        select_mask[i] &= price_array.value(i) > 5000000.0;
    }

    let property_type_array = land_registry_records.column(1).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
    let property_type_filter: Vec<i8> = (vec![ "D", "S", "T" ]).into_iter().map(|v| find_dictionary_key(&property_type_array, v).unwrap()).collect();
    // for (i, v) in property_type_array.keys().enumerate() {
    //     select_mask[i] &= property_type_filter.contains(&v.unwrap());
    // }
    let property_type_values = PrimitiveArray::<Int8Type>::from(property_type_array.data());
    // for i in 0..property_type_values.len() {
    //     select_mask[i] &= property_type_filter.contains(&property_type_values.value(i));
    // }
    let mut property_type_mask = vec![false; property_type_array.len()];
    for f in property_type_filter {
        for i in 0..property_type_values.len() {
            property_type_mask[i] |= f == property_type_values.value(i);
        }
    }
    for i in 0..property_type_mask.len() {
        select_mask[i] &= property_type_mask[i];
    }

    let tenure_array = land_registry_records.column(2).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
    let tenure_filter = find_dictionary_key(&tenure_array, "F").unwrap();
    // for (i, v) in tenure_array.keys().enumerate() {
    //     select_mask[i] &= tenure_filter == v.unwrap();
    // }
    let tenure_values = PrimitiveArray::<Int8Type>::from(tenure_array.data());
    for i in 0..tenure_values.len() {
        select_mask[i] &= tenure_filter == tenure_values.value(i);
    }

    let date_array = land_registry_records.column(3).as_any().downcast_ref::<PrimitiveArray<Date32Type>>().unwrap();
    let date_filter = get_timestamp(&NaiveDate::from_ymd(2019,1, 1));
    for i in 0..date_array.len() {
        select_mask[i] &= date_array.value(i) >= date_filter;
    }

    select_mask.into_iter().filter(|v| *v).count()
}

fn filter_with_arrow_no_simd(land_registry_records: &RecordBatch) -> usize {
    // filter by price
    let price_array = land_registry_records.column(0).as_any().downcast_ref::<PrimitiveArray<Float32Type>>().unwrap();
    let mut select_mask = compute::no_simd_compare_op_scalar(price_array, 5000000.0, |a, b| a > b).unwrap();

    // filter by property type
    let property_type_array = land_registry_records.column(1).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
    let property_type_filter: Vec<i8> = (vec![ "D", "S", "T" ]).into_iter().map(|v| find_dictionary_key(&property_type_array, v).unwrap()).collect();
    let property_type_values = PrimitiveArray::<Int8Type>::from(property_type_array.data());
    let mut property_type_mask: BooleanArray = vec![false; property_type_array.len()].into();
    for f in property_type_filter {
        let temp_property_mask = compute::no_simd_compare_op_scalar(&property_type_values, f, |a, b| a == b).unwrap();
        property_type_mask = compute::or(&property_type_mask, &temp_property_mask).unwrap();
    }
    select_mask = compute::and(&select_mask, &property_type_mask).unwrap();
    
    // filter by tenure
    let tenure_array = land_registry_records.column(2).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
    let tenure_filter = find_dictionary_key(&tenure_array, "F").unwrap();
    let tenure_values = PrimitiveArray::<Int8Type>::from(tenure_array.data());
    select_mask = compute::and(
        &select_mask, 
        &compute::no_simd_compare_op_scalar(&tenure_values, tenure_filter, |a, b| a == b).unwrap()
    ).unwrap();
    
    // filter by date
    let date_array = land_registry_records.column(3).as_any().downcast_ref::<PrimitiveArray<Date32Type>>().unwrap();
    let date_filter = get_timestamp(&NaiveDate::from_ymd(2019,1, 1));
    select_mask = compute::and(
        &select_mask, 
        &compute::no_simd_compare_op_scalar(&date_array, date_filter, |a, b| a >= b).unwrap()
    ).unwrap();

    // count results
    let count = bit_util::count_set_bits(select_mask.data().buffers()[0].data());
    // let mut count = 0;
    // for i in 0..select_mask.len() {
    //     count += select_mask[i] as usize;
    // }
    return  count;
}

fn filter_with_arrow_simd_array(land_registry_records: &RecordBatch) -> usize {
    // filter by price
    let price_array = land_registry_records.column(0).as_any().downcast_ref::<PrimitiveArray<Float32Type>>().unwrap();
    let price_filter_array: PrimitiveArray<Float32Type> = vec![5000000.0; price_array.len()].into();
    let mut select_mask = compute::gt(price_array, &price_filter_array).unwrap();

    // filter by property type
    let property_type_array = land_registry_records.column(1).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
    let property_type_filter: Vec<i8> = (vec![ "D", "S", "T" ]).into_iter().map(|v| find_dictionary_key(&property_type_array, v).unwrap()).collect();
    let property_type_values = PrimitiveArray::<Int8Type>::from(property_type_array.data());
    let mut property_type_mask: BooleanArray = vec![false; property_type_array.len()].into();
    for f in property_type_filter {
        let property_type_filter_array: PrimitiveArray<Int8Type> = vec![f; property_type_array.len()].into();
        let temp_property_mask = compute::eq(&property_type_values, &property_type_filter_array).unwrap();
        property_type_mask = compute::or(&property_type_mask, &temp_property_mask).unwrap();
    }
    select_mask = compute::and(&select_mask, &property_type_mask).unwrap();
    
    // filter by tenure
    let tenure_array = land_registry_records.column(2).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
    let tenure_filter = find_dictionary_key(&tenure_array, "F").unwrap();
    let tenure_values = PrimitiveArray::<Int8Type>::from(tenure_array.data());
    let tenure_filter_array: PrimitiveArray<Int8Type> = vec![tenure_filter; tenure_array.len()].into();
    select_mask = compute::and(
        &select_mask, 
        &compute::eq(&tenure_values, &tenure_filter_array).unwrap()
    ).unwrap();
    
    // filter by date
    let date_array = land_registry_records.column(3).as_any().downcast_ref::<PrimitiveArray<Date32Type>>().unwrap();
    let date_filter = get_timestamp(&NaiveDate::from_ymd(2019,1, 1));
    let date_filter_array: PrimitiveArray<Date32Type> = vec![date_filter; date_array.len()].into();
    select_mask = compute::and(
        &select_mask, 
        &compute::gt_eq(&date_array, &date_filter_array).unwrap()
    ).unwrap();

    // count results
    let count = bit_util::count_set_bits(select_mask.data().buffers()[0].data());
    // let mut count = 0;
    // for i in 0..select_mask.len() {
    //     count += select_mask[i] as usize;
    // }
    return  count;
}

fn filter_with_arrow_simd_scalar(land_registry_records: &RecordBatch) -> usize {
    // filter by price
    let price_array = land_registry_records.column(0).as_any().downcast_ref::<PrimitiveArray<Float32Type>>().unwrap();
    let mut select_mask = compute::gt_scalar(price_array, 5000000.0).unwrap();

    // filter by property type
    let property_type_array = land_registry_records.column(1).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
    let property_type_filter: Vec<i8> = (vec![ "D", "S", "T" ]).into_iter().map(|v| find_dictionary_key(&property_type_array, v).unwrap()).collect();
    let property_type_values = PrimitiveArray::<Int8Type>::from(property_type_array.data());
    let mut property_type_mask: BooleanArray = vec![false; property_type_array.len()].into();
    for f in property_type_filter {
        let temp_property_mask = compute::eq_scalar(&property_type_values, f).unwrap();
        property_type_mask = compute::or(&property_type_mask, &temp_property_mask).unwrap();
    }
    select_mask = compute::and(&select_mask, &property_type_mask).unwrap();
    
    // filter by tenure
    let tenure_array = land_registry_records.column(2).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
    let tenure_filter = find_dictionary_key(&tenure_array, "F").unwrap();
    let tenure_values = PrimitiveArray::<Int8Type>::from(tenure_array.data());
    select_mask = compute::and(
        &select_mask, 
        &compute::eq_scalar(&tenure_values, tenure_filter).unwrap()
    ).unwrap();
    
    // filter by date
    let date_array = land_registry_records.column(3).as_any().downcast_ref::<PrimitiveArray<Date32Type>>().unwrap();
    let date_filter = get_timestamp(&NaiveDate::from_ymd(2019,1, 1));
    select_mask = compute::and(
        &select_mask, 
        &compute::gt_eq_scalar(&date_array, date_filter).unwrap()
    ).unwrap();

    // count results
    let count = bit_util::count_set_bits(select_mask.data().buffers()[0].data());
    // let mut count = 0;
    // for i in 0..select_mask.len() {
    //     count += select_mask[i] as usize;
    // }
    return  count;
}

#[derive(Debug)]
pub struct LandRegistryRecord {
    price: f32,
    date: NaiveDate,
    post_code: String,
    property_type: String,
    is_new: String,
    tenure: String,
    primary_name: String,
    secondary_name: String,
    street: String,
    city: String,
    //category: String,
    record_type: String
}

impl LandRegistryRecord {
    pub fn deserialise(record: &csv::StringRecord) -> LandRegistryRecord {
        let land_registry_record = LandRegistryRecord {
            price: String::from(&record[1]).trim().to_uppercase().parse::<f32>().unwrap(),
            date: NaiveDate::parse_from_str(&record[2].trim_start()[..10], "%Y-%m-%d").unwrap(),
            post_code: String::from(&record[3]).trim().to_uppercase(),
            // property_type: LandRegistryRecord::deserialise_property_type(&record[4]),
            property_type: String::from(&record[4]).trim().to_uppercase(),
            is_new: String::from(&record[5]).trim().to_uppercase(),
            //tenure: LandRegistryRecord::deserialise_tenure(&record[6]),
            tenure: String::from(&record[6]).trim().to_uppercase(),
            primary_name: String::from(&record[7]).trim().to_uppercase(),
            secondary_name: String::from(&record[8]).trim().to_uppercase(),
            street: String::from(&record[9]).trim().to_uppercase(),
            // skip locality field
            city: String::from(&record[11]).trim().to_uppercase(),
            // skip district field
            // skip county field,
            //category: String::from(&record[14]).trim().to_uppercase(),
            record_type: String::from(&record[15]).trim().to_uppercase()
        };
        return land_registry_record;
    }

    // fn deserialise_property_type(value: &str) -> String {
    //     let new_value = match value {
    //         "D" => "detached",
    //         "S" => "semi-detached",
    //         "T" => "terraced",
    //         "F" => "flat",
    //         _ => "other"
    //     };
    //     return String::from(new_value);
    // }

    // fn deserialise_tenure(value: &str) -> String {
    //     let new_value = match value {
    //         "F" => "freehold",
    //         "L" => "leasehold",
    //         _ => "other"
    //     };
    //     return String::from(new_value);
    // }
}

fn load_land_registry_data() -> Vec<LandRegistryRecord> {
    let data_path = "\\\\nas.local\\Download\\pp-monthly-update-new-version.csv";
    // "\\\\nas.local\\Download\\pp-complete.csv"
    println!("Loading land registry data");
    let timer_start = Instant::now();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(data_path)
        .unwrap();
    let property_records: Vec<LandRegistryRecord> = reader.records()
        .map(|record| LandRegistryRecord::deserialise(&record.unwrap()) )
        .collect();
    let timer_end = Instant::now();
    let record_count = property_records.len();
    println!("Loaded {} records in {}ms", record_count, timer_end.duration_since(timer_start).as_millis());
    return  property_records;
    // .into_iter().take(record_count - (record_count % 64)).collect();
}

fn load_arrow_record_batch(land_registry_records: &Vec<LandRegistryRecord>) -> RecordBatch {
    println!("Loading land registry records into arrow arrays");
    let timer_start = Instant::now();
    let price_array: PrimitiveArray<Float32Type> = land_registry_records.into_iter()
        .map(|r| r.price).collect::<Vec<f32>>().into();
    println!("Loaded price array with {} elements", price_array.len());
    
    // let property_type_array: StringArray = land_registry_records.into_iter()
    //     .map(|r| r.property_type.as_str()).collect::<Vec<&str>>().into();
    let mut property_type_iter = land_registry_records.into_iter()
        .map(|r| r.property_type.as_str());
    let property_type_array = build_string_dictionary(&mut property_type_iter);
    println!("Loaded property type array with {} elements", property_type_array.len());

    // let tenure_array: StringArray = land_registry_records.into_iter()
    //     .map(|r| r.tenure.as_str()).collect::<Vec<&str>>().into();
    let mut tenure_iter = land_registry_records.into_iter()
        .map(|r| r.tenure.as_str());
    let tenure_array = build_string_dictionary(&mut tenure_iter);
    println!("Loaded tenure array with {} elements", tenure_array.len());

    let date_array: PrimitiveArray<Date32Type> = land_registry_records.into_iter()
        .map(|r| get_timestamp(&r.date)).collect::<Vec<i32>>().into();
    println!("Loaded date array with {} elements", date_array.len());

    let timer_end = Instant::now();
    println!("Loaded arrow arrays in {}ms", timer_end.duration_since(timer_start).as_millis());

    let schema = Schema::new(vec![
        Field::new("price", DataType::Float32, false),
        Field::new("property_type", DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        ), false),
        Field::new("tenure", DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        ), false),
        Field::new("date", DataType::Date32(DateUnit::Day), false)
    ]);
    
    RecordBatch::try_new(
        Arc::new(schema), 
        vec![Arc::new(price_array), Arc::new(property_type_array), Arc::new(tenure_array), Arc::new(date_array)]
    ).unwrap()
}

fn get_timestamp(date: &NaiveDate) -> i32 {
    let zero_time = NaiveTime::from_hms(0, 0, 0);
    let seconds_in_day = 24 * 60 * 60;
    return (date.and_time(zero_time).timestamp() / seconds_in_day) as i32;
}

use arrow::{util::bit_util, array::{PrimitiveBuilder, StringBuilder, StringDictionaryBuilder}};

fn build_string_dictionary(iter: &mut dyn Iterator<Item = &str>) -> DictionaryArray<Int8Type> {
    //let iter = iter.into_iter();
    let (lower, _) = iter.size_hint();
    let key_builder = PrimitiveBuilder::<Int8Type>::new(lower);
    let value_builder = StringBuilder::new(256);
    let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
    for i in iter {
        builder
            .append(i)
            .expect("Unable to append a value to a dictionary array.");
    }
    builder.finish()
}

fn find_dictionary_key(dict: &DictionaryArray<Int8Type>, value: &str) -> Option<i8>
{
    let dict_values = dict.values();
    let values: &StringArray = dict_values.as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..values.len() {
        if value == values.value(i) {
            return Some(i as i8);
        }
    }
    return None;
}
