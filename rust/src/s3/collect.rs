use super::bucket::*;
use crate::common::keypath::*;
use crate::common::records::*;
use crate::*;
use aws_s3::bucket::Bucket;
use regex::Regex;

pub struct ContinuationParser {
    rex: Regex,
}
impl ContinuationParser {
    pub fn new() -> Self {
        Self {
            rex: Regex::new(r"^[fb]:(\d+):(\d+)$").unwrap(),
        }
    }
    fn parse(&self, s: &str) -> Result<(Direction, Position), StoreError> {
        match self.rex.captures(s) {
            None => Err(StoreError::InvalidContinuation(s.to_string())),
            Some(cap) => {
                let direction = match &cap[1] {
                    "f" => Direction::Forwards,
                    "b" => Direction::Backwards,
                    _ => return Err(StoreError::InvalidContinuation(s.to_string())),
                };
                let next_offset = match cap[2].parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => return Err(StoreError::InvalidContinuation(s.to_string())),
                };
                let last_start_offset = match cap[3].parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => return Err(StoreError::InvalidContinuation(s.to_string())),
                };
                Ok((direction, Position::new(next_offset, last_start_offset)))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadStats {
    pub list_operation_count: u64,
    pub read_operation_count: u64,
    pub read_size_total: u64,
    pub continuation_miss_count: u64,
}

#[derive(Clone, Debug)]
pub struct Position {
    pub next_offset: u64,
    pub anchor_start_offset: u64,
}
impl Position {
    pub fn new(next_offset: u64, anchor_start_offset: u64) -> Self {
        Self {
            next_offset,
            anchor_start_offset,
        }
    }
    pub fn get_start_from(&self, root_prefix: &str, keyspace: &str, key: &str) -> String {
        // last_start_offset defaults to 1, so -1 does not need safety checking
        KeyPath::after_offset_prefix(root_prefix, keyspace, key, self.anchor_start_offset - 1)
    }
}

pub struct CollectOutcome {
    /// contains records that were read, or None if the first matching object did not exist
    pub records: Vec<Record>,
    /// contains position, None if iteration end was reached
    pub position: Option<Position>,
    /// marks final object as missing
    pub requires_retry: bool,
}
impl CollectOutcome {
    pub fn finished(records: Vec<Record>) -> Self {
        Self {
            records: records,
            position: None,
            requires_retry: false,
        }
    }
    pub fn progress(
        records: Vec<Record>,
        last_position: &Position,
        anchor_start_offset: u64,
    ) -> Self {
        if records.is_empty() {
            return Self {
                records: records,
                position: Some(last_position.clone()),
                requires_retry: false,
            };
        } else {
            return Self {
                position: Some(Position::new(
                    records.last().unwrap().offset + 1,
                    anchor_start_offset,
                )),
                records: records,
                requires_retry: false,
            };
        }
    }
    pub fn missing(
        records: Vec<Record>,
        last_position: &Position,
        anchor_start_offset: u64,
    ) -> Self {
        if records.is_empty() {
            return Self {
                records: records,
                position: Some(last_position.clone()),
                requires_retry: true,
            };
        } else {
            return Self {
                position: Some(Position::new(
                    records.last().unwrap().offset + 1,
                    anchor_start_offset,
                )),
                records: records,
                requires_retry: true,
            };
        }
    }
    pub fn continuation(&self) -> Option<String> {
        match &self.position {
            None => None,
            Some(p) => Some(format!("{}:{}", p.next_offset, p.anchor_start_offset)),
        }
    }
}

pub fn collect_first_page(
    stats: &mut ReadStats,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    start: &StartPosition,
    max_results: u64,
    key_path_parser: &KeyPathParser,
    direction: &Direction,
) -> Result<CollectOutcome, StoreError> {
    // create record filter with min/max defaults to avoid Option checks
    let record_filter = RecordFilter::from(start, max_results, direction.clone());

    // no continuation for first page, use filter
    let position = match search_start_from(
        stats,
        bucket,
        root_prefix,
        keyspace,
        key,
        data_prefix,
        &record_filter,
        key_path_parser,
    )? {
        // no filter match -> no results
        None => {
            return Ok(CollectOutcome::finished(Vec::new()));
        }
        // filter match -> start from there
        Some(position) => position,
    };

    // iterate and parse until max_results is filled or end of paging is reached
    return collect_records_from_position(
        stats,
        &position,
        bucket,
        root_prefix,
        keyspace,
        key,
        data_prefix,
        &record_filter,
        key_path_parser,
        direction,
    );
}

pub fn collect_next_page(
    stats: &mut ReadStats,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    max_results: u64,
    key_path_parser: &KeyPathParser,
    continuation: &String,
    continuation_parser: &ContinuationParser,
) -> Result<CollectOutcome, StoreError> {
    // create record filter with min/max defaults to avoid Option checks
    let (direction, position) = continuation_parser.parse(continuation)?;
    let mut record_filter =
        RecordFilter::for_offset(position.next_offset, max_results, direction.clone());

    // try to use continuation
    let collect_outcome = collect_records_from_position(
        stats,
        &position,
        bucket,
        root_prefix,
        keyspace,
        key,
        data_prefix,
        &record_filter,
        key_path_parser,
        &direction,
    )?;
    if collect_outcome.requires_retry {
        // failed, fall back to normal filter search using continuation position
        stats.continuation_miss_count += 1;
        record_filter.start_offset = position.next_offset;
    } else {
        // continuation worked, return results
        return Ok(collect_outcome);
    }

    // continuation failed use filter
    let position = match search_start_from(
        stats,
        bucket,
        root_prefix,
        keyspace,
        key,
        data_prefix,
        &record_filter,
        key_path_parser,
    )? {
        // no filter match -> no results
        None => {
            return Ok(CollectOutcome::finished(Vec::new()));
        }
        // filter match -> start from there
        Some(position) => position,
    };

    // iterate and parse until max_results is filled or end of paging is reached
    return collect_records_from_position(
        stats,
        &position,
        bucket,
        root_prefix,
        keyspace,
        key,
        data_prefix,
        &record_filter,
        key_path_parser,
        &direction,
    );
}

pub fn collect_records_from_position(
    stats: &mut ReadStats,
    start_position: &Position,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    record_filter: &RecordFilter,
    key_path_parser: &KeyPathParser,
    direction: &Direction,
) -> Result<CollectOutcome, StoreError> {
    match direction {
        Direction::Forwards => collect_records_forward_from_position(
            stats,
            start_position,
            bucket,
            root_prefix,
            keyspace,
            key,
            data_prefix,
            record_filter,
            key_path_parser,
        ),
        Direction::Backwards => collect_records_backward_from_position(
            stats,
            start_position,
            bucket,
            root_prefix,
            keyspace,
            key,
            data_prefix,
            record_filter,
            key_path_parser,
        ),
    }
}

pub fn collect_records_forward_from_position(
    stats: &mut ReadStats,
    start_position: &Position,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    record_filter: &RecordFilter,
    key_path_parser: &KeyPathParser,
) -> Result<CollectOutcome, StoreError> {
    let mut records: Vec<Record> = Vec::new();
    let mut s3_cont_token: Option<String> = None;
    let mut cur_position = start_position.clone();
    let start_from = cur_position.get_start_from(root_prefix, keyspace, key);
    loop {
        let (list, next_s3_cont_token) = list_page(
            bucket,
            data_prefix,
            Some(start_from.clone()),
            s3_cont_token,
            None,
        )?;
        stats.list_operation_count += 1;
        for object_key in list {
            // read, deserialize, and further filter next object
            let key_path = key_path_parser.parse_or_error(&object_key)?;
            if cur_position.next_offset < key_path.first_offset {
                // concurrent compaction of expected object lead to object missing since last page, return results so far
                return Ok(CollectOutcome::missing(
                    records,
                    &cur_position,
                    key_path.first_offset,
                ));
            }
            let (new_records, read_fully) =
                collect_object(stats, bucket, &object_key, record_filter, &cur_position)?;
            match new_records {
                None => {
                    // concurrent compaction of expected object lead to object missing since last page, return results so far
                    return Ok(CollectOutcome::missing(
                        records,
                        &cur_position,
                        key_path.first_offset,
                    ));
                }
                Some(mut new_records) => {
                    records.append(&mut new_records);
                }
            };

            let anchor = match read_fully {
                false => key_path.first_offset,   // keep anchoring to current offset
                true => key_path.last_offset + 1, // anchor to next object
            };

            if records.len() > 0 && records.last().unwrap().offset == u64::MAX {
                // reached end of offset space
                return Ok(CollectOutcome::finished(records));
            }

            if records.len() as u64 >= record_filter.max_size {
                // max results have been retreived, return full page
                return Ok(CollectOutcome::progress(records, &cur_position, anchor));
            }

            // advance position for next page
            cur_position.next_offset = key_path.last_offset + 1;
            cur_position.anchor_start_offset = anchor;
        }
        if let None = next_s3_cont_token {
            // no more data to find
            return Ok(CollectOutcome::finished(records));
        }
        s3_cont_token = next_s3_cont_token;
    }
}

fn collect_records_backward_from_position(
    stats: &mut ReadStats,
    start_position: &Position,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    record_filter: &RecordFilter,
    key_path_parser: &KeyPathParser,
) -> Result<CollectOutcome, StoreError> {
    let mut records: Vec<Record> = Vec::new();
    let mut cur_position = start_position.clone();

    loop {
        // backwards continuation anchor uses linked start offsets from the key
        let start_from = cur_position.get_start_from(root_prefix, keyspace, key);

        // only need to list the next matching key from anchor
        let (list, _) = list_page(bucket, data_prefix, Some(start_from.clone()), None, Some(1))?;
        stats.list_operation_count += 1;
        if list.len() == 0 {
            // concurrent compaction lead to a bad read and the expected first object is no longer there, return nothing
            return Ok(CollectOutcome::missing(
                Vec::new(),
                &start_position,
                0, // not used
            ));
        }
        let object_key = list.first().unwrap();
        let key_path = key_path_parser.parse_or_error(&object_key)?;

        // read, deserialize, and further filter next object
        if cur_position.next_offset > key_path.last_offset {
            // concurrent compaction of expected object lead to object missing since last page, return results so far
            return Ok(CollectOutcome::missing(
                records,
                &cur_position,
                key_path.prior_start_offset,
            ));
        }

        let (new_records, read_fully) =
            collect_object(stats, bucket, &object_key, record_filter, &cur_position)?;
        match new_records {
            None => {
                // concurrent compaction of expected object lead to object missing since list, return results so far
                return Ok(CollectOutcome::missing(
                    records,
                    &cur_position,
                    key_path.first_offset,
                ));
            }
            Some(mut new_records) => {
                records.append(&mut new_records);
            }
        };

        let anchor = match read_fully {
            false => key_path.first_offset, // keep anchoring to current offset
            true => key_path.prior_start_offset, // anchor to next object
        };

        if records.len() == 0 {
            // nothing was read from the object, hit an unexpected end
            return Ok(CollectOutcome::finished(records));
        }

        if records.len() > 0 && records.last().unwrap().offset == u64::MAX {
            // reached end of offset space
            return Ok(CollectOutcome::finished(records));
        }

        if cur_position.next_offset == 0 {
            // no more data to find
            return Ok(CollectOutcome::finished(records));
        }

        if records.len() as u64 >= record_filter.max_size {
            // max results have been retreived, return full page
            return Ok(CollectOutcome::progress(records, &cur_position, anchor));
        }

        // advance position for next page
        cur_position.next_offset = key_path.first_offset - 1;
        cur_position.anchor_start_offset = anchor;
    }
}

fn collect_object(
    stats: &mut ReadStats,
    bucket: &Bucket,
    object_key: &str,
    record_filter: &RecordFilter,
    position: &Position,
) -> Result<(Option<Vec<Record>>, bool), StoreError> {
    // read, deserialize, and further filter next object
    let mut records: Vec<Record> = Vec::new();
    let contents = match get_object_optional(bucket, object_key.to_string())? {
        None => return Ok((None, false)), // compaction may have invalidated next object
        Some(contents) => contents,
    };
    stats.read_operation_count += 1;
    stats.read_size_total += contents.len() as u64;
    let read_fully =
        deserialize_and_filter_records(&contents, &mut records, record_filter, position.next_offset)?;
    return Ok((Some(records), read_fully));
}

fn search_start_from(
    stats: &mut ReadStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &RecordFilter,
    key_path_parser: &KeyPathParser,
) -> Result<Option<Position>, StoreError> {
    // always check first 1000 results first
    // for many keys it is the only page and it gives us a start offset for the key.
    let (first_page, first_page_cont_token) = list_page(bucket, data_prefix, None, None, None)?;
    stats.list_operation_count += 1;

    if first_page.is_empty() {
        // first page is empty, no position
        return Ok(None);
    } else if let None = first_page_cont_token {
        // first page is only page, return result from it
        return Ok(find_start_from_in_page(
            &first_page,
            &filter,
            key_path_parser,
        ));
    }

    // optimization: check if iteration starts in the first page
    let last_path_in_first_page = key_path_parser.parse_or_error(first_page.last().unwrap())?;
    match filter.direction {
        Direction::Forwards => {
            if last_path_in_first_page.matches(filter) {
                // last key matches, iterations start in the first page
                return Ok(find_start_from_in_page(
                    &first_page,
                    &filter,
                    key_path_parser,
                ));
            }
        }
        Direction::Backwards => {
            if !last_path_in_first_page.matches(filter) {
                // last key does not match, iteration starts in the first page
                return Ok(find_start_from_in_page(
                    &first_page,
                    filter,
                    key_path_parser,
                ));
            }
        }
    }

    // find last path in key to use for a search range
    let last_path = last_path_for_key(stats, bucket, object_prefix, keyspace, key, data_prefix)?;
    let last_path_in_key = match last_path {
        None => return Ok(None),
        Some(v) => key_path_parser.parse_or_error(&v)?,
    };

    // optimization: if backwards iteration and last key matches, start from there
    if let Direction::Backwards = filter.direction {
        if last_path_in_key.matches(filter) {
            return Ok(Some(Position::new(
                filter.start_offset,
                last_path_in_key.first_offset,
            )));
        }
    }

    // result is not in the first page or the last record, search for it between found min and max offsets
    let first_path_in_key = key_path_parser.parse_or_error(first_page.last().unwrap())?;
    return binary_search_start_from(
        stats,
        bucket,
        object_prefix,
        keyspace,
        key,
        data_prefix,
        &filter,
        key_path_parser,
        first_path_in_key.first_offset,
        last_path_in_key.last_offset,
    );
}
fn last_path_for_key(
    stats: &mut ReadStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
) -> Result<Option<String>, StoreError> {
    let watermark_path = Watermark::path(object_prefix, keyspace, key);
    let watermark_contents_opt = get_object_optional(bucket, watermark_path)?;
    let start_from = match watermark_contents_opt {
        Some(v) => Some(Watermark::from(&v)?.start_from(object_prefix, keyspace, key)),
        None => None,
    };
    let mut last: Option<String> = None;
    let mut next_cont_token: Option<String> = None;
    loop {
        let (page, cont) = list_page(
            bucket,
            data_prefix,
            start_from.clone(),
            next_cont_token,
            None,
        )?;
        stats.list_operation_count += 1;
        if !page.is_empty() {
            last = Some(page.last().unwrap().to_string());
        }
        if let Some(_) = cont {
            next_cont_token = cont;
        } else {
            return Ok(last);
        }
    }
}

fn binary_search_start_from(
    stats: &mut ReadStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &RecordFilter,
    key_path_parser: &KeyPathParser,
    start_min: u64,
    start_max: u64,
) -> Result<Option<Position>, StoreError> {
    match filter.direction {
        Direction::Forwards => binary_search_start_from_forwards(
            stats,
            bucket,
            object_prefix,
            keyspace,
            key,
            data_prefix,
            filter,
            key_path_parser,
            start_min,
            start_max,
        ),
        Direction::Backwards => binary_search_start_from_backwards(
            stats,
            bucket,
            object_prefix,
            keyspace,
            key,
            data_prefix,
            filter,
            key_path_parser,
            start_min,
            start_max,
        ),
    }
}

fn binary_search_start_from_forwards(
    stats: &mut ReadStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &RecordFilter,
    key_path_parser: &KeyPathParser,
    start_min: u64,
    start_max: u64,
) -> Result<Option<Position>, StoreError> {
    // first check if result is in the first page of results
    // since it will return 1000 records, this prevents many list_page operations in many cases
    let start_from = KeyPath::after_offset_prefix(object_prefix, keyspace, key, start_min);
    let (page_list, _) = list_page(bucket, data_prefix, Some(start_from), None, None)?;
    stats.list_operation_count += 1;
    if page_list.is_empty() {
        return Ok(None);
    } else {
        let last_object_in_cur_page = key_path_parser.parse_or_error(&page_list.last().unwrap())?;
        if last_object_in_cur_page.matches(filter) {
            // first page contains the record, return it
            return Ok(find_start_from_in_page(
                &page_list,
                &filter,
                key_path_parser,
            ));
        }
    }

    // perform a normal binary search, with some optimizations since a single request always returns 1000 records
    let mut min = start_min;
    let mut max = start_max;
    loop {
        let next_check = ((min as u128 + max as u128) / 2) as u64;
        let start_from = KeyPath::after_offset_prefix(object_prefix, keyspace, key, next_check);
        let (page_list, s3_cont_token) =
            list_page(bucket, data_prefix, Some(start_from), None, None)?;
        stats.list_operation_count += 1;
        if page_list.is_empty() {
            // no results in this page, result is before this page
            max = next_check;
        } else {
            let first_object_in_cur_page =
                key_path_parser.parse_or_error(&page_list.first().unwrap())?;
            let last_object_in_cur_page =
                key_path_parser.parse_or_error(&page_list.last().unwrap())?;
            if first_object_in_cur_page.matches(filter) {
                // result is before this page
                max = next_check;
            } else if !last_object_in_cur_page.matches(filter) {
                // result is after this page
                if let None = s3_cont_token {
                    // result is after this page and there are no more results
                    return Ok(None);
                } else {
                    // result is after this page and there are more results
                    min = last_object_in_cur_page.last_offset;
                }
            } else {
                // result is in this page
                return Ok(find_start_from_in_page(
                    &page_list,
                    &filter,
                    key_path_parser,
                ));
            }
        }
    }
}

fn binary_search_start_from_backwards(
    stats: &mut ReadStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &RecordFilter,
    key_path_parser: &KeyPathParser,
    start_min: u64,
    start_max: u64,
) -> Result<Option<Position>, StoreError> {
    // first check if result is in the first page of results
    // since it will return 1000 records, this prevents many list_page operations in many cases
    let start_from = KeyPath::after_offset_prefix(object_prefix, keyspace, key, start_min);
    let (page_list, _) = list_page(bucket, data_prefix, Some(start_from), None, None)?;
    stats.list_operation_count += 1;
    if page_list.is_empty() {
        return Ok(None);
    } else {
        let last_object_in_cur_page = key_path_parser.parse_or_error(&page_list.last().unwrap())?;
        if !last_object_in_cur_page.matches(filter) {
            // first page contains the record, return it
            return Ok(find_start_from_in_page(
                &page_list,
                &filter,
                key_path_parser,
            ));
        }
    }

    // perform a normal binary search, with some optimizations since a single request always returns 1000 records
    let mut min = start_min;
    let mut max = start_max;
    loop {
        let next_check = ((min as u128 + max as u128) / 2) as u64;
        let start_from = KeyPath::after_offset_prefix(object_prefix, keyspace, key, next_check);
        let (page_list, s3_cont_token) =
            list_page(bucket, data_prefix, Some(start_from), None, None)?;
        stats.list_operation_count += 1;
        if page_list.is_empty() {
            // no results in this page, result is before this page
            max = next_check;
        } else {
            let first_object_in_cur_page =
                key_path_parser.parse_or_error(&page_list.first().unwrap())?;
            let last_object_in_cur_page =
                key_path_parser.parse_or_error(&page_list.last().unwrap())?;
            if last_object_in_cur_page.matches(filter) {
                if let None = s3_cont_token {
                    // result is after this page and there are no more results
                    return Ok(find_start_from_in_page(
                        &page_list,
                        &filter,
                        key_path_parser,
                    ));
                } else {
                    // result is after this page and there are more results
                    min = next_check;
                }
            } else if first_object_in_cur_page.matches(filter) {
                // result is in this page
                return Ok(find_start_from_in_page(
                    &page_list,
                    &filter,
                    key_path_parser,
                ));
            } else {
                // result is before this page
                max = first_object_in_cur_page.first_offset;
            }
        }
    }
}

fn find_start_from_in_page(
    page_list: &Vec<String>,
    filter: &RecordFilter,
    key_path_parser: &KeyPathParser,
) -> Option<Position> {
    match &filter.direction {
        Direction::Forwards => {
            for path in page_list.iter() {
                match key_path_parser.parse(path) {
                    Some(kp) => {
                        if kp.matches(filter) {
                            return Some(Position::new(kp.first_offset, kp.first_offset));
                        }
                    }
                    None => {}
                }
            }
        }
        Direction::Backwards => {
            for path in page_list.iter().rev() {
                match key_path_parser.parse(path) {
                    Some(kp) => {
                        if kp.matches(filter) {
                            return Some(Position::new(kp.last_offset, kp.first_offset));
                        }
                    }
                    None => {}
                }
            }
        }
    }
    return None;
}
