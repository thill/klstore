use super::bucket::*;
use crate::common::items::*;
use crate::common::keypath::*;
use crate::*;
use aws_s3::bucket::Bucket;
use regex::Regex;

pub struct ContinuationParser {
    rex: Regex,
}
impl ContinuationParser {
    pub fn new() -> Self {
        Self {
            rex: Regex::new(r"^(\d+):(\d+)$").unwrap(),
        }
    }
    fn parse(&self, s: &str) -> Result<Position, StoreError> {
        match self.rex.captures(s) {
            None => Err(StoreError::InvalidContinuation(s.to_string())),
            Some(cap) => {
                let next_offset = match cap[1].parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => return Err(StoreError::InvalidContinuation(s.to_string())),
                };
                let last_start_offset = match cap[2].parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => return Err(StoreError::InvalidContinuation(s.to_string())),
                };
                Ok(Position::new(next_offset, last_start_offset))
            }
        }
    }
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
    /// contains items that were read, or None if the first matching object did not exist
    pub items: Vec<Item>,
    /// contains position, None if iteration end was reached
    pub position: Option<Position>,
    /// marks final object as missing
    pub requires_retry: bool,
}
impl CollectOutcome {
    pub fn finished(items: Vec<Item>) -> Self {
        Self {
            items: items,
            position: None,
            requires_retry: false,
        }
    }
    pub fn progress(items: Vec<Item>, last_position: &Position, anchor_start_offset: u64) -> Self {
        if items.is_empty() {
            return Self {
                items: items,
                position: Some(last_position.clone()),
                requires_retry: false,
            };
        } else {
            return Self {
                position: Some(Position::new(
                    items.last().unwrap().offset + 1,
                    anchor_start_offset,
                )),
                items: items,
                requires_retry: false,
            };
        }
    }
    pub fn missing(items: Vec<Item>, last_position: &Position, anchor_start_offset: u64) -> Self {
        if items.is_empty() {
            return Self {
                items: items,
                position: Some(last_position.clone()),
                requires_retry: true,
            };
        } else {
            return Self {
                position: Some(Position::new(
                    items.last().unwrap().offset + 1,
                    anchor_start_offset,
                )),
                items: items,
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

pub fn collect_next_page(
    stats: &mut ListStats,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &Option<ItemFilter>,
    max_results: u64,
    key_path_parser: &KeyPathParser,
    order: &IterationOrder,
    continuation: &Option<String>,
    continuation_parser: &ContinuationParser,
) -> Result<CollectOutcome, StoreError> {
    // create item filter with min/max defaults to avoid Option checks
    let mut defaulted_item_filter = DefaultedItemFilter::from(filter, max_results, order.clone());

    // if continuation is defined, try to use it
    if let Some(continuation) = continuation {
        let position = continuation_parser.parse(&continuation)?;
        let collect_outcome = collect_items_from_position(
            stats,
            &position,
            bucket,
            root_prefix,
            keyspace,
            key,
            data_prefix,
            &defaulted_item_filter,
            key_path_parser,
            order,
        )?;
        if !collect_outcome.requires_retry {
            // continuation worked, return results
            return Ok(collect_outcome);
        }
        // otherwise, fall back to normal filter search using continuation position
        stats.continuation_miss_count += 1;
        defaulted_item_filter.start_offset = position.next_offset
    }

    // continuation was not defined or failed, use filter
    let position = match search_start_from(
        stats,
        bucket,
        root_prefix,
        keyspace,
        key,
        data_prefix,
        &defaulted_item_filter,
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
    return collect_items_from_position(
        stats,
        &position,
        bucket,
        root_prefix,
        keyspace,
        key,
        data_prefix,
        &defaulted_item_filter,
        key_path_parser,
        order,
    );
}

pub fn collect_items_from_position(
    stats: &mut ListStats,
    start_position: &Position,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    item_filter: &DefaultedItemFilter,
    key_path_parser: &KeyPathParser,
    order: &IterationOrder,
) -> Result<CollectOutcome, StoreError> {
    match order {
        IterationOrder::Forwards => collect_items_forward_from_position(
            stats,
            start_position,
            bucket,
            root_prefix,
            keyspace,
            key,
            data_prefix,
            item_filter,
            key_path_parser,
        ),
        IterationOrder::Backwards => collect_items_backward_from_position(
            stats,
            start_position,
            bucket,
            root_prefix,
            keyspace,
            key,
            data_prefix,
            item_filter,
            key_path_parser,
        ),
    }
}

pub fn collect_items_forward_from_position(
    stats: &mut ListStats,
    start_position: &Position,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    item_filter: &DefaultedItemFilter,
    key_path_parser: &KeyPathParser,
) -> Result<CollectOutcome, StoreError> {
    let mut items: Vec<Item> = Vec::new();
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
                    items,
                    &cur_position,
                    key_path.first_offset,
                ));
            }
            let (new_items, read_fully) =
                collect_object(stats, bucket, &object_key, item_filter, &cur_position)?;
            match new_items {
                None => {
                    // concurrent compaction of expected object lead to object missing since last page, return results so far
                    return Ok(CollectOutcome::missing(
                        items,
                        &cur_position,
                        key_path.first_offset,
                    ));
                }
                Some(mut new_items) => {
                    items.append(&mut new_items);
                }
            };

            let anchor = match read_fully {
                false => key_path.first_offset,   // keep anchoring to current offset
                true => key_path.last_offset + 1, // anchor to next object
            };

            if items.len() > 0 && items.last().unwrap().offset == u64::MAX {
                // reached end of offset space
                return Ok(CollectOutcome::finished(items));
            }

            if items.len() as u64 >= item_filter.max_size {
                // max results have been retreived, return full page
                return Ok(CollectOutcome::progress(items, &cur_position, anchor));
            }

            // advance position for next page
            cur_position.next_offset = key_path.last_offset + 1;
            cur_position.anchor_start_offset = anchor;
        }
        if let None = next_s3_cont_token {
            // no more data to find
            return Ok(CollectOutcome::finished(items));
        }
        s3_cont_token = next_s3_cont_token;
    }
}

fn collect_items_backward_from_position(
    stats: &mut ListStats,
    start_position: &Position,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    item_filter: &DefaultedItemFilter,
    key_path_parser: &KeyPathParser,
) -> Result<CollectOutcome, StoreError> {
    let mut items: Vec<Item> = Vec::new();
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
                items,
                &cur_position,
                key_path.prior_start_offset,
            ));
        }

        let (new_items, read_fully) =
            collect_object(stats, bucket, &object_key, item_filter, &cur_position)?;
        match new_items {
            None => {
                // concurrent compaction of expected object lead to object missing since list, return results so far
                return Ok(CollectOutcome::missing(
                    items,
                    &cur_position,
                    key_path.first_offset,
                ));
            }
            Some(mut new_items) => {
                items.append(&mut new_items);
            }
        };

        let anchor = match read_fully {
            false => key_path.first_offset, // keep anchoring to current offset
            true => key_path.prior_start_offset, // anchor to next object
        };

        if items.len() == 0 {
            // nothing was read from the object, hit an unexpected end
            return Ok(CollectOutcome::finished(items));
        }

        if items.len() > 0 && items.last().unwrap().offset == u64::MAX {
            // reached end of offset space
            return Ok(CollectOutcome::finished(items));
        }

        if cur_position.next_offset == 0 {
            // no more data to find
            return Ok(CollectOutcome::finished(items));
        }

        if items.len() as u64 >= item_filter.max_size {
            // max results have been retreived, return full page
            return Ok(CollectOutcome::progress(items, &cur_position, anchor));
        }

        // advance position for next page
        cur_position.next_offset = key_path.first_offset - 1;
        cur_position.anchor_start_offset = anchor;
    }
}

fn collect_object(
    stats: &mut ListStats,
    bucket: &Bucket,
    object_key: &str,
    item_filter: &DefaultedItemFilter,
    position: &Position,
) -> Result<(Option<Vec<Item>>, bool), StoreError> {
    // read, deserialize, and further filter next object
    let mut items: Vec<Item> = Vec::new();
    let contents = match get_object_optional(bucket, object_key.to_string())? {
        None => return Ok((None, false)), // compaction may have invalidated next object
        Some(contents) => contents,
    };
    stats.read_operation_count += 1;
    stats.read_size_total += contents.len() as u64;
    let read_fully =
        deserialize_and_filter_items(&contents, &mut items, item_filter, position.next_offset)?;
    return Ok((Some(items), read_fully));
}

fn search_start_from(
    stats: &mut ListStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &DefaultedItemFilter,
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
    match filter.order {
        IterationOrder::Forwards => {
            if last_path_in_first_page.matches(filter) {
                // last key matches, iterations start in the first page
                return Ok(find_start_from_in_page(
                    &first_page,
                    &filter,
                    key_path_parser,
                ));
            }
        }
        IterationOrder::Backwards => {
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
    if let IterationOrder::Backwards = filter.order {
        if last_path_in_key.matches(filter) {
            return Ok(Some(Position::new(
                filter.start_offset,
                last_path_in_key.first_offset,
            )));
        }
    }

    // result is not in the first page or the last element, search for it between found min and max offsets
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
    stats: &mut ListStats,
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
    stats: &mut ListStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &DefaultedItemFilter,
    key_path_parser: &KeyPathParser,
    start_min: u64,
    start_max: u64,
) -> Result<Option<Position>, StoreError> {
    match filter.order {
        IterationOrder::Forwards => binary_search_start_from_forwards(
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
        IterationOrder::Backwards => binary_search_start_from_backwards(
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
    stats: &mut ListStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &DefaultedItemFilter,
    key_path_parser: &KeyPathParser,
    start_min: u64,
    start_max: u64,
) -> Result<Option<Position>, StoreError> {
    // first check if result is in the first page of results
    // since it will return 1000 elements, this prevents many list_page operations in many cases
    let start_from = KeyPath::after_offset_prefix(object_prefix, keyspace, key, start_min);
    let (page_list, _) = list_page(bucket, data_prefix, Some(start_from), None, None)?;
    stats.list_operation_count += 1;
    if page_list.is_empty() {
        return Ok(None);
    } else {
        let last_object_in_cur_page = key_path_parser.parse_or_error(&page_list.last().unwrap())?;
        if last_object_in_cur_page.matches(filter) {
            // first page contains the item, return it
            return Ok(find_start_from_in_page(
                &page_list,
                &filter,
                key_path_parser,
            ));
        }
    }

    // perform a normal binary search, with some optimizations since a single request always returns 1000 elements
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
    stats: &mut ListStats,
    bucket: &Bucket,
    object_prefix: &str,
    keyspace: &str,
    key: &str,
    data_prefix: &str,
    filter: &DefaultedItemFilter,
    key_path_parser: &KeyPathParser,
    start_min: u64,
    start_max: u64,
) -> Result<Option<Position>, StoreError> {
    // first check if result is in the first page of results
    // since it will return 1000 elements, this prevents many list_page operations in many cases
    let start_from = KeyPath::after_offset_prefix(object_prefix, keyspace, key, start_min);
    let (page_list, _) = list_page(bucket, data_prefix, Some(start_from), None, None)?;
    stats.list_operation_count += 1;
    if page_list.is_empty() {
        return Ok(None);
    } else {
        let last_object_in_cur_page = key_path_parser.parse_or_error(&page_list.last().unwrap())?;
        if !last_object_in_cur_page.matches(filter) {
            // first page contains the item, return it
            return Ok(find_start_from_in_page(
                &page_list,
                &filter,
                key_path_parser,
            ));
        }
    }

    // perform a normal binary search, with some optimizations since a single request always returns 1000 elements
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
    filter: &DefaultedItemFilter,
    key_path_parser: &KeyPathParser,
) -> Option<Position> {
    match &filter.order {
        IterationOrder::Forwards => {
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
        IterationOrder::Backwards => {
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

// fn exponential_search_start_from(
//     stats: &mut ListStats,
//     bucket: &Bucket,
//     object_prefix: &str,
//     keyspace: &str,
//     key: &str,
//     data_prefix: &str,
//     order: &IterationOrder,
//     filter: &DefaultedItemFilter,
//     first_offset_in_key: u64,
//     key_path_parser: &KeyPathParser,
//     start_min: u64,
// ) -> Result<Option<Position>, StoreError> {
//     // First, searching exponentially and determining which powers-of-two the result is between.
//     // In most cases, this will require fewer S3::list operations than a full binary search through u64 offsets.
//     let (min, max) = find_range_exponentially(
//         stats,
//         bucket,
//         object_prefix,
//         keyspace,
//         key,
//         data_prefix,
//         order,
//         filter,
//         first_offset_in_key,
//         key_path_parser,
//         start_min,
//     )?;
//     // Perform a binary search between the powers-of-two from step 2, considering 1000 elements at a time
//     return binary_search_start_from(
//         stats,
//         bucket,
//         object_prefix,
//         keyspace,
//         key,
//         data_prefix,
//         filter,
//         key_path_parser,
//         min,
//         max,
//     );
// }

// fn find_range_exponentially(
//     stats: &mut ListStats,
//     bucket: &Bucket,
//     object_prefix: &str,
//     keyspace: &str,
//     key: &str,
//     data_prefix: &str,
//     order: &IterationOrder,
//     filter: &DefaultedItemFilter,
//     first_offset_in_key: u64,
//     key_path_parser: &KeyPathParser,
//     start_min: u64,
// ) -> Result<(u64, u64), StoreError> {
//     let mut min = start_min;
//     let mut next_pow2: u64 = 512;
//     let mut next_check = start_min;
//     loop {
//         let start_from = KeyPath::after_offset_prefix(object_prefix, keyspace, key, next_check);
//         let (page_list, _) = list_page(bucket, data_prefix, Some(start_from), None, None)?;
//         stats.list_operation_count += 1;
//         if page_list.is_empty() {
//             // no more data, set max to this check and break
//             return Ok((min, next_check));
//         }
//         let last_object_in_cur_page = key_path_parser.parse_or_error(&page_list.last().unwrap())?;
//         let last_object_matches = last_object_in_cur_page.matches(&filter);
//         let in_page = match order {
//             IterationOrder::Forwards => last_object_matches,
//             IterationOrder::Backwards => !last_object_matches,
//         };
//         if in_page {
//             // result is on or before the last element in this page, set max and break
//             return Ok((min, last_object_in_cur_page.last_offset));
//         } else {
//             // result is after, set new min, continue paging
//             min = last_object_in_cur_page.last_offset;
//             next_check = last_object_in_cur_page.last_offset;
//             // find next power-of-two that is after the current page's last offset
//             while first_offset_in_key + next_pow2 < min {
//                 if u64::MAX - next_pow2 < last_object_in_cur_page.last_offset {
//                     // do not surpass u64::MAX
//                     next_check = u64::MAX;
//                     break;
//                 }
//                 next_pow2 *= 2;
//                 next_check = first_offset_in_key + next_pow2;
//             }
//             if next_check == u64::MAX {
//                 // we've reached end of address space, max is u64::MAX, break
//                 return Ok((min, u64::MAX));
//             }
//         }
//     }
// }
