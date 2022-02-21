

use warp::reject::Reject;

#[macro_export]
macro_rules! okky {
    ($cell:expr, $item:expr) => {
        match $cell.set($item) {
            Ok(_) => {}
            Err(_) => panic!("oncecell already full"),
        }
    };
}

/// Merge two sorted iterators.
/// The iterator should be sorted.
///
/// Returns `None` if the iterators were not sorted, `Some(vec)` containing the iterators merged
/// otherwise.
pub fn merge<A, B, T, T2, F>(a: A, b: B, mut map: F) -> Option<Vec<T>>
where
    A: IntoIterator<Item = T>,
    B: IntoIterator<Item = T>,
    F: FnMut(&T) -> T2,
    T2: Ord,
{
    let mut a = a.into_iter().peekable();
    let mut b = b.into_iter().peekable();

    let mut res = Vec::with_capacity(a.size_hint().0 + b.size_hint().0);
    let mut prev_item = None;
    macro_rules! push {
        ($iter:expr, $mapped:expr) => {{
            let item = $iter.next().unwrap();
            let mapped = $mapped.unwrap_or_else(|| map(&item));
            match prev_item {
                Some(prev_item) if prev_item > mapped => return None,
                _ => {
                    res.push(item);
                    prev_item = Some(mapped);
                }
            }
        }};
    }

    loop {
        match (a.peek(), b.peek()) {
            (None, None) => break,
            (Some(_), None) => push!(a, None),
            (None, Some(_)) => push!(b, None),
            (Some(a_next), Some(b_next)) => {
                let a_next = map(a_next);
                let b_next = map(b_next);
                if a_next <= b_next {
                    push!(a, Some(a_next));
                } else {
                    push!(b, Some(b_next));
                }
            }
        }
    }

    res.shrink_to_fit();
    Some(res)
}

#[derive(Debug)]
pub struct AnyhowError(pub anyhow::Error);
impl Reject for AnyhowError {}

#[macro_export]
macro_rules! check {
    ($expr:expr) => {
        match $expr {
            Ok(x) => x,
            Err(e) => {
                eprintln!("returning error from web request handler: {:?}", e);
                return Err(warp::reject::custom(AnyhowError(e)));
            }
        }
    };
}
