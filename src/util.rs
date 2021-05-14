use std::str::pattern::Pattern;

#[macro_export]
macro_rules! okky {
    ($cell:expr, $item:expr) => {
        match $cell.set($item) {
            Ok(_) => {}
            Err(_) => panic!("oncecell already full"),
        }
    };
}

pub fn split_tuple<'a, P>(s: &'a str, pattern: P) -> Option<(&'a str, &'a str)>
where
    P: Pattern<'a>,
{
    let mut splitted = s.splitn(2, pattern);
    let a = splitted.next()?;
    let b = splitted.next()?;
    Some((a, b))
}
