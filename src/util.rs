#[macro_export]
macro_rules! okky {
    ($cell:expr, $item:expr) => {
        match $cell.set($item) {
            Ok(_) => {}
            Err(_) => panic!("oncecell already full"),
        }
    };
}
