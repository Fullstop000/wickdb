use crate::mem::arena::BlockArena;
use crate::mem::inlineskiplist::InlineSkipList;
use crate::BytewiseComparator;

fn new_test_skl() -> InlineSkipList<BytewiseComparator, BlockArena> {
    InlineSkipList::new(BytewiseComparator::default(), BlockArena::default())
}

#[test]
fn test_empty() {
    let key = b"aaa";
    let skl = new_test_skl();
    for less in &[false, true] {
        for allow_equal in &[false, true] {
            let (node, found) = skl.find_near(key, *less, *allow_equal);
            assert!(node.is_null());
            assert!(!found);
        }
    }
}
