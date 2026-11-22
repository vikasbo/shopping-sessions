//! Linked IDs: union-find over every ID in `links/`, then each person is named by
//! the byte-wise smallest of their IDs.

use ahash::RandomState;
use std::collections::HashMap;

/// Maps a user ID to its person's name. IDs never seen in a link are their own person.
pub struct Persons<'a> {
    index: HashMap<&'a [u8], u32, RandomState>,
    name: Vec<&'a [u8]>,
}

impl<'a> Persons<'a> {
    /// `'a` is a lifetime: the returned slices point into the memory-mapped files and
    /// the compiler guarantees they cannot outlive them (Java's GC makes this implicit).
    #[inline]
    pub fn name_of(&self, user: &'a [u8]) -> &'a [u8] {
        match self.index.get(user) {
            Some(&i) => self.name[i as usize],
            None => user,
        }
    }

    pub fn build(pairs: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> Self {
        let mut index: HashMap<&'a [u8], u32, RandomState> = HashMap::with_hasher(RandomState::new());
        let mut ids: Vec<&'a [u8]> = Vec::new();
        let mut parent: Vec<u32> = Vec::new();

        // `|...|` closures cannot borrow `index` and `ids` mutably while another closure holds them,
        // so interning is a plain inner function that receives them as arguments.
        fn intern<'a>(id: &'a [u8], index: &mut HashMap<&'a [u8], u32, RandomState>,
                      ids: &mut Vec<&'a [u8]>, parent: &mut Vec<u32>) -> u32 {
            *index.entry(id).or_insert_with(|| {
                let i = ids.len() as u32;
                ids.push(id);
                parent.push(i);
                i
            })
        }
        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                let p = parent[x as usize];
                parent[x as usize] = parent[p as usize]; // path halving
                x = p;
            }
            x
        }

        for (a, b) in pairs {
            let ia = intern(a, &mut index, &mut ids, &mut parent);
            let ib = intern(b, &mut index, &mut ids, &mut parent);
            let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
            if ra != rb {
                // Keep the smaller ID as the root, so after all unions the root *is* the name.
                if ids[rb as usize] < ids[ra as usize] {
                    parent[ra as usize] = rb;
                } else {
                    parent[rb as usize] = ra;
                }
            }
        }
        let name = (0..ids.len() as u32).map(|i| ids[find(&mut parent, i) as usize]).collect();
        Persons { index, name }
    }
}
