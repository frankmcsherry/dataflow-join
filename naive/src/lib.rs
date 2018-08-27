extern crate graph_map;

pub struct GraphMap {
    map: graph_map::GraphMMap,
    reverse: Vec<u32>,
}

impl GraphMap {
    pub fn new(filename: &str) -> Self {

        let map = graph_map::GraphMMap::new(filename);

        let mut reverse = vec![0; map.nodes()];
        for node in 0 .. map.nodes() {
            for &neighbor in map.edges(node) {
                if (neighbor as usize) < node {
                    reverse[node] += 1;
                }
                if (neighbor as usize) == node {
                    // panic!("self-loop");
                }
            }
        }

        GraphMap {
            map: map,
            reverse: reverse,
        }
    }

    #[inline(always)]
    pub fn nodes(&self) -> u32 { self.map.nodes() as u32 }
    #[inline(always)]
    pub fn edges(&self, node: u32) -> &[u32] { self.map.edges(node as usize) }
    #[inline(always)]
    pub fn forward(&self, node: u32) -> &[u32] {
        &self.edges(node)[(self.reverse[node as usize] as usize)..]
    }
}

pub fn intersect_and<F: FnMut(u32)>(aaa: &[u32], mut bbb: &[u32], mut func: F) {

    if aaa.len() > bbb.len() {
        intersect_and(bbb, aaa, func);
    }
    else {
        if aaa.len() < bbb.len() / 16 {
            for &a in aaa.iter() {
                bbb = gallop_ge(bbb, &a);
                if bbb.len() > 0 && bbb[0] == a {
                    func(a)
                }
            }
        }
        else {
            for &a in aaa.iter() {
                while bbb.len() > 0 && bbb[0] < a {
                    bbb = &bbb[1..];
                }
                if bbb.len() > 0 && a == bbb[0] {
                    func(a);
                }
            }
        }
    }
}


#[inline(always)]
pub fn gallop_ge<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
    // if empty slice, or already >= element, return
    if slice.len() > 0 && &slice[0] < value {
        let mut step = 1;
        while step < slice.len() && &slice[step] < value {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && &slice[step] < value {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed < value
    }

    return slice;
}

#[inline(always)]
pub fn gallop_gt<'a, T: Ord>(mut slice: &'a [T], value: &T) -> &'a [T] {
    // if empty slice, or already > element, return
    if slice.len() > 0 && &slice[0] <= value {
        let mut step = 1;
        while step < slice.len() && &slice[step] <= value {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && &slice[step] <= value {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..]; // advance one, as we always stayed <= value
    }

    return slice;
}