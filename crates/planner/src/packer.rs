/*
 * Bin-packing algorithm for grouping files.
 *
 * Adapted from iceberg-compaction/core/src/file_selection/packer.rs
 * Uses First-Fit Decreasing algorithm for optimal file grouping.
 */

/// A bin-packer that groups items into bins of a target size.
pub struct ListPacker {
    target_weight: u64,
    lookback: usize,
}

impl ListPacker {
    /// Creates a new packer with the given target weight per bin.
    pub fn new(target_weight: u64) -> Self {
        Self {
            target_weight,
            lookback: 1,
        }
    }

    /// Creates a new packer with custom lookback.
    #[allow(dead_code)]
    pub fn with_lookback(target_weight: u64, lookback: usize) -> Self {
        Self {
            target_weight,
            lookback,
        }
    }

    /// Packs items into bins using First-Fit Decreasing.
    ///
    /// Items are sorted by weight descending, then placed into the first bin
    /// that can accommodate them (within lookback window).
    pub fn pack<T, F>(&self, items: impl IntoIterator<Item = T>, weight_fn: F) -> Vec<Vec<T>>
    where
        F: Fn(&T) -> u64,
    {
        // Collect and sort by weight descending
        let mut items: Vec<(T, u64)> = items
            .into_iter()
            .map(|item| {
                let weight = weight_fn(&item);
                (item, weight)
            })
            .collect();

        items.sort_by(|a, b| b.1.cmp(&a.1));

        let mut bins: Vec<(Vec<T>, u64)> = Vec::new();

        for (item, weight) in items {
            // Find a bin that can fit this item (within lookback)
            let start_idx = bins.len().saturating_sub(self.lookback);

            // Find the index of a bin that can fit this item
            let fit_idx = bins[start_idx..]
                .iter()
                .position(|bin| bin.1 + weight <= self.target_weight)
                .map(|i| i + start_idx);

            match fit_idx {
                Some(idx) => {
                    bins[idx].0.push(item);
                    bins[idx].1 += weight;
                }
                None => {
                    // Start a new bin
                    bins.push((vec![item], weight));
                }
            }
        }

        bins.into_iter().map(|(items, _)| items).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packer_basic() {
        let packer = ListPacker::new(100);
        let items = vec![30u64, 40, 50, 20, 10];
        let bins = packer.pack(items, |&x| x);

        // Should pack into bins without exceeding 100
        for bin in &bins {
            let total: u64 = bin.iter().sum();
            assert!(total <= 100);
        }
    }

    #[test]
    fn test_packer_single_large_item() {
        let packer = ListPacker::new(100);
        let items = vec![150u64]; // Larger than target
        let bins = packer.pack(items, |&x| x);

        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0], vec![150]);
    }
}
