use std::{collections::BTreeMap, ops::Range};

use crate::{ConnectionId, ResetToken, frame::NewConnectionId};

/// DataType stored in CidQueue buffer
type CidData = (ConnectionId, Option<ResetToken>);

/// Sliding window of active Connection IDs
///
/// May contain gaps due to packet loss or reordering
#[derive(Debug)]
pub(crate) struct CidQueue {
    /// The known CIDs by sequence number, the active one first: only a CID the peer issued
    /// takes memory, so a large active connection ID limit costs nothing until used
    cids: BTreeMap<u64, CidData>,
    /// The active connection ID limit: how far past the active CID a sequence number may be
    limit: usize,
}

impl CidQueue {
    /// A queue holding up to `limit` connection IDs, the active one first
    pub(crate) fn new(cid: ConnectionId, limit: usize) -> Self {
        Self {
            cids: BTreeMap::from([(0, (cid, None))]),
            limit,
        }
    }

    /// Handle a `NEW_CONNECTION_ID` frame
    ///
    /// Returns a non-empty range of retired sequence numbers and the reset token of the new active
    /// CID iff any CIDs were retired.
    pub(crate) fn insert(
        &mut self,
        cid: NewConnectionId,
    ) -> Result<Option<(Range<u64>, ResetToken)>, InsertError> {
        let orig_offset = self.offset();
        // Position of new CID wrt. the current active CID
        let index = match cid.sequence.checked_sub(orig_offset) {
            None => return Err(InsertError::Retired),
            Some(x) => x,
        };

        let retired_count = cid.retire_prior_to.saturating_sub(orig_offset);
        if index >= self.limit as u64 + retired_count {
            return Err(InsertError::ExceedsLimit);
        }
        // Discard retired CIDs, if any
        if retired_count != 0 {
            self.cids = self.cids.split_off(&cid.retire_prior_to);
        }

        // Record the new CID
        self.cids
            .insert(cid.sequence, (cid.id, Some(cid.reset_token)));

        if retired_count == 0 {
            return Ok(None);
        }

        // The active CID was retired. Find the first known CID with sequence number of at least
        // retire_prior_to, and inform the caller that all prior CIDs have been retired, and of
        // the new CID's reset token.
        let (_, token) = self
            .iter()
            .next()
            .expect("it is impossible to retire a CID without supplying a new one")
            .1;
        // We don't immediately retire CIDs in the range (orig_offset +
        // self.limit)..self.offset(). These are CIDs that we haven't yet received from a
        // NEW_CONNECTION_ID frame, since having previously received them would violate the
        // connection ID limit we specified. If we do receive a such a frame
        // in the future, e.g. due to reordering, we'll retire it then. This ensures we can't be
        // made to buffer an arbitrarily large number of RETIRE_CONNECTION_ID frames.
        Ok(Some((
            orig_offset..self.offset().min(orig_offset + self.limit as u64),
            token.expect("non-initial CID missing reset token"),
        )))
    }

    /// Switch to next active CID if possible, return
    /// 1) the corresponding ResetToken and 2) a non-empty range preceding it to retire
    pub(crate) fn next(&mut self) -> Option<(ResetToken, Range<u64>)> {
        let (i, cid_data) = self.iter().nth(1)?;
        let orig_offset = self.offset();
        self.cids.remove(&orig_offset);
        Some((cid_data.1.unwrap(), orig_offset..orig_offset + i as u64))
    }

    /// Iterate CIDs in CidQueue, including the active CID, each with its distance from it
    fn iter(&self) -> impl Iterator<Item = (usize, CidData)> + '_ {
        let offset = self.offset();
        self.cids
            .iter()
            .map(move |(&seq, &cid_data)| ((seq - offset) as usize, cid_data))
    }

    /// Replace the initial CID
    pub(crate) fn update_initial_cid(&mut self, cid: ConnectionId) {
        debug_assert_eq!(self.offset(), 0);
        self.cids.insert(0, (cid, None));
    }

    /// Return active remote CID itself
    pub(crate) fn active(&self) -> ConnectionId {
        self.cids.values().next().unwrap().0
    }

    /// Return the sequence number of active remote CID
    pub(crate) fn active_seq(&self) -> u64 {
        self.offset()
    }

    /// Sequence number of the active CID, the smallest in `cids`
    fn offset(&self) -> u64 {
        *self.cids.keys().next().unwrap()
    }

    /// The default active connection ID limit
    pub(crate) const LEN: usize = 5;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum InsertError {
    /// CID was already retired
    Retired,
    /// Sequence number violates the leading edge of the window
    ExceedsLimit,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(sequence: u64, retire_prior_to: u64) -> NewConnectionId {
        NewConnectionId {
            sequence,
            id: ConnectionId::new(&[0xAB; 8]),
            reset_token: ResetToken::from([0xCD; crate::RESET_TOKEN_SIZE]),
            retire_prior_to,
        }
    }

    fn initial_cid() -> ConnectionId {
        ConnectionId::new(&[0xFF; 8])
    }

    #[test]
    fn next_dense() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        assert!(q.next().is_none());
        assert!(q.next().is_none());

        for i in 1..CidQueue::LEN as u64 {
            q.insert(cid(i, 0)).unwrap();
        }
        for i in 1..CidQueue::LEN as u64 {
            let (_, retire) = q.next().unwrap();
            assert_eq!(q.active_seq(), i);
            assert_eq!(retire.end - retire.start, 1);
        }
        assert!(q.next().is_none());
    }
    #[test]
    fn next_sparse() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        let seqs = (1..CidQueue::LEN as u64).filter(|x| x % 2 == 0);
        for i in seqs.clone() {
            q.insert(cid(i, 0)).unwrap();
        }
        for i in seqs {
            let (_, retire) = q.next().unwrap();
            dbg!(&retire);
            assert_eq!(q.active_seq(), i);
            assert_eq!(retire, (q.active_seq().saturating_sub(2))..q.active_seq());
        }
        assert!(q.next().is_none());
    }

    #[test]
    fn wrap() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);

        for i in 1..CidQueue::LEN as u64 {
            q.insert(cid(i, 0)).unwrap();
        }
        for _ in 1..(CidQueue::LEN as u64 - 1) {
            q.next().unwrap();
        }
        for i in CidQueue::LEN as u64..(CidQueue::LEN as u64 + 3) {
            q.insert(cid(i, 0)).unwrap();
        }
        for i in (CidQueue::LEN as u64 - 1)..(CidQueue::LEN as u64 + 3) {
            q.next().unwrap();
            assert_eq!(q.active_seq(), i);
        }
        assert!(q.next().is_none());
    }

    #[test]
    fn retire_dense() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);

        for i in 1..CidQueue::LEN as u64 {
            q.insert(cid(i, 0)).unwrap();
        }
        assert_eq!(q.active_seq(), 0);

        assert_eq!(q.insert(cid(4, 2)).unwrap().unwrap().0, 0..2);
        assert_eq!(q.active_seq(), 2);
        assert_eq!(q.insert(cid(4, 2)), Ok(None));

        for i in 2..(CidQueue::LEN as u64 - 1) {
            let _ = q.next().unwrap();
            assert_eq!(q.active_seq(), i + 1);
            assert_eq!(q.insert(cid(i + 1, i + 1)), Ok(None));
        }

        assert!(q.next().is_none());
    }

    #[test]
    fn retire_sparse() {
        // Retiring CID 0 when CID 1 is not known should retire CID 1 as we move to CID 2
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        q.insert(cid(2, 0)).unwrap();
        assert_eq!(q.insert(cid(3, 1)).unwrap().unwrap().0, 0..2,);
        assert_eq!(q.active_seq(), 2);
    }

    #[test]
    fn retire_many() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        q.insert(cid(2, 0)).unwrap();
        assert_eq!(
            q.insert(cid(1_000_000, 1_000_000)).unwrap().unwrap().0,
            0..CidQueue::LEN as u64,
        );
        assert_eq!(q.active_seq(), 1_000_000);
    }

    #[test]
    fn insert_limit() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        assert_eq!(q.insert(cid(CidQueue::LEN as u64 - 1, 0)), Ok(None));
        assert_eq!(
            q.insert(cid(CidQueue::LEN as u64, 0)),
            Err(InsertError::ExceedsLimit)
        );
    }

    #[test]
    fn insert_duplicate() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        q.insert(cid(0, 0)).unwrap();
        q.insert(cid(0, 0)).unwrap();
    }

    #[test]
    fn insert_retired() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        assert_eq!(
            q.insert(cid(0, 0)),
            Ok(None),
            "reinserting active CID succeeds"
        );
        assert!(q.next().is_none(), "active CID isn't requeued");
        q.insert(cid(1, 0)).unwrap();
        q.next().unwrap();
        assert_eq!(
            q.insert(cid(0, 0)),
            Err(InsertError::Retired),
            "previous active CID is already retired"
        );
    }

    #[test]
    fn retire_then_insert_next() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        for i in 1..CidQueue::LEN as u64 {
            q.insert(cid(i, 0)).unwrap();
        }
        q.next().unwrap();
        q.insert(cid(CidQueue::LEN as u64, 0)).unwrap();
        assert_eq!(
            q.insert(cid(CidQueue::LEN as u64 + 1, 0)),
            Err(InsertError::ExceedsLimit)
        );
    }

    #[test]
    fn always_valid() {
        let mut q = CidQueue::new(initial_cid(), CidQueue::LEN);
        assert!(q.next().is_none());
        assert_eq!(q.active(), initial_cid());
        assert_eq!(q.active_seq(), 0);
    }
}
