use super::*;
use bitcoin::{absolute::LockTime, transaction::Version, Amount, Sequence, TxIn, TxOut, Witness};

const VALUE: u64 = 100_000;
const TIP: u32 = 100;

fn contract_script() -> ScriptBuf {
    ScriptBuf::from_bytes(vec![0x51, 0x20].into_iter().chain([1; 32]).collect())
}

fn funding(value: u64) -> Transaction {
    Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![input(OutPoint::null())],
        output: vec![TxOut {
            value: Amount::from_sat(value),
            script_pubkey: contract_script(),
        }],
    }
}

fn input(previous_output: OutPoint) -> TxIn {
    TxIn {
        previous_output,
        script_sig: ScriptBuf::new(),
        sequence: Sequence::ZERO,
        witness: Witness::new(),
    }
}

fn spending(funding: &Transaction, index: u32) -> Transaction {
    let mut transaction = funding.clone();
    transaction.input = vec![input(OutPoint::new(funding.compute_txid(), index))];
    transaction.output = vec![TxOut {
        script_pubkey: ScriptBuf::new(),
        value: Amount::from_sat(VALUE - 500),
    }];
    transaction
}

fn observe_history(history: &[(Transaction, i32)]) -> Result<Option<Observation>> {
    classify_history(&contract_script(), VALUE, TIP, history)
}

#[test]
fn dust_does_not_hide_valid_funding() {
    let lockup = funding(VALUE);
    let observation = observe_history(&[(funding(1), 99), (lockup.clone(), 99)])
        .unwrap()
        .unwrap();
    assert_eq!(
        observation.transaction.id,
        lockup.compute_txid().to_string()
    );
    assert_eq!(observation.confirmations, 2);
    assert!(observation.spend.is_none());
}

#[test]
fn dust_without_valid_funding_is_not_an_observation() {
    assert!(observe_history(&[(funding(546), 98)]).unwrap().is_none());
}

#[test]
fn unrelated_outputs_do_not_count_as_funding() {
    let mut unrelated = funding(VALUE);
    unrelated.output[0].script_pubkey = ScriptBuf::new();
    assert!(observe_history(&[(unrelated, 98)]).unwrap().is_none());
}

#[test]
fn ambiguous_valid_outputs_are_rejected() {
    assert!(matches!(
        observe_history(&[(funding(VALUE), 98), (funding(VALUE + 1), 99)]),
        Err(Error::Validation)
    ));
}

#[test]
fn ambiguous_outputs_in_one_transaction_are_rejected() {
    let mut lockup = funding(VALUE);
    lockup.output.push(lockup.output[0].clone());
    assert!(matches!(
        observe_history(&[(lockup, 98)]),
        Err(Error::Validation)
    ));
}

#[test]
fn spend_matches_the_exact_outpoint() {
    let mut lockup = funding(VALUE);
    lockup.output.insert(
        0,
        TxOut {
            value: Amount::from_sat(1),
            script_pubkey: contract_script(),
        },
    );
    let unrelated_spend = spending(&lockup, 0);
    let actual_spend = spending(&lockup, 1);
    let observation = observe_history(&[
        (lockup.clone(), 90),
        (unrelated_spend, 95),
        (actual_spend.clone(), 99),
    ])
    .unwrap()
    .unwrap();
    assert_eq!(
        observation.outpoint,
        OutPoint::new(lockup.compute_txid(), 1)
    );
    assert_eq!(
        observation.spend.unwrap().id,
        actual_spend.compute_txid().to_string()
    );
    assert_eq!(observation.spend_confirmations, 2);
}

#[test]
fn mempool_spend_is_reported_without_confirmations() {
    let lockup = funding(VALUE);
    let spend = spending(&lockup, 0);
    let observation = observe_history(&[(lockup, 90), (spend, 0)])
        .unwrap()
        .unwrap();
    assert!(observation.spend.is_some());
    assert_eq!(observation.spend_confirmations, 0);
}

#[test]
fn conflicting_spenders_are_rejected() {
    let lockup = funding(VALUE);
    let spend = spending(&lockup, 0);
    let mut conflicting = spend.clone();
    conflicting.output[0].value = Amount::from_sat(VALUE - 1000);
    assert!(matches!(
        observe_history(&[(lockup, 90), (spend, 0), (conflicting, 0)]),
        Err(Error::Validation)
    ));
}

#[test]
fn future_block_height_cannot_count_as_a_confirmation() {
    assert!(matches!(
        observe_history(&[(funding(VALUE), TIP as i32 + 1)]),
        Err(Error::Chain)
    ));
}

#[test]
fn unconfirmed_funding_has_zero_confirmations() {
    let observation = observe_history(&[(funding(VALUE), -1)]).unwrap().unwrap();
    assert_eq!(observation.confirmations, 0);
}

#[test]
fn refund_sweeps_incorrect_amounts_and_multiple_outputs() {
    let mut underpayment = funding(500);
    underpayment.output.push(TxOut {
        value: Amount::from_sat(250_000),
        script_pubkey: contract_script(),
    });
    let excess = funding(1_000_000);
    let actual =
        refundable_outputs(&contract_script(), &[(underpayment, 99), (excess, 0)]).unwrap();
    assert_eq!(actual.len(), 3);
    assert_eq!(
        actual
            .iter()
            .map(|(_, output)| output.value.to_sat())
            .sum::<u64>(),
        1_250_500
    );
}

#[test]
fn refund_excludes_confirmed_and_mempool_spent_outputs() {
    for height in [0, 100] {
        let lockup = funding(500);
        let spend = spending(&lockup, 0);
        let actual =
            refundable_outputs(&contract_script(), &[(lockup, 90), (spend, height)]).unwrap();
        assert!(actual.is_empty());
    }
}

#[test]
fn refund_rejects_duplicate_history_entries() {
    let lockup = funding(500);
    assert!(matches!(
        refundable_outputs(&contract_script(), &[(lockup.clone(), 90), (lockup, 90)]),
        Err(Error::Validation)
    ));
}
